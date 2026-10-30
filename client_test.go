package queueservice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestSignatureVector checks signing against a vector made by the
// service's own signer.
func TestSignatureVector(t *testing.T) {
	body := []byte(`{"body":"hello"}`)
	req, _ := http.NewRequest("POST", "https://queues.thailandhosting.com/th-bkk-1/v1/queues/jobs/messages?x=1", bytes.NewReader(body))
	req.Header.Set("Content-Digest", contentDigest(body))
	signRequest(req, "AKID", "secret", time.Unix(1767225600, 0))
	if got, want := req.Header.Get("Content-Digest"), "sha-256=:HaY64dHGT0VJzs5YVVsj7yU6p7ssXKbEjBKRiGPP9Ro=:"; got != want {
		t.Fatalf("digest %s", got)
	}
	if got, want := req.Header.Get("Signature-Input"), `th=("@method" "@authority" "@path" "@query" "content-digest");created=1767225600;keyid="AKID";alg="hmac-sha256"`; got != want {
		t.Fatalf("input %s", got)
	}
	if got, want := req.Header.Get("Signature"), "th=:eOsMVDCzdaff3N9m/aWz1ijBsJ9BgdiTtotBm4vfDdo=:"; got != want {
		t.Fatalf("signature %s", got)
	}
	req, _ = http.NewRequest("GET", "https://queues.thailandhosting.com/th-bkk-1/v1/queues", nil)
	signRequest(req, "AKID", "secret", time.Unix(1767225600, 0))
	if got, want := req.Header.Get("Signature"), "th=:OjjuqbpOgI6K6AMMgIsPsySRmAEGuQPAwO/wXEyBfb4=:"; got != want {
		t.Fatalf("GET signature %s", got)
	}
}

func TestEndpointAndRegion(t *testing.T) {
	for _, tc := range []struct{ endpoint, region, want string }{
		{"", "", "https://queues.thailandhosting.com/v1"},
		{"", "th-bkk-1", "https://queues.thailandhosting.com/th-bkk-1/v1"},
		{"https://queues.thailandhosting.com/th-bkk-1/", "", "https://queues.thailandhosting.com/th-bkk-1/v1"},
		{"http://127.0.0.1:7811/v1", "th-bkk-1", "http://127.0.0.1:7811/th-bkk-1/v1"},
	} {
		c, err := New(Config{Endpoint: tc.endpoint, Region: tc.region, AccessKeyID: "k", SecretAccessKey: "s"})
		if err != nil || c.base.String() != tc.want {
			t.Fatalf("%q %q: %v %v", tc.endpoint, tc.region, c.base, err)
		}
	}
	if _, err := New(Config{AccessKeyID: "k"}); err == nil {
		t.Fatal("a key without its secret was accepted")
	}
}

// TestErrorsAndRetries reads Problem Details and retries busy answers.
func TestErrorsAndRetries(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.Header.Get("Signature-Input"), "th=(") {
			t.Errorf("unsigned request")
		}
		switch r.URL.Path {
		case "/th-bkk-1/v1/queues/busy":
			if calls.Add(1) < 3 {
				w.Header().Set("Retry-After", "0")
				w.Header().Set("Content-Type", "application/problem+json")
				w.WriteHeader(503)
				w.Write([]byte(`{"type":"tag:x","title":"Busy","status":503,"code":"busy"}`))
				return
			}
			w.Write([]byte(`{"name":"busy","messages":{"visible":3}}`))
		default:
			w.Header().Set("X-Error-Code", "queue_not_found")
			w.Header().Set("Content-Type", "application/problem+json")
			w.WriteHeader(404)
			w.Write([]byte(`{"type":"tag:x","title":"The queue does not exist.","status":404,"code":"queue_not_found","instance":"` + r.URL.Path + `"}`))
		}
	}))
	defer srv.Close()
	c, _ := New(Config{Endpoint: srv.URL, Region: "th-bkk-1", AccessKeyID: "k", SecretAccessKey: "s"})
	info, err := c.GetQueue(context.Background(), "busy")
	if err != nil || info.Messages == nil || info.Messages.Visible != 3 || calls.Load() != 3 {
		t.Fatalf("%+v %v after %d calls", info, err, calls.Load())
	}
	_, err = c.GetQueue(context.Background(), "missing")
	var e *Error
	if !IsNotFound(err) || !errors.As(err, &e) || e.Code != "queue_not_found" || e.Instance != "/th-bkk-1/v1/queues/missing" {
		t.Fatalf("%#v", err)
	}
}

// TestTokenSource sends a fresh token on every attempt, with no signature,
// and long-polls with it.
func TestTokenSource(t *testing.T) {
	var answers atomic.Int32
	var wait atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Signature") != "" || r.Header.Get("Signature-Input") != "" {
			t.Errorf("signed with a token: %v", r.Header)
		}
		n := answers.Add(1)
		if want := fmt.Sprintf("Bearer tok-%d", n); r.Header.Get("Authorization") != want {
			t.Errorf("Authorization %q, want %q", r.Header.Get("Authorization"), want)
		}
		if strings.HasSuffix(r.URL.Path, "/receive") {
			var in struct {
				Wait int64 `json:"wait"`
			}
			json.NewDecoder(r.Body).Decode(&in)
			wait.Store(in.Wait)
			w.Write([]byte(`{"messages":[{"id":"m1","receipt":"r1","body":"hello"}]}`))
			return
		}
		if n == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(503)
			return
		}
		w.Write([]byte(`{"name":"jobs"}`))
	}))
	defer srv.Close()
	var calls atomic.Int32
	c, err := New(Config{Endpoint: srv.URL, TokenSource: func(ctx context.Context) (string, error) {
		return fmt.Sprintf("tok-%d", calls.Add(1)), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := c.GetQueue(ctx, "jobs"); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || answers.Load() != 2 {
		t.Fatalf("%d tokens for %d requests", calls.Load(), answers.Load())
	}
	msgs, err := c.Queue("jobs").Receive(ctx, &ReceiveOptions{Wait: 20 * time.Second})
	if err != nil || len(msgs) != 1 || msgs[0].Body != "hello" || wait.Load() != 20 {
		t.Fatalf("long poll: %+v %v, wait %d", msgs, err, wait.Load())
	}
}

// TestTokenSourceFailure sends nothing, retries nothing and stops a worker.
func TestTokenSourceFailure(t *testing.T) {
	var answers atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		answers.Add(1)
	}))
	defer srv.Close()
	boom := errors.New("boom")
	var calls atomic.Int32
	source := func(ctx context.Context) (string, error) {
		calls.Add(1)
		return "", boom
	}
	c, err := New(Config{Endpoint: srv.URL, TokenSource: source})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := c.GetQueue(ctx, "jobs"); !errors.Is(err, ErrTokenSource) || !errors.Is(err, boom) || calls.Load() != 1 {
		t.Fatalf("%v after %d calls", err, calls.Load())
	}
	// A worker outlives a token outage: it keeps trying until told to stop.
	short, stop := context.WithTimeout(ctx, 700*time.Millisecond)
	defer stop()
	before := calls.Load()
	err = c.Queue("jobs").Process(short, func(context.Context, Message) error { return nil }, nil)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Process: %v", err)
	}
	if calls.Load() < before+2 {
		t.Fatalf("Process stopped after %d calls", calls.Load()-before)
	}
	empty, _ := New(Config{Endpoint: srv.URL, TokenSource: func(context.Context) (string, error) { return "", nil }})
	if _, err := empty.GetQueue(ctx, "jobs"); !errors.Is(err, ErrTokenSource) {
		t.Fatalf("an empty token: %v", err)
	}
	if answers.Load() != 0 {
		t.Fatalf("%d requests sent without a token", answers.Load())
	}
}

func TestCredentialsAreExclusive(t *testing.T) {
	source := func(context.Context) (string, error) { return "tok", nil }
	for _, cfg := range []Config{
		{},
		{AccessKeyID: "k"},
		{AccessKeyID: "k", SecretAccessKey: "s", TokenSource: source},
		{AccessKeyID: "k", TokenSource: source},
		{SecretAccessKey: "s", TokenSource: source},
	} {
		if _, err := New(cfg); err == nil {
			t.Fatalf("accepted key %q, secret %q, source %v", cfg.AccessKeyID, cfg.SecretAccessKey, cfg.TokenSource != nil)
		}
	}
	if _, err := New(Config{TokenSource: source}); err != nil {
		t.Fatal(err)
	}
}
