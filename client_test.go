package queueservice

import (
	"bytes"
	"context"
	"errors"
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
