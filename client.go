// Package queueservice is the Go SDK for ThailandHosting Queue Service.
//
//	client, err := queueservice.New(queueservice.Config{
//		Region:          "th-bkk-1",
//		AccessKeyID:     os.Getenv("TH_QUEUE_ACCESS_KEY_ID"),
//		SecretAccessKey: os.Getenv("TH_QUEUE_SECRET_ACCESS_KEY"),
//	})
//	jobs := client.Queue("jobs")
//	id, err := jobs.SendJSON(ctx, map[string]any{"orderId": 42}, nil)
//
//	// A worker: receive with long polling, handle, delete.
//	err = jobs.Process(ctx, func(ctx context.Context, m queueservice.Message) error {
//		return handle(m.Body)
//	}, nil)
//
// Delivery is at least once: a message is handed out again when it is not
// deleted before its visibility timeout ends, so handlers should be safe to
// run twice. The client is safe for concurrent use and should be reused: it
// keeps connections open. Requests that fail because the service was busy
// or unreachable are retried with exponential backoff.
package queueservice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Version is the SDK version, sent in the User-Agent.
const Version = "1.0.0"

// DefaultEndpoint is the Queue Service endpoint.
const DefaultEndpoint = "https://queues.thailandhosting.com"

// Config configures a Client. AccessKeyID and SecretAccessKey are
// required; every other field has a sensible default.
type Config struct {
	// Endpoint is the Queue Service endpoint (default DefaultEndpoint). It
	// may already end in a region, for example
	// https://queues.thailandhosting.com/th-bkk-1.
	Endpoint string
	// Region is the region of the queues, for example th-bkk-1, appended to
	// Endpoint. Without one the service finds each queue's region itself.
	Region string
	// AccessKeyID and SecretAccessKey are a Project access key from the
	// Console (Queue Service → Access keys). The secret signs every request
	// (HTTP Message Signatures, RFC 9421, HMAC-SHA256); it is never sent.
	AccessKeyID     string
	SecretAccessKey string

	// HTTPClient replaces the default client. Its Timeout, if any, must be
	// longer than the longest receive wait (20 s).
	HTTPClient *http.Client
	// MaxRetries is how many times a request is retried after the service
	// was busy or unreachable (default 4; negative disables retries).
	MaxRetries int
	// UserAgent is appended to the SDK's own.
	UserAgent string
}

const defaultMaxRetries = 4

// Client talks to Queue Service with one access key.
type Client struct {
	base      *url.URL // up to and including /v1
	keyID     string
	secret    string
	http      *http.Client
	retries   int
	userAgent string
	now       func() time.Time
}

// New creates a Client.
func New(cfg Config) (*Client, error) {
	if cfg.AccessKeyID == "" || cfg.SecretAccessKey == "" {
		return nil, errors.New("queueservice: AccessKeyID and SecretAccessKey are required")
	}
	endpoint := cfg.Endpoint
	if endpoint == "" {
		endpoint = DefaultEndpoint
	}
	u, err := url.Parse(strings.TrimRight(endpoint, "/"))
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		return nil, fmt.Errorf("queueservice: invalid Endpoint %q", endpoint)
	}
	u.Path = strings.TrimSuffix(u.Path, "/v1")
	if cfg.Region != "" {
		u.Path += "/" + url.PathEscape(cfg.Region)
	}
	u.Path += "/v1"
	u.RawPath = ""
	c := &Client{base: u, keyID: cfg.AccessKeyID, secret: cfg.SecretAccessKey, http: cfg.HTTPClient, retries: cfg.MaxRetries, now: time.Now}
	if c.http == nil {
		c.http = &http.Client{Transport: defaultTransport()}
	}
	if c.retries == 0 {
		c.retries = defaultMaxRetries
	} else if c.retries < 0 {
		c.retries = 0
	}
	c.userAgent = "thailandhosting-queue-service-sdk-go/" + Version
	if cfg.UserAgent != "" {
		c.userAgent += " " + cfg.UserAgent
	}
	return c, nil
}

func defaultTransport() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 256
	t.MaxIdleConnsPerHost = 64
	t.IdleConnTimeout = 90 * time.Second
	// A receive may wait up to 20 s for a message before answering.
	t.ResponseHeaderTimeout = time.Minute
	t.ForceAttemptHTTP2 = true
	return t
}

// Error is an error answer from the service, read from its Problem
// Details (RFC 9457).
type Error struct {
	StatusCode int
	// Code is the machine-readable reason, for example "queue_not_found",
	// "queue_full", "invalid_receipt" or "busy".
	Code string
	// Message is the human-readable title.
	Message string
	// Type is the problem type URI and Instance the path it happened on.
	Type, Instance string
}

func (e *Error) Error() string {
	return fmt.Sprintf("queueservice: %s (HTTP %d): %s", e.Code, e.StatusCode, e.Message)
}

// IsNotFound reports whether err means the queue does not exist (or the key
// may not reach it).
func IsNotFound(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.StatusCode == http.StatusNotFound
}

// IsInvalidReceipt reports whether err means a receipt is not valid, or
// its message was handed out again after its visibility timeout (deleting
// an already deleted message is not an error).
func IsInvalidReceipt(err error) bool {
	return ErrorCode(err) == "invalid_receipt"
}

// ErrorCode returns the service's error code of err, or "".
func ErrorCode(err error) string {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}

// do sends one call and decodes a JSON answer into out (when not nil).
// path is after /v1 and already escaped.
func (c *Client) do(ctx context.Context, method, path string, in, out any) (int, error) {
	var body []byte
	if in != nil {
		var err error
		if body, err = json.Marshal(in); err != nil {
			return 0, fmt.Errorf("queueservice: encode request: %w", err)
		}
	}
	for attempt := 0; ; attempt++ {
		status, retry, wait, err := c.once(ctx, method, path, body, out)
		if err == nil || !retry || attempt >= c.retries {
			return status, err
		}
		if wait <= 0 {
			wait = backoff(attempt)
		}
		select {
		case <-ctx.Done():
			return status, err
		case <-time.After(wait):
		}
	}
}

// backoff is the pause before retry n: exponential from 100 ms, capped at
// 5 s, with jitter.
func backoff(n int) time.Duration {
	d := 100 * time.Millisecond << min(n, 6)
	d = min(d, 5*time.Second)
	return d/2 + time.Duration(rand.Int64N(int64(d/2)+1))
}

func (c *Client) once(ctx context.Context, method, path string, body []byte, out any) (status int, retry bool, wait time.Duration, err error) {
	u := *c.base
	u.Path += path
	u.RawPath = c.base.EscapedPath() + path
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), rd)
	if err != nil {
		return 0, false, 0, err
	}
	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Content-Digest", contentDigest(body))
	}
	signRequest(req, c.keyID, c.secret, c.now())
	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return 0, false, 0, ctx.Err()
		}
		var ne net.Error
		return 0, errors.As(err, &ne) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF), 0, fmt.Errorf("queueservice: %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return resp.StatusCode, true, 0, fmt.Errorf("queueservice: read answer: %w", err)
	}
	if resp.StatusCode >= 300 {
		e := &Error{StatusCode: resp.StatusCode, Code: resp.Header.Get("X-Error-Code")}
		var p struct {
			Type, Title, Code, Instance string
		}
		if json.Unmarshal(data, &p) == nil {
			e.Type, e.Message, e.Instance = p.Type, p.Title, p.Instance
			if p.Code != "" {
				e.Code = p.Code
			}
		}
		if e.Code == "" {
			e.Code = strings.ToLower(strings.ReplaceAll(http.StatusText(resp.StatusCode), " ", "_"))
		}
		if e.Message == "" {
			e.Message = strings.TrimSpace(string(data))
		}
		retry := resp.StatusCode == http.StatusBadGateway || resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusGatewayTimeout
		if s, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && s >= 0 && s <= 60 {
			wait = time.Duration(s) * time.Second
		}
		return resp.StatusCode, retry, wait, e
	}
	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			return resp.StatusCode, false, 0, fmt.Errorf("queueservice: decode answer: %w", err)
		}
	}
	return resp.StatusCode, false, 0, nil
}
