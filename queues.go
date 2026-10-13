package queueservice

import (
	"context"
	"net/http"
	"net/url"
	"time"
)

// Settings of a queue, in whole seconds. Nil fields keep their value (or
// take the default on a new queue).
type Settings struct {
	// VisibilityTimeout is how long a received message stays hidden before
	// it is handed out again (0–43200, default 30).
	VisibilityTimeout *int64 `json:"visibilityTimeout,omitempty"`
	// Retention is how long a message is kept (60–1209600, default 345600:
	// 4 days).
	Retention *int64 `json:"retention,omitempty"`
	// Delay hides every new message for this long (0–900, default 0).
	Delay *int64 `json:"delay,omitempty"`
	// MaxReceives moves a message to DeadLetterQueue (or drops it) after
	// this many receives (0 never, up to 1000).
	MaxReceives *int `json:"maxReceives,omitempty"`
	// DeadLetterQueue is a queue of the same Project ("" for none).
	DeadLetterQueue *string `json:"deadLetterQueue,omitempty"`
}

// Seconds is a convenience for Settings fields: Seconds(time.Minute).
func Seconds(d time.Duration) *int64 {
	s := int64(d / time.Second)
	return &s
}

// Int is a convenience for Settings.MaxReceives.
func Int(v int) *int { return &v }

// String is a convenience for Settings.DeadLetterQueue.
func String(v string) *string { return &v }

// QueueInfo is a queue's settings, and with GetQueue its backlog.
type QueueInfo struct {
	Name              string    `json:"name"`
	Region            string    `json:"region"`
	CreatedAt         time.Time `json:"createdAt"`
	VisibilityTimeout int64     `json:"visibilityTimeout"`
	Retention         int64     `json:"retention"`
	Delay             int64     `json:"delay"`
	MaxReceives       int       `json:"maxReceives"`
	DeadLetterQueue   string    `json:"deadLetterQueue,omitempty"`
	// Messages is the backlog (GetQueue and Queue.Info only).
	Messages *Stats `json:"messages,omitempty"`
}

// Stats is a queue's backlog.
type Stats struct {
	Visible  int64 `json:"visible"`  // ready to be received
	InFlight int64 `json:"inFlight"` // received, not yet deleted
	Delayed  int64 `json:"delayed"`  // sent with a delay still running
	Bytes    int64 `json:"bytes"`
	// OldestAge is the age of the oldest message, in seconds.
	OldestAge int64 `json:"oldestAge"`
}

// ListQueues lists the queues the key can reach, by name.
func (c *Client) ListQueues(ctx context.Context) ([]QueueInfo, error) {
	var out struct {
		Queues []QueueInfo `json:"queues"`
	}
	_, err := c.do(ctx, http.MethodGet, "/queues", nil, &out)
	return out.Queues, err
}

// PutQueue creates a queue, or changes the settings of the one by that
// name; created reports which. The key must be allowed to manage queues.
func (c *Client) PutQueue(ctx context.Context, name string, s *Settings) (info QueueInfo, created bool, err error) {
	if s == nil {
		s = &Settings{}
	}
	status, err := c.do(ctx, http.MethodPut, "/queues/"+url.PathEscape(name), s, &info)
	return info, status == http.StatusCreated, err
}

// GetQueue returns a queue's settings and backlog.
func (c *Client) GetQueue(ctx context.Context, name string) (QueueInfo, error) {
	var info QueueInfo
	_, err := c.do(ctx, http.MethodGet, "/queues/"+url.PathEscape(name), nil, &info)
	return info, err
}

// DeleteQueue deletes a queue with every message in it.
func (c *Client) DeleteQueue(ctx context.Context, name string) error {
	_, err := c.do(ctx, http.MethodDelete, "/queues/"+url.PathEscape(name), nil, nil)
	return err
}

// Queue is a handle for sending and receiving on one queue. It makes no
// call by itself.
func (c *Client) Queue(name string) *Queue {
	return &Queue{c: c, name: name, path: "/queues/" + url.PathEscape(name)}
}
