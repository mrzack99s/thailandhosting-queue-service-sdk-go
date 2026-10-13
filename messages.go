package queueservice

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// Limits of the service.
const (
	// MaxBatch is the most messages one send, receive or delete takes.
	MaxBatch = 10
	// MaxMessageBytes is the largest message, body and attributes together.
	MaxMessageBytes = 256 << 10
	// MaxWait is the longest a receive waits for a first message.
	MaxWait = 20 * time.Second
)

// Queue sends and receives on one queue.
type Queue struct {
	c    *Client
	name string
	path string
}

// Name is the queue's name.
func (q *Queue) Name() string { return q.name }

// Info returns the queue's settings and backlog.
func (q *Queue) Info(ctx context.Context) (QueueInfo, error) { return q.c.GetQueue(ctx, q.name) }

// SendOptions shape one message.
type SendOptions struct {
	// Attributes travel with the message (string keys and values).
	Attributes map[string]string
	// Delay hides the message for this long first (whole seconds, at most
	// 15 minutes; 0 uses the queue's delay).
	Delay time.Duration
}

// Outgoing is one message of a batch.
type Outgoing struct {
	Body       string
	Attributes map[string]string
	Delay      time.Duration
}

type outgoingJSON struct {
	Body       string            `json:"body"`
	Attributes map[string]string `json:"attributes,omitempty"`
	Delay      *int64            `json:"delay,omitempty"`
}

func outgoing(body string, attrs map[string]string, delay time.Duration) outgoingJSON {
	o := outgoingJSON{Body: body, Attributes: attrs}
	if delay > 0 {
		o.Delay = Seconds(delay)
	}
	return o
}

// Send sends one message and returns its ID.
func (q *Queue) Send(ctx context.Context, body string, opt *SendOptions) (string, error) {
	if opt == nil {
		opt = &SendOptions{}
	}
	var out struct {
		ID string `json:"id"`
	}
	_, err := q.c.do(ctx, http.MethodPost, q.path+"/messages", outgoing(body, opt.Attributes, opt.Delay), &out)
	return out.ID, err
}

// SendJSON sends v encoded as JSON.
func (q *Queue) SendJSON(ctx context.Context, v any, opt *SendOptions) (string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("queueservice: encode message: %w", err)
	}
	return q.Send(ctx, string(data), opt)
}

// SendBatch sends up to MaxBatch messages in one request (all or none) and
// returns their IDs in order.
func (q *Queue) SendBatch(ctx context.Context, msgs []Outgoing) ([]string, error) {
	if len(msgs) == 0 || len(msgs) > MaxBatch {
		return nil, fmt.Errorf("queueservice: send 1 to %d messages at once", MaxBatch)
	}
	in := struct {
		Messages []outgoingJSON `json:"messages"`
	}{make([]outgoingJSON, len(msgs))}
	for i, m := range msgs {
		in.Messages[i] = outgoing(m.Body, m.Attributes, m.Delay)
	}
	var out struct {
		IDs []string `json:"ids"`
	}
	_, err := q.c.do(ctx, http.MethodPost, q.path+"/messages", in, &out)
	return out.IDs, err
}

// ReceiveOptions shape one receive.
type ReceiveOptions struct {
	// Max is how many messages to take, 1 to MaxBatch (default 1).
	Max int
	// VisibilityTimeout hides the received messages for this long instead
	// of the queue's timeout (whole seconds, at least 1 s).
	VisibilityTimeout time.Duration
	// Wait long-polls up to this long (at most MaxWait) for a first
	// message; 0 answers at once.
	Wait time.Duration
}

// Message is a received message. Delete it with its Receipt once handled.
type Message struct {
	ID         string            `json:"id"`
	Receipt    string            `json:"receipt"`
	Body       string            `json:"body"`
	Attributes map[string]string `json:"attributes,omitempty"`
	SentAt     time.Time         `json:"sentAt"`
	// Receives counts how many times the message was handed out, this one
	// included.
	Receives        int       `json:"receives"`
	FirstReceivedAt time.Time `json:"firstReceivedAt"`
}

// JSON decodes the body into v.
func (m Message) JSON(v any) error { return json.Unmarshal([]byte(m.Body), v) }

// Receive takes up to opt.Max messages, hiding them for the visibility
// timeout. It returns none when the queue is empty (after waiting up to
// opt.Wait).
func (q *Queue) Receive(ctx context.Context, opt *ReceiveOptions) ([]Message, error) {
	in := map[string]int64{}
	if opt != nil {
		if opt.Max > 0 {
			in["max"] = int64(opt.Max)
		}
		if opt.VisibilityTimeout > 0 {
			in["visibilityTimeout"] = max(int64(opt.VisibilityTimeout/time.Second), 1)
		}
		if opt.Wait > 0 {
			in["wait"] = int64(min(opt.Wait, MaxWait) / time.Second)
		}
	}
	var out struct {
		Messages []Message `json:"messages"`
	}
	_, err := q.c.do(ctx, http.MethodPost, q.path+"/receive", in, &out)
	return out.Messages, err
}

// Delete deletes a handled message by its receipt. Deleting it again is
// not an error.
func (q *Queue) Delete(ctx context.Context, receipt string) error {
	_, err := q.c.do(ctx, http.MethodDelete, q.path+"/messages/"+url.PathEscape(receipt), nil, nil)
	return err
}

// DeleteResult is the outcome of one receipt of DeleteBatch.
type DeleteResult struct {
	Receipt string `json:"receipt"`
	Deleted bool   `json:"deleted"`
	// Error is "invalid_receipt" or "unavailable" when not deleted.
	Error string `json:"error,omitempty"`
}

// DeleteBatch deletes up to MaxBatch handled messages; each receipt has its
// own result, in the order of receipts.
func (q *Queue) DeleteBatch(ctx context.Context, receipts []string) ([]DeleteResult, error) {
	if len(receipts) == 0 || len(receipts) > MaxBatch {
		return nil, fmt.Errorf("queueservice: delete 1 to %d receipts at once", MaxBatch)
	}
	var out struct {
		Results []DeleteResult `json:"results"`
	}
	if _, err := q.c.do(ctx, http.MethodPost, q.path+"/delete", map[string]any{"receipts": receipts}, &out); err != nil {
		return nil, err
	}
	return inOrder(receipts, out.Results), nil
}

// inOrder lines results up with the receipts they answer.
func inOrder(receipts []string, results []DeleteResult) []DeleteResult {
	byReceipt := map[string][]DeleteResult{}
	for _, r := range results {
		byReceipt[r.Receipt] = append(byReceipt[r.Receipt], r)
	}
	out := make([]DeleteResult, len(receipts))
	for i, rc := range receipts {
		if rs := byReceipt[rc]; len(rs) > 0 {
			out[i], byReceipt[rc] = rs[0], rs[1:]
		} else {
			out[i] = DeleteResult{Receipt: rc, Error: "unavailable"}
		}
	}
	return out
}

// ChangeVisibility hides a received message for timeout from now (to work
// on it longer), or with 0 hands it out again at once.
func (q *Queue) ChangeVisibility(ctx context.Context, receipt string, timeout time.Duration) error {
	_, err := q.c.do(ctx, http.MethodPost, q.path+"/visibility", map[string]any{"receipt": receipt, "timeout": int64(timeout / time.Second)}, nil)
	return err
}

// Purge removes every message of the queue.
func (q *Queue) Purge(ctx context.Context) error {
	_, err := q.c.do(ctx, http.MethodPost, q.path+"/purge", map[string]any{}, nil)
	return err
}
