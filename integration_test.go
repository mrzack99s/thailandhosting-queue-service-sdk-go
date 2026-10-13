package queueservice

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestIntegration runs against a real endpoint when TH_QUEUE_TEST_ENDPOINT
// (with TH_QUEUE_TEST_REGION, TH_QUEUE_TEST_KEY and TH_QUEUE_TEST_SECRET)
// is set. The key must manage queues; it creates and deletes its own.
func TestIntegration(t *testing.T) {
	endpoint := os.Getenv("TH_QUEUE_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("TH_QUEUE_TEST_ENDPOINT is not set")
	}
	c, err := New(Config{Endpoint: endpoint, Region: os.Getenv("TH_QUEUE_TEST_REGION"), AccessKeyID: os.Getenv("TH_QUEUE_TEST_KEY"), SecretAccessKey: os.Getenv("TH_QUEUE_TEST_SECRET")})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	name := fmt.Sprintf("sdk-go-%d", time.Now().UnixNano())
	dlq := name + "-dlq"
	if _, created, err := c.PutQueue(ctx, dlq, nil); err != nil || !created {
		t.Fatalf("create dlq: %v %v", created, err)
	}
	info, created, err := c.PutQueue(ctx, name, &Settings{VisibilityTimeout: Seconds(30 * time.Second), MaxReceives: Int(2), DeadLetterQueue: String(dlq)})
	if err != nil || !created || info.MaxReceives != 2 || info.DeadLetterQueue != dlq {
		t.Fatalf("create: %+v %v %v", info, created, err)
	}
	defer c.DeleteQueue(ctx, name)
	defer c.DeleteQueue(ctx, dlq)
	if _, created, err := c.PutQueue(ctx, name, &Settings{VisibilityTimeout: Seconds(20 * time.Second)}); err != nil || created {
		t.Fatalf("update: %v %v", created, err)
	}
	list, err := c.ListQueues(ctx)
	if err != nil || len(list) < 2 {
		t.Fatalf("list: %v %v", list, err)
	}

	q := c.Queue(name)
	id, err := q.SendJSON(ctx, map[string]any{"order": 1, "name": "สวัสดี"}, &SendOptions{Attributes: map[string]string{"kind": "order"}})
	if err != nil || id == "" {
		t.Fatalf("send: %q %v", id, err)
	}
	ids, err := q.SendBatch(ctx, []Outgoing{{Body: "a"}, {Body: "b"}, {Body: "c", Attributes: map[string]string{"x": "1"}}})
	if err != nil || len(ids) != 3 {
		t.Fatalf("batch: %v %v", ids, err)
	}
	if info, err := q.Info(ctx); err != nil || info.Messages == nil || info.Messages.Visible != 4 {
		t.Fatalf("info: %+v %v", info, err)
	}
	got, err := q.Receive(ctx, &ReceiveOptions{Max: 10, Wait: time.Second})
	if err != nil || len(got) != 4 {
		t.Fatalf("receive: %d %v", len(got), err)
	}
	var first struct {
		Order int    `json:"order"`
		Name  string `json:"name"`
	}
	if err := got[0].JSON(&first); err != nil || first.Order != 1 || first.Name != "สวัสดี" || got[0].Attributes["kind"] != "order" || got[0].Receives != 1 {
		t.Fatalf("first: %+v %+v %v", got[0], first, err)
	}
	if err := q.Delete(ctx, got[0].Receipt); err != nil {
		t.Fatalf("delete: %v", err)
	}
	// Deleting again is harmless.
	if err := q.Delete(ctx, got[0].Receipt); err != nil {
		t.Fatalf("second delete: %v", err)
	}
	res, err := q.DeleteBatch(ctx, []string{got[1].Receipt, got[2].Receipt, "nonsense"})
	if err != nil || len(res) != 3 || !res[0].Deleted || !res[1].Deleted || res[2].Deleted {
		t.Fatalf("delete batch: %+v %v", res, err)
	}
	// Release the last one at once: it comes back with a second receive.
	if err := q.ChangeVisibility(ctx, got[3].Receipt, 0); err != nil {
		t.Fatalf("visibility: %v", err)
	}
	again, err := q.Receive(ctx, &ReceiveOptions{Wait: 2 * time.Second})
	if err != nil || len(again) != 1 || again[0].ID != got[3].ID || again[0].Receives != 2 {
		t.Fatalf("again: %+v %v", again, err)
	}
	// Past maxReceives (2) it goes to the dead-letter queue.
	q.ChangeVisibility(ctx, again[0].Receipt, 0)
	if m, _ := q.Receive(ctx, &ReceiveOptions{Wait: time.Second}); len(m) != 0 {
		t.Fatalf("a message past maxReceives was handed out: %+v", m)
	}
	if dead, err := c.Queue(dlq).Receive(ctx, &ReceiveOptions{Wait: 2 * time.Second}); err != nil || len(dead) != 1 || dead[0].Body != "c" {
		t.Fatalf("dead letter: %+v %v", dead, err)
	}

	// A worker handles every message; a failing one comes back.
	for i := range 25 {
		if _, err := q.Send(ctx, fmt.Sprint(i), nil); err != nil {
			t.Fatal(err)
		}
	}
	var mu sync.Mutex
	seen := map[string]int{}
	var failed atomic.Bool
	wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	err = q.Process(wctx, func(_ context.Context, m Message) error {
		if m.Body == "7" && failed.CompareAndSwap(false, true) {
			return fmt.Errorf("try again")
		}
		mu.Lock()
		seen[m.Body]++
		done := len(seen) == 25
		mu.Unlock()
		if done {
			cancel()
		}
		return nil
	}, &ProcessOptions{Concurrency: 4, Wait: time.Second, RetryDelay: time.Second})
	if err != nil || len(seen) != 25 || !failed.Load() || wctx.Err() != context.Canceled {
		t.Fatalf("process: %d seen, %v %v", len(seen), err, wctx.Err())
	}
	if info, _ := q.Info(ctx); info.Messages == nil || info.Messages.Visible+info.Messages.InFlight != 0 {
		t.Fatalf("left after the worker: %+v", info.Messages)
	}
	if _, err := q.Send(ctx, "x", nil); err != nil {
		t.Fatal(err)
	}
	if err := q.Purge(ctx); err != nil {
		t.Fatalf("purge: %v", err)
	}
	if _, err := c.GetQueue(ctx, "no-such-queue-"+name); !IsNotFound(err) {
		t.Fatalf("missing queue: %v", err)
	}
}
