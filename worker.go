package queueservice

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ProcessOptions shape Process. Every field has a default.
type ProcessOptions struct {
	// Concurrency is how many messages are handled at once (default 1).
	Concurrency int
	// Wait is how long each receive long-polls (default and at most 20 s).
	Wait time.Duration
	// VisibilityTimeout hides received messages for this long instead of
	// the queue's own timeout; make it longer than a handler takes.
	VisibilityTimeout time.Duration
	// RetryDelay is how long a message whose handler failed stays hidden
	// before it is handed out again (0: until its visibility timeout ends).
	RetryDelay time.Duration
	// OnError hears about handler failures and errors of the service the
	// worker rode out (m is nil for those). Default: none.
	OnError func(m *Message, err error)
}

// Process receives messages and runs handle on each, Concurrency at a
// time, until ctx ends (then it returns nil once the running handlers
// return). A message whose handler returns nil is deleted; one whose
// handler fails or panics is left to be handed out again (after RetryDelay
// when set), and after the queue's MaxReceives it goes to its dead-letter
// queue. Busy or unreachable service answers are retried with backoff;
// errors that retrying cannot fix (a missing queue, a key that may not
// receive) stop Process and are returned.
func (q *Queue) Process(ctx context.Context, handle func(ctx context.Context, m Message) error, opt *ProcessOptions) error {
	o := ProcessOptions{}
	if opt != nil {
		o = *opt
	}
	if o.Concurrency <= 0 {
		o.Concurrency = 1
	}
	if o.Wait <= 0 || o.Wait > MaxWait {
		o.Wait = MaxWait
	}
	report := func(m *Message, err error) {
		if o.OnError != nil {
			o.OnError(m, err)
		}
	}
	slots := make(chan struct{}, o.Concurrency)
	var wg sync.WaitGroup
	defer wg.Wait()
	failures := 0
	for ctx.Err() == nil {
		// Take only as many messages as there are free slots, so none
		// waits hidden while its visibility timeout runs.
		slots <- struct{}{}
		free := 1
		for free < min(o.Concurrency, MaxBatch) {
			select {
			case slots <- struct{}{}:
				free++
				continue
			default:
			}
			break
		}
		msgs, err := q.Receive(ctx, &ReceiveOptions{Max: free, Wait: o.Wait, VisibilityTimeout: o.VisibilityTimeout})
		for range free - len(msgs) {
			<-slots
		}
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			if permanent(err) {
				return err
			}
			report(nil, err)
			failures++
			select {
			case <-ctx.Done():
			case <-time.After(backoff(failures)):
			}
			continue
		}
		failures = 0
		for _, m := range msgs {
			wg.Add(1)
			go func(m Message) {
				defer func() { <-slots; wg.Done() }()
				// Handlers finish and delete even as ctx ends.
				hctx := context.WithoutCancel(ctx)
				if err := run(hctx, handle, m); err != nil {
					report(&m, err)
					if o.RetryDelay > 0 {
						if err := q.ChangeVisibility(hctx, m.Receipt, o.RetryDelay); err != nil && !IsInvalidReceipt(err) {
							report(&m, err)
						}
					}
					return
				}
				if err := q.Delete(hctx, m.Receipt); err != nil {
					report(&m, err)
				}
			}(m)
		}
	}
	return nil
}

// run calls handle, turning a panic into an error.
func run(ctx context.Context, handle func(context.Context, Message) error, m Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("queueservice: handler panicked: %v", r)
		}
	}()
	return handle(ctx, m)
}

// permanent reports whether retrying cannot help.
func permanent(err error) bool {
	var e *Error
	if !errors.As(err, &e) {
		return false
	}
	switch e.StatusCode {
	case 400, 401, 403, 404, 421:
		return e.Code != "project_suspended"
	}
	return false
}
