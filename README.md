# ThailandHosting Queue Service — Go SDK

Go client for ThailandHosting Queue Service: message queues that take slow
work off your site or app. One side sends jobs, workers receive them, and
nothing is lost when a worker fails.

```
go get github.com/mrzack99s/thailandhosting-queue-service-sdk-go
```

Go 1.23+. No dependencies outside the standard library.

## Quick start

```go
import queueservice "github.com/mrzack99s/thailandhosting-queue-service-sdk-go"

client, err := queueservice.New(queueservice.Config{
	Region:          "th-bkk-1",
	AccessKeyID:     os.Getenv("TH_QUEUE_ACCESS_KEY_ID"),
	SecretAccessKey: os.Getenv("TH_QUEUE_SECRET_ACCESS_KEY"),
})
jobs := client.Queue("jobs")

// Send
id, err := jobs.SendJSON(ctx, map[string]any{"orderId": 42}, nil)
_, err = jobs.Send(ctx, "plain text", &queueservice.SendOptions{
	Attributes: map[string]string{"kind": "email"},
	Delay:      30 * time.Second,
})
ids, err := jobs.SendBatch(ctx, []queueservice.Outgoing{{Body: "a"}, {Body: "b"}})

// A worker: long-polls, runs the handler, deletes what it handled.
// It runs until ctx ends.
err = jobs.Process(ctx, func(ctx context.Context, m queueservice.Message) error {
	var order struct{ OrderID int }
	if err := m.JSON(&order); err != nil {
		return err
	}
	return ship(ctx, order.OrderID) // an error leaves the message to be retried
}, &queueservice.ProcessOptions{Concurrency: 8, RetryDelay: 30 * time.Second})
```

Create access keys in the Console (Queue Service → Access keys). Reuse one
`Client`: it is safe for concurrent use and keeps connections open.

## Receiving by hand

```go
msgs, err := jobs.Receive(ctx, &queueservice.ReceiveOptions{Max: 10, Wait: 20 * time.Second})
for _, m := range msgs {
	if err := handle(m); err != nil {
		continue // handed out again after the visibility timeout
	}
	jobs.Delete(ctx, m.Receipt)
}
```

- `Wait` long-polls up to 20 s for a first message, so an idle worker makes
  few requests.
- A received message is hidden for the queue's visibility timeout (or
  `VisibilityTimeout`). Delete it with its receipt once handled; otherwise it
  is handed out again. `m.Receives` counts the deliveries.
- `ChangeVisibility(ctx, receipt, d)` keeps a message hidden longer while you
  work on it, or with 0 releases it at once.
- `DeleteBatch` deletes up to 10 receipts and reports each one.

Delivery is **at least once**: make handlers safe to run twice (check an
order ID before charging it, for example).

## Queues

```go
info, created, err := client.PutQueue(ctx, "jobs", &queueservice.Settings{
	VisibilityTimeout: queueservice.Seconds(2 * time.Minute),
	MaxReceives:       queueservice.Int(5),
	DeadLetterQueue:   queueservice.String("jobs-dlq"), // create it first
})
queues, err := client.ListQueues(ctx)
info, err = client.GetQueue(ctx, "jobs") // info.Messages: visible, in flight, delayed, oldest age
err = jobs.Purge(ctx)
err = client.DeleteQueue(ctx, "jobs")
```

`PutQueue` creates a queue or changes the given settings of an existing one
(the key must be allowed to manage queues). After `MaxReceives` deliveries a
message moves to the dead-letter queue (or is dropped without one).

| Setting | Default | Range |
| --- | --- | --- |
| `VisibilityTimeout` | 30 s | 0 – 12 h |
| `Retention` | 4 days | 1 min – 14 days |
| `Delay` | 0 | 0 – 15 min |
| `MaxReceives` | 0 (never move) | 0 – 1000 |

Limits: a message is at most 256 KiB (body and up to 10 attributes), a batch 10
messages and 1 MiB.

## Errors

Error answers are `*queueservice.Error` with the service's `Code`:

```go
if queueservice.IsNotFound(err) { ... }          // queue_not_found
if queueservice.ErrorCode(err) == "queue_full" { ... }
```

Common codes: `queue_not_found`, `forbidden`, `invalid_receipt`,
`queue_full`, `too_many_in_flight`, `too_large`, `project_suspended`,
`wrong_region`, `busy`. Busy or unreachable answers are retried with
exponential backoff (`MaxRetries`, default 4).

## Endpoint and region

The default endpoint is `https://queues.thailandhosting.com`. Set `Region`
to the queues' region; without one the service finds each queue's region.
`Endpoint` may also be given with the region already in it:
`https://queues.thailandhosting.com/th-bkk-1`.

## Authentication

Every request is signed with HTTP Message Signatures (RFC 9421,
HMAC-SHA256 with the secret access key) over the method, host, path, query
and the body's Content-Digest (RFC 9530). The secret is never sent.

## Tests

```
go test ./...
```

Set `TH_QUEUE_TEST_ENDPOINT`, `TH_QUEUE_TEST_REGION`, `TH_QUEUE_TEST_KEY` and
`TH_QUEUE_TEST_SECRET` to also run the round trip against a real endpoint
(the key must manage queues; the test creates and deletes its own).

## License

MIT
