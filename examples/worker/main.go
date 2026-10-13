// Command worker sends a few jobs and handles them with Process.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"

	queueservice "github.com/mrzack99s/thailandhosting-queue-service-sdk-go"
)

func main() {
	client, err := queueservice.New(queueservice.Config{
		Region:          "th-bkk-1",
		AccessKeyID:     os.Getenv("TH_QUEUE_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("TH_QUEUE_SECRET_ACCESS_KEY"),
	})
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	jobs := client.Queue("jobs")
	for i := range 3 {
		if _, err := jobs.SendJSON(ctx, map[string]int{"job": i}, nil); err != nil {
			log.Fatal(err)
		}
	}
	// Runs until Ctrl-C; each handled message is deleted.
	err = jobs.Process(ctx, func(ctx context.Context, m queueservice.Message) error {
		var job struct{ Job int }
		if err := m.JSON(&job); err != nil {
			return err
		}
		fmt.Println("handling job", job.Job)
		return nil
	}, &queueservice.ProcessOptions{Concurrency: 4})
	if err != nil {
		log.Fatal(err)
	}
}
