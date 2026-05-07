package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"task-queue/job"
	"task-queue/store"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	queueName  = "job_queue"
	maxRetries = 3
)

type Worker struct {
	redis *redis.Client
	store *store.Store
}

func NewWorker(r *redis.Client, s *store.Store) *Worker {
	return &Worker{redis: r, store: s}
}

func (w *Worker) Start(ctx context.Context) error {
	fmt.Println("Worker started, waiting for jobs...")
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Worker shutting down...")
			return ctx.Err()
		default:
		}

		result, err := w.redis.BLPop(ctx, 0, queueName).Result()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			fmt.Println("Error popping from queue:", err)
			time.Sleep(1 * time.Second)
			continue
		}

		var j job.Job
		if err := json.Unmarshal([]byte(result[1]), &j); err != nil {
			fmt.Println("Error unmarshaling job:", err)
			continue
		}

		w.processJob(ctx, j)
	}
}

func (w *Worker) processJob(ctx context.Context, j job.Job) {
	fmt.Printf("Processing job %v [%s]\n", j.ID, j.TaskType)

	if err := w.store.UpdateJobStatus(ctx, j.ID, job.StatusProcessing); err != nil {
		fmt.Printf("Failed to update job %v status: %v\n", j.ID, err)
	}

	err := w.executeJob(j)
	if err != nil {
		fmt.Printf("Job %v failed: %v\n", j.ID, err)
		w.store.IncrementRetry(ctx, j.ID)

		if j.Retries+1 < maxRetries {
			fmt.Printf("Re-queuing job %v (retry %d/%d)\n", j.ID, j.Retries+1, maxRetries)
			j.Retries++
			j.Status = job.StatusPending
			j.UpdatedAt = time.Now()
			w.store.UpdateJobStatus(ctx, j.ID, job.StatusPending)

			data, _ := json.Marshal(j)
			w.redis.RPush(ctx, queueName, data)
		} else {
			fmt.Printf("Max retries reached for job %v, marking as failed\n", j.ID)
			w.store.UpdateJobStatus(ctx, j.ID, job.StatusFailed)
		}
		return
	}

	w.store.UpdateJobStatus(ctx, j.ID, job.StatusCompleted)
	fmt.Printf("Job %v completed successfully\n", j.ID)
}

func (w *Worker) executeJob(j job.Job) error {
	switch j.TaskType {
	case "send_email":
		fmt.Printf("  → Sending email to %v\n", j.Payload["to"])
		time.Sleep(2 * time.Second)
		return nil
	case "resize_image":
		fmt.Printf("  → Resizing image %v\n", j.Payload["image"])
		time.Sleep(2 * time.Second)
		return nil
	case "generate_pdf":
		fmt.Printf("  → Generating PDF %v\n", j.Payload["pdf"])
		time.Sleep(2 * time.Second)
		return nil
	default:
		return fmt.Errorf("unknown task type: %s", j.TaskType)
	}
}
