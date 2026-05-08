package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"task-queue/job"
	"task-queue/store"
	"time"

	"github.com/redis/go-redis/v9"
	"sync/atomic"
)

const (
	queueName  = "job_queue"
	maxRetries = 3
)

type Worker struct {
	redis       *redis.Client
	store       *store.Store
	startTime   time.Time
	activeJobs  int32
	jobsFinished int32
}

func NewWorker(r *redis.Client, s *store.Store) *Worker {
	return &Worker{redis: r, store: s}
}


func (w *Worker) Start(ctx context.Context) error {
	fmt.Println("Worker started, waiting for jobs...")
	
	sem := make(chan struct{},  20000)


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

		sem <- struct{}{} 
		
		if atomic.LoadInt32(&w.activeJobs) == 0 {
			w.startTime = time.Now()
			atomic.StoreInt32(&w.jobsFinished, 0)
			fmt.Println(">>> Starting batch timer...")
		}
		atomic.AddInt32(&w.activeJobs, 1)

		go func(jobData job.Job) {
			defer func() { 
				<-sem
				finished := atomic.AddInt32(&w.jobsFinished, 1)
				active := atomic.AddInt32(&w.activeJobs, -1)
				
				if active == 0 {
					duration := time.Since(w.startTime)
					fmt.Printf("\n==========================================\n")
					fmt.Printf("BATCH COMPLETED: %d jobs in %v\n", finished, duration)
					fmt.Printf("Average speed: %.2f jobs/sec\n", float64(finished)/duration.Seconds())
					fmt.Printf("==========================================\n\n")
				}
			}() 
			w.processJob(ctx, jobData)
		}(j)
	}
}


func (w *Worker) processJob(ctx context.Context, j job.Job) {
	_ = w.executeJob(j)
}

func (w *Worker) executeJob(j job.Job) error {
	switch j.TaskType {
	case "send_email":
		return nil
	case "resize_image":
		return nil
	case "generate_pdf":
		return nil
	default:
		return fmt.Errorf("unknown task type: %s", j.TaskType)
	}
}
