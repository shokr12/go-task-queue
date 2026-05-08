package queue

import (
	"context"
	"encoding/json"
	"log"
	"task-queue/job"
	"task-queue/store"
	"time"

	"github.com/redis/go-redis/v9"
)

type Producer struct {
	redis     *redis.Client
	store     *store.Store
	buffer    chan *job.Job
	maxBatch  int
	flushFreq time.Duration
}

func NewProducer(r *redis.Client, s *store.Store) *Producer {
	p := &Producer{
		redis:     r,
		store:     s,
		buffer:    make(chan *job.Job, 100000), 
		maxBatch:  500,                        
		flushFreq: 100 * time.Millisecond,     
	}
	go p.startFlushLoop()
	return p
}

func (p *Producer) Enqueue(j *job.Job) {
	select {
	case p.buffer <- j:
	default:
		log.Println("Warning: Buffer full, dropping job or applying backpressure")
	}
}

func (p *Producer) startFlushLoop() {
	ticker := time.NewTicker(p.flushFreq)
	defer ticker.Stop()

	var batch []*job.Job
	ctx := context.Background()

	for {
		select {
		case j := <-p.buffer:
			batch = append(batch, j)
			if len(batch) >= p.maxBatch {
				p.flush(ctx, batch)
				batch = make([]*job.Job, 0, p.maxBatch)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				p.flush(ctx, batch)
				batch = make([]*job.Job, 0, p.maxBatch)
			}
		}
	}
}

func (p *Producer) flush(ctx context.Context, batch []*job.Job) {

	if err := p.store.CreateJobBatch(ctx, batch); err != nil {
		log.Printf("Failed to batch insert to Postgres: %v", err)
	}
	pipe := p.redis.Pipeline()
	for _, j := range batch {
		data, _ := json.Marshal(j)
		pipe.RPush(ctx, "job_queue", data)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		log.Printf("Failed to pipeline push to Redis: %v", err)
	}
}
