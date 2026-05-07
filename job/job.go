package job

import (
	"time"
)

type JobStatus string

const (
	StatusPending    JobStatus = "pending"
	StatusProcessing JobStatus = "processing"
	StatusCompleted  JobStatus = "completed"
	StatusFailed     JobStatus = "failed"
)

type Job struct {
	ID        int                    `json:"id"`
	TaskType  string                 `json:"task_type"`
	Payload   map[string]interface{} `json:"payload"`
	Status    JobStatus              `json:"status"`
	CreatedAt time.Time              `json:"created_at"`
	Retries   int                    `json:"retries"`
	UpdatedAt time.Time              `json:"updated_at"`
}

type CreateJobRequest struct {
	TaskType string                 `json:"task_type" binding:"required"`
	Payload  map[string]interface{} `json:"payload"`
}

func NewJob(taskType string, payload map[string]interface{}) *Job {
	now := time.Now()
	return &Job{
		TaskType:  taskType,
		Payload:   payload,
		Status:    StatusPending,
		CreatedAt: now,
		Retries:   0,
		UpdatedAt: now,
	}
}
