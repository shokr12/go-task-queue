package store

import (
	"context"
	"encoding/json"
	"fmt"
	"task-queue/job"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pg *pgxpool.Pool
}

func NewStore(dsn string) (*Store, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}

	config.MaxConns = 50
	config.MinConns = 10
	config.MaxConnIdleTime = 5 * time.Minute

	pg, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %v", err)
	}
	return &Store{pg: pg}, nil
}


func (s *Store) Close() {
	s.pg.Close()
}

func (s *Store) RunMigrations(ctx context.Context, schema string) error {
	_, err := s.pg.Exec(ctx, schema)
	return err
}

func (s *Store) CreateJobBatch(ctx context.Context, jobs []*job.Job) error {
	batch := &pgx.Batch{}
	for _, j := range jobs {
		payloadJSON, _ := json.Marshal(j.Payload)
		batch.Queue(`INSERT INTO jobs (id, task_type, payload, status, created_at, retries, updated_at) 
		             VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			j.ID, j.TaskType, string(payloadJSON), j.Status, j.CreatedAt, j.Retries, j.UpdatedAt)
	}
	
	results := s.pg.SendBatch(ctx, batch)
	defer results.Close()
	
	for i := 0; i < len(jobs); i++ {
		_, err := results.Exec()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) UpdateJob(ctx context.Context, j *job.Job) error {
	_, err := s.pg.Exec(ctx, `
		UPDATE jobs 
		SET status = $1, retries = $2, updated_at = $3
		WHERE id = $4
	`, j.Status, j.Retries, j.UpdatedAt, j.ID)
	return err
}

func (s *Store) GetJobByID(ctx context.Context, jobID string) (*job.Job, error) {
	var j job.Job
	var payloadStr string
	err := s.pg.QueryRow(ctx, `
		SELECT id, task_type, payload, status, created_at, retries, updated_at
		FROM jobs
		WHERE id = $1
	`, jobID).Scan(&j.ID, &j.TaskType, &payloadStr, &j.Status, &j.CreatedAt, &j.Retries, &j.UpdatedAt)
	if err != nil {
		return nil, err
	}
	json.Unmarshal([]byte(payloadStr), &j.Payload)
	return &j, nil
}

func (s *Store) ListJobs(ctx context.Context, status string, limit int) ([]job.Job, error) {
	query := `SELECT id, task_type, payload, status, created_at, retries, updated_at FROM jobs`
	var args []interface{}
	argIdx := 1

	if status != "" {
		query += fmt.Sprintf(" WHERE status = $%d", argIdx)
		args = append(args, status)
		argIdx++
	}

	query += fmt.Sprintf(" ORDER BY created_at DESC LIMIT $%d", argIdx)
	args = append(args, limit)

	rows, err := s.pg.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []job.Job
	for rows.Next() {
		var j job.Job
		var payloadStr string
		err := rows.Scan(&j.ID, &j.TaskType, &payloadStr, &j.Status, &j.CreatedAt, &j.Retries, &j.UpdatedAt)
		if err != nil {
			return nil, err
		}
		json.Unmarshal([]byte(payloadStr), &j.Payload)
		jobs = append(jobs, j)
	}
	return jobs, nil
}

func (s *Store) IncrementRetry(ctx context.Context, jobID string) error {
	_, err := s.pg.Exec(ctx, `
		UPDATE jobs 
		SET retries = retries + 1, updated_at = now()
		WHERE id = $1
	`, jobID)
	return err
}

func (s *Store) UpdateJobStatus(ctx context.Context, jobID string, status job.JobStatus) error {
	_, err := s.pg.Exec(ctx, `
		UPDATE jobs 
		SET status = $1, updated_at = now()
		WHERE id = $2
	`, status, jobID)
	return err
}
