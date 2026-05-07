package store

import (
	"context"
	"encoding/json"
	"fmt"
	"task-queue/job"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pg *pgxpool.Pool
}

func NewStore(dsn string) (*Store, error) {
	pg, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %v", err)
	}
	err = pg.Ping(context.Background())
	if err != nil {
		return nil, fmt.Errorf("unable to ping database: %v", err)
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

func (s *Store) CreateJob(ctx context.Context, j *job.Job) error {
	payloadJSON, err := json.Marshal(j.Payload)
	if err != nil {
		return err
	}
	err = s.pg.QueryRow(ctx, `
		INSERT INTO jobs (task_type, payload, status, created_at, retries, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id
	`, j.TaskType, string(payloadJSON), j.Status, j.CreatedAt, j.Retries, j.UpdatedAt).Scan(&j.ID)
	return err
}

func (s *Store) UpdateJob(ctx context.Context, j *job.Job) error {
	_, err := s.pg.Exec(ctx, `
		UPDATE jobs 
		SET status = $1, retries = $2, updated_at = $3
		WHERE id = $4
	`, j.Status, j.Retries, j.UpdatedAt, j.ID)
	return err
}

func (s *Store) GetJobByID(ctx context.Context, jobID int) (*job.Job, error) {
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

func (s *Store) IncrementRetry(ctx context.Context, jobID int) error {
	_, err := s.pg.Exec(ctx, `
		UPDATE jobs 
		SET retries = retries + 1, updated_at = now()
		WHERE id = $1
	`, jobID)
	return err
}

func (s *Store) UpdateJobStatus(ctx context.Context, jobID int, status job.JobStatus) error {
	_, err := s.pg.Exec(ctx, `
		UPDATE jobs 
		SET status = $1, updated_at = now()
		WHERE id = $2
	`, status, jobID)
	return err
}
