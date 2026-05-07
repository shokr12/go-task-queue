CREATE TABLE IF NOT EXISTS jobs (
    id SERIAL PRIMARY KEY,
    task_type VARCHAR(255) NOT NULL,
    payload TEXT NOT NULL DEFAULT '{}',
    status VARCHAR(255) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    retries INT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status);