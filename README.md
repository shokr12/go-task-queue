# Task queue

A robust, asynchronous task queue system built with Go, PostgreSQL, and Redis. This project demonstrates a production-ready pattern for handling background jobs with automatic retries, persistence, and a RESTful API.

## 🚀 Features

- **Asynchronous Processing**: Offload heavy tasks to background workers.
- **Persistence**: Job states and payloads are stored in PostgreSQL.
- **Reliability**: Automatic retries (up to 3 times) for failed jobs.
- **REST API**: Simple endpoints to create, track, and list jobs.
- **Graceful Shutdown**: Handles OS signals to finish current work before stopping.

## 🛠️ Technology Stack

- **Go**: Core application logic.
- **Gin**: High-performance HTTP web framework.
- **PostgreSQL**: Relational database for job persistence.
- **Redis**: Fast, in-memory message broker for the task queue.
- **Docker**: Containerized infrastructure.

## 📋 Prerequisites

- [Go](https://golang.org/doc/install) (1.21 or later)
- [Docker & Docker Desktop](https://www.docker.com/products/docker-desktop)

## 🚦 Getting Started

### 1. Infrastructure Setup

Start the PostgreSQL and Redis containers using Docker Compose:

```powershell
docker compose up -d
```

### 2. Initialize the Database

If you've run the project before with different settings, ensure the database is clean:

```powershell
docker exec task_queue-postgres-1 psql -U postgres -c "CREATE DATABASE task_queue;"
```

_(Note: If the database already exists, you can skip this. If you need to reset the table schema, run `DROP TABLE jobs;` first.)_

### 3. Install Dependencies

```powershell
go mod tidy
```

### 4. Run the Application

```powershell
go run main.go
```

The server will start on `http://localhost:8080`.

## 📡 API Endpoints

### Create a Job

**POST** `/api/jobs`

```json
{
  "task_type": "send_email",
  "payload": {
    "to": "user@example.com",
    "subject": "Hello World"
  }
}
```

_Supported task types: `send_email`, `resize_image`, `generate_pdf`_

### Get Job Status

**GET** `/api/jobs/:id`
Returns the status, retry count, and metadata for a specific job.

### List All Jobs

**GET** `/api/jobs`
**GET** `/api/jobs?status=completed`

## 🏗️ Project Structure

- `/job`: Job models and constants.
- `/store`: Database logic (PostgreSQL).
- `/handler`: HTTP API handlers (Gin).
- `/worker`: Background processing logic.
- `main.go`: Application entry point and wiring.
- `schema.sql`: Database migration script.

## 🔄 How it Works

1. **API receives a request** and generates a **UUID** instantly.
2. **Async Ingestion**: The request is handed off to an internal **Buffered Producer** (lock-free) and returns `201 Created` immediately (latency < 1ms).
3. **Batch Persistence**: The Producer gathers jobs and performs **Batched Inserts** (1,000 at a time) into PostgreSQL using `pgx.Batch`.
4. **Pipelined Queuing**: The Producer pushes jobs to Redis in batches using **Redis Pipelining**.
5. **Concurrent Workers**: A pool of thousands of goroutines (controlled via semaphores) processes jobs in parallel.

## ⚡ Performance Benchmarks

Initial implementation: **~364 jobs/sec**
Optimized Architecture: **~2,762 jobs/sec** (50,000 jobs in 18s)

### Optimization Keys:
- **Zero-Wait Ingestion**: Decoupled HTTP response from DB persistence.
- **Batching**: Reduced DB network round-trips by 99%.
- **High Concurrency**: Tuned worker pool to handle 20,000+ concurrent tasks.
- **DB Tuning**: Disabled `synchronous_commit` for ultra-high-speed inserts.

