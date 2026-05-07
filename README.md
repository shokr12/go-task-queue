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

1. **API receives a request** and validates the task type.
2. **Job is persisted** in PostgreSQL with a `pending` status and an auto-incremented ID.
3. **Job is pushed** to a Redis list (`job_queue`).
4. **Worker pops** the job from Redis using `BLPop` (blocking pop).
5. **Worker executes** the task and updates the status in PostgreSQL.
6. **On failure**, the worker increments the retry count and re-queues the job if it hasn't reached the limit.
