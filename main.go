package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"task-queue/handler"
	"task-queue/store"
	"task-queue/worker"
	"task-queue/queue"

	"github.com/joho/godotenv"


	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

func main() {
	godotenv.Load()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	dsn := fmt.Sprintf("postgresql://%s:%s@%s:%s/%s?sslmode=disable", os.Getenv("DB_USER"), os.Getenv("DB_PASSWORD"), os.Getenv("DB_HOST"), os.Getenv("DB_PORT"), os.Getenv("DB_NAME"))
	s, err := store.NewStore(dsn)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer s.Close()

	schema, err := os.ReadFile("schema.sql")
	if err != nil {
		log.Fatalf("Failed to read schema.sql: %v", err)
	}
	if err := s.RunMigrations(ctx, string(schema)); err != nil {
		log.Fatalf("Failed to run migrations: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: os.Getenv("REDIS_ADDR"),
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	fmt.Println("Connected to Redis")
	defer rdb.Close()

	w := worker.NewWorker(rdb, s)
	go func() {
		if err := w.Start(ctx); err != nil && ctx.Err() == nil {
			log.Printf("Worker error: %v", err)
		}
	}()

	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()

	p := queue.NewProducer(rdb, s)

	h := handler.NewHandler(s, rdb, p)
	h.RegisterRoutes(r)

	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	fmt.Println("Server starting on :8081")
	go func() {
		if err := r.Run(":8081"); err != nil {
			log.Printf("Failed to start server: %v", err)
		}
	}()



	<-ctx.Done()
	fmt.Println("\nShutting down gracefully...")
}
