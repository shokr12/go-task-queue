package handler

import (
	"log"
	"net/http"
	"task-queue/job"
	"task-queue/queue"
	"task-queue/store"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

type Handler struct {
	store    *store.Store
	redis    *redis.Client
	producer *queue.Producer
}

func NewHandler(s *store.Store, r *redis.Client, p *queue.Producer) *Handler {
	return &Handler{store: s, redis: r, producer: p}
}

func (h *Handler) RegisterRoutes(r *gin.Engine) {
	api := r.Group("/api")
	{
		api.POST("/jobs", h.CreateJob)
		api.GET("/jobs", h.ListJobs)
		api.GET("/jobs/:id", h.GetJob)
	}
}

func (h *Handler) CreateJob(c *gin.Context) {
	var req job.CreateJobRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		log.Printf("JSON Binding Error: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	j := job.NewJob(req.TaskType, req.Payload)


	h.producer.Enqueue(j)

	c.JSON(http.StatusCreated, j)
}

func (h *Handler) GetJob(c *gin.Context) {
	id := c.Param("id")

	j, err := h.store.GetJobByID(c.Request.Context(), id)

	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
		return
	}

	c.JSON(http.StatusOK, j)
}

func (h *Handler) ListJobs(c *gin.Context) {
	status := c.Query("status")
	jobs, err := h.store.ListJobs(c.Request.Context(), status, 100)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, jobs)
}
