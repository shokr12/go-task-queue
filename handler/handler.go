package handler

import (
	"encoding/json"
	"net/http"
	"strconv"
	"task-queue/job"
	"task-queue/store"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

type Handler struct {
	store *store.Store
	redis *redis.Client
}

func NewHandler(s *store.Store, r *redis.Client) *Handler {
	return &Handler{store: s, redis: r}
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
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	j := job.NewJob(req.TaskType, req.Payload)
	if err := h.store.CreateJob(c.Request.Context(), j); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to save job: " + err.Error()})
		return
	}

	data, _ := json.Marshal(j)
	h.redis.RPush(c.Request.Context(), "job_queue", data)

	c.JSON(http.StatusCreated, j)
}

func (h *Handler) GetJob(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid job id"})
		return
	}

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
