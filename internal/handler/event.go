package handler

import (
	"encoding/json"
	"net/http"
	"predix/internal/dto"
	"predix/internal/repository"
	"predix/pkg/redis"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

func (h *Handler) GetEvents(c *gin.Context) {
	events, err := h.Queries.GetActiveEvents(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch events"})
		return
	}
	if len(events) == 0 {
		c.JSON(http.StatusOK, gin.H{"success": true, "data": []interface{}{}})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": events})
}

func (h *Handler) GetEvent(c *gin.Context) {
	eventID := c.Param("id")
	id, err := uuid.Parse(eventID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid event ID", "success": false})
		return
	}
	event, err := h.Queries.GetEventByID(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "event not found", "success": false})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": event})
}

func (h *Handler) CreateEvent(c *gin.Context) {
	var req dto.CreateEventRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error(), "success": false})
		return
	}

	expiresAt, err := time.Parse(time.RFC3339, req.ExpiresAt)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid expiresAt format, use RFC3339", "success": false})
		return
	}

	// After sqlc override, ExpiresAt is time.Time
	event, err := h.Queries.CreateEvent(c.Request.Context(), repository.CreateEventParams{
		Title:       req.Title,
		Description: req.Description,
		Question:    req.Question,
		Thumbnail:   req.ImageURL,
		ExpiresAt: pgtype.Timestamptz{
			Time:  expiresAt,
			Valid: true, // Tells pgx this is NOT a NULL database value
		},
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create event", "success": false})
		return
	}

	payload := map[string]interface{}{
		"eventId":   event.ID.String(),
		"title":     event.Title,
		"expiresAt": event.ExpiresAt.Time.Format(time.RFC3339),
	}
	payloadBytes, _ := json.Marshal(payload)

	msg := redis.MessageToEngine{
		Type:    "CREATE_EVENT",
		Payload: payloadBytes,
	}

	resp, err := h.RedisManager.SendAndAwait(c.Request.Context(), event.ID.String(), msg)
	if err != nil || !resp.Success {
		// Rollback
		h.Queries.DeleteEvent(c.Request.Context(), event.ID)
		c.JSON(http.StatusBadRequest, gin.H{"error": "orderbook not responding", "success": false})
		return
	}

	c.JSON(http.StatusCreated, gin.H{"success": true, "data": event})
}
