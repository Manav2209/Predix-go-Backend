package handler

import (
	"encoding/json"
	"net/http"
	"predix/pkg/redis"
	
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

func (h *Handler) GetOrderbook(c *gin.Context) {
	eventID := c.Param("eventId")

	eid, err := uuid.Parse(eventID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid event ID", "success": false})
		return
	}

	_, err = h.Queries.GetEventByID(c.Request.Context(), eid)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "event not found", "success": false})
		return
	}

	payload := map[string]interface{}{
		"eventId": eventID,
	}
	payloadBytes, _ := json.Marshal(payload)

	msg := redis.MessageToEngine{
		Type:    "GET_DEPTH",
		Payload: payloadBytes,
	}

	resp, err := h.RedisManager.SendAndAwait(c.Request.Context(), msg)
	if err != nil || !resp.Success {
		c.JSON(http.StatusBadRequest, gin.H{"error": "orderbook not responding", "success": false})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "data": resp.Data})
}

func (h *Handler) GetOrderbookDepth(c *gin.Context) {
	eventID := c.Param("eventId")

	eid, err := uuid.Parse(eventID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid event ID", "success": false})
		return
	}

	_, err = h.Queries.GetEventByID(c.Request.Context(), eid)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "event not found", "success": false})
		return
	}

	payload := map[string]interface{}{
		"eventId": eventID,
	}
	payloadBytes, _ := json.Marshal(payload)

	msg := redis.MessageToEngine{
		Type:    "GET_DEPTH",
		Payload: payloadBytes,
	}

	resp, err := h.RedisManager.SendAndAwait(c.Request.Context(), msg)
	if err != nil || !resp.Success {
		c.JSON(http.StatusBadRequest, gin.H{"error": "orderbook not responding", "success": false})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "data": resp.Data})
}