package handler

import (
	"encoding/json"
	"net/http"
	"predix/internal/dto"
	"predix/internal/engine"
	"predix/pkg/redis"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

func (h *Handler) CreateOrder(c *gin.Context) {
	userID, exists := c.Get("userID")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized", "success": false})
		return
	}

	var req dto.CreateOrderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error(), "success": false})
		return
	}

	if req.OrderType == "LIMIT" && req.Price <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "price is required for LIMIT orders", "success": false})
		return
	}

	eventID, err := uuid.Parse(req.EventID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid event ID", "success": false})
		return
	}

	event, err := h.Queries.GetEventByID(c.Request.Context(), eventID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "event not found or not active", "success": false})
		return
	}
	if !event.IsActive.Bool {
		c.JSON(http.StatusBadRequest, gin.H{"error": "event is not active", "success": false})
		return
	}

	orderID := req.OrderID
	if orderID == "" {
		orderID = uuid.NewString()
	}

	payload := map[string]interface{}{
		"orderId":   orderID,
		"eventId":   req.EventID,
		"price":     engine.ScalePrice(req.Price),
		"quantity":  req.Quantity,
		"side":      req.Side,
		"outcome":   req.Outcome,
		"orderType": req.OrderType,
		"userId":    userID,
	}
	payloadBytes, _ := json.Marshal(payload)

	msg := redis.MessageToEngine{
		Type:    "CREATE_ORDER",
		Payload: payloadBytes,
	}

	resp, err := h.RedisManager.SendAndAwait(c.Request.Context(), req.EventID, msg)
	if err != nil || !resp.Success {
		c.JSON(http.StatusBadRequest, gin.H{"error": "orderbook not responding", "success": false})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "data": resp.Data})
}

func (h *Handler) DeleteOrder(c *gin.Context) {
	orderID := c.Param("orderId")

	userID, exists := c.Get("userID")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized", "success": false})
		return
	}

	oid, err := uuid.Parse(orderID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid order ID", "success": false})
		return
	}

	order, err := h.Queries.GetOrderByID(c.Request.Context(), oid)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "order not found", "success": false})
		return
	}

	if order.UserID.String() != userID {
		c.JSON(http.StatusForbidden, gin.H{"error": "you are not authorized to cancel this order", "success": false})
		return
	}

	payload := map[string]interface{}{
		"orderId": orderID,
		"eventId": order.EventID.String(),
	}
	payloadBytes, _ := json.Marshal(payload)

	msg := redis.MessageToEngine{
		Type:    "CANCEL_ORDER",
		Payload: payloadBytes,
	}

	resp, err := h.RedisManager.SendAndAwait(c.Request.Context(), order.EventID.String(), msg)
	if err != nil || !resp.Success {
		c.JSON(http.StatusBadRequest, gin.H{"error": "orderbook not responding", "success": false})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "data": resp.Data})
}