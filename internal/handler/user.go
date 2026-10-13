package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

func (h *Handler) GetPosition(c *gin.Context) {
	userID, exists := c.Get("userID")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized", "success": false})
		return
	}

	id, err := uuid.Parse(userID.(string))
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid user ID", "success": false})
		return
	}
	positions, err := h.Queries.UserPositions(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch positions", "success": false})
		return
	}
	type positionResponse struct {
		EventID   string      `json:"eventId"`
		Outcome   string      `json:"outcome"`
		Shares    json.Number `json:"shares"`
		AvgPrice  json.Number `json:"avgPrice"`
		UpdatedAt time.Time   `json:"updatedAt"`
	}
	response := make([]positionResponse, 0, len(positions))
	for _, position := range positions {
		shares, err := numericNumber(position.Shares)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "invalid position shares", "success": false})
			return
		}
		price, err := numericNumber(position.AvgPrice)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "invalid position price", "success": false})
			return
		}
		response = append(response, positionResponse{EventID: position.EventID.String(), Outcome: position.Outcome, Shares: shares, AvgPrice: price, UpdatedAt: position.UpdatedAt})
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": response})
}

func (h *Handler) GetBalances(c *gin.Context) {
	userID, exists := c.Get("userID")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized", "success": false})
		return
	}

	id, err := uuid.Parse(userID.(string))
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid user ID", "success": false})
		return
	}
	balance, err := h.Queries.GetUserBalance(c.Request.Context(), id)
	if errors.Is(err, pgx.ErrNoRows) {
		c.JSON(http.StatusNotFound, gin.H{"error": "user not found", "success": false})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch balance", "success": false})
		return
	}
	number, err := numericNumber(balance)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "invalid balance", "success": false})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": gin.H{"userId": id.String(), "balance": number}})
}

// Preserve the exact DECIMAL value as a JSON number without float conversion.
func numericNumber(value pgtype.Numeric) (json.Number, error) {
	raw, err := value.MarshalJSON()
	if err != nil {
		return "", err
	}
	if !value.Valid || len(raw) == 0 || raw[0] == '"' {
		return "", errors.New("invalid numeric value")
	}
	return json.Number(string(raw)), nil
}
