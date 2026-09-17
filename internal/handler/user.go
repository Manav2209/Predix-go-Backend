package handler

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

func (h *Handler) GetPosition(c *gin.Context) {
	userID, exists := c.Get("userID")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized", "success": false})
		return
	}

	// TODO: Get user positions from DB/engine
	c.JSON(http.StatusOK, gin.H{"success": true, "data": gin.H{"message": "get position for user: " + userID.(string)}})
}

func (h *Handler) GetBalances(c *gin.Context) {
	userID, exists := c.Get("userID")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized", "success": false})
		return
	}

	// TODO: Get user balances from DB/engine
	c.JSON(http.StatusOK, gin.H{"success": true, "data": gin.H{"message": "get balances for user: " + userID.(string)}})
}
