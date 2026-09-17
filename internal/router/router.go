package router

import (
	"github.com/gin-gonic/gin"
	"predix/internal/handler"
	"predix/internal/middleware"
)

func SetupRoutes(r *gin.Engine, h *handler.Handler) {
	// Health
	r.GET("/", func(c *gin.Context) {
		c.JSON(200, gin.H{"message": "hello"})
	})
	r.GET("/ping", func(c *gin.Context) {
		c.String(200, "pong")
	})

	// Auth (public)
	auth := r.Group("/auth")
	{
		auth.POST("/signup", h.Signup)
		auth.POST("/signin", h.Signin)
	}

	// Protected routes
	protected := r.Group("/")
	protected.Use(middleware.AuthMiddleware())
	{
		// User
		protected.GET("/me", h.Me)
		protected.GET("/position", h.GetPosition)
		protected.GET("/balances", h.GetBalances)

		// Events
		protected.GET("/events", h.GetEvents)
		protected.GET("/event/:id", h.GetEvent)
		protected.POST("/event", h.CreateEvent)

		// Orders
		protected.POST("/order", h.CreateOrder)
		protected.DELETE("/order/:orderId", h.DeleteOrder)

		// Orderbook
		protected.GET("/orderbook/:eventId", h.GetOrderbook)
		protected.GET("/orderbook/:eventId/depth", h.GetOrderbookDepth)
	}
}
