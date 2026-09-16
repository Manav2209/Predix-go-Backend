package main

import (
	"context"
	"log"
	"predix/internal/handler"
	"predix/internal/repository"
	"predix/internal/router"
	"predix/pkg/redis"
	"predix/pkg/auth" 
	"predix/pkg/config"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	
	// Load config
	cfg := config.Load()

	auth.Init(cfg.JWTSecret)

	ctx := context.Background()
	// PostgreSQL
    conn, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatal("DB connection failed:", err)
	}
	defer conn.Close()

	// Redis
    redisManager := redis.NewRedisManager(cfg.RedisURL, "")
	defer redisManager.Close()

	queries := repository.New(conn)

	// Handler with dependencies
	h := handler.NewHandler(queries, redisManager)

	// Gin
	r := gin.Default()

	// Routes
	router.SetupRoutes(r, h)

	r.Run(":3000")
}