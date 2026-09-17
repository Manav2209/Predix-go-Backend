package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"predix/internal/handler"
	"predix/internal/observability"
	"predix/internal/partition"
	"predix/internal/repository"
	"predix/internal/router"
	"predix/pkg/auth"
	"predix/pkg/config"
	"predix/pkg/redis"

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

	redisManager.SetRouter(partition.NewRouter(
		cfg.PartitionCount,
		cfg.CommandStreamPrefix,
	))

	queries := repository.New(conn)

	// Handler with dependencies
	h := handler.NewHandler(queries, redisManager)

	// P3.4/P3.5: metrics + health on the API server.
	ops := observability.NewOps(nil)

	ops.AddCheck("redis", func(ctx context.Context) error {
		return redisManager.GetClient().Ping(ctx).Err()
	})

	ops.AddCheck("postgres", func(ctx context.Context) error {
		return conn.Ping(ctx)
	})

	// Gin
	r := gin.Default()

	// Routes
	router.SetupRoutes(r, h)

	r.GET("/health/live", gin.WrapF(observability.LiveHandler()))
	r.GET("/health/ready", gin.WrapF(ops.ReadyHandler()))
	r.GET("/metrics", gin.WrapH(ops.MetricsHandler()))

	srv := &http.Server{
		Addr:              ":" + cfg.APIPort,
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("API server listening on :%s", cfg.APIPort)

		if err := srv.ListenAndServe(); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	// Graceful shutdown (P3.6).
	sig := make(chan os.Signal, 1)

	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("shutting down API server")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("API shutdown error: %v", err)
	}
}
