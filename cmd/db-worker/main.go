package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"predix/internal/dbworker"
	"predix/internal/observability"
	"predix/internal/repository"
	"predix/pkg/config"
	"predix/pkg/redis"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	cfg := config.Load()

	ctx := context.Background()

	db, err := pgxpool.New(
		ctx,
		cfg.DatabaseURL,
	)
	if err != nil {
		log.Fatal("database connection failed:", err)
	}

	defer db.Close()

	redisManager := redis.NewRedisManager(
		cfg.RedisURL,
		"",
	)

	defer redisManager.Close()

	queries := repository.New(db)

	worker := dbworker.NewWithStreams(
		redisManager.GetClient(),
		db,
		queries,
		cfg.DBStream,
		cfg.DBGroup,
	)

	// P3.4/P3.5: metrics + health for the DB worker process.
	ops := observability.NewOps(nil)

	ops.AddCheck("redis", func(ctx context.Context) error {
		return redisManager.GetClient().Ping(ctx).Err()
	})

	ops.AddCheck("postgres", func(ctx context.Context) error {
		return db.Ping(ctx)
	})

	opsCtx, opsCancel := context.WithCancel(ctx)
	defer opsCancel()

	go func() {
		if err := ops.Run(opsCtx, ":"+cfg.OpsPort); err != nil {
			log.Printf("ops server stopped: %v", err)
		}
	}()

	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()

	workerDone := make(chan error, 1)

	log.Println("DB worker started")

	go func() {
		workerDone <- worker.Run(workerCtx)
	}()

	signalChan := make(chan os.Signal, 1)

	signal.Notify(
		signalChan,
		syscall.SIGINT,
		syscall.SIGTERM,
	)

	<-signalChan

	log.Println("shutting down DB worker")

	opsCancel()
	workerCancel()

	// Bound the worker drain so shutdown always completes (P3.6).
	select {
	case err := <-workerDone:
		if err != nil {
			log.Printf("DB worker stopped with error: %v", err)
		}
	case <-time.After(10 * time.Second):
		log.Println("DB worker drain timed out")
	}
}
