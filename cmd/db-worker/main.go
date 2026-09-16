package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"predix/internal/dbworker"
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

	worker := dbworker.New(
		redisManager.GetClient(),
		db,
		queries,
	)

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		if err := worker.Run(workerCtx); err != nil {
			log.Println(
				"DB worker stopped:",
				err,
			)
		}
	}()

	signalChan := make(chan os.Signal, 1)

	signal.Notify(
		signalChan,
		syscall.SIGINT,
		syscall.SIGTERM,
	)

	<-signalChan

	log.Println("shutting down DB worker")

	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer shutdownCancel()

	<-shutdownCtx.Done()
}