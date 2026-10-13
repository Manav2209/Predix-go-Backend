package main

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/prometheus/client_golang/prometheus"
)

func main() {
	if err := run(); err != nil {
		log.Printf("DB worker failed: %v", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load("db-worker")
	if err != nil {
		return err
	}

	ctx := context.Background()

	db, err := pgxpool.New(
		ctx,
		cfg.DatabaseURL,
	)
	if err != nil {
		return fmt.Errorf("database connection failed: %w", err)
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
	ops.AddCheck("consumer", worker.Ready)
	failures := prometheus.NewCounter(prometheus.CounterOpts{Name: "db_worker_failures_total", Help: "Unexpected DB consumer terminations."})
	ops.Registry().MustRegister(failures)

	ops.AddCheck("redis", func(ctx context.Context) error {
		return redisManager.GetClient().Ping(ctx).Err()
	})

	ops.AddCheck("postgres", func(ctx context.Context) error {
		return db.Ping(ctx)
	})

	opsCtx, opsCancel := context.WithCancel(ctx)
	defer opsCancel()

	go func() {
		if err := ops.Run(opsCtx, cfg.OpsAddress()); err != nil {
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
	defer signal.Stop(signalChan)

	err = waitForWorker(signalChan, workerDone, workerCancel, 10*time.Second)
	if err != nil {
		failures.Inc()
	}
	return err
}

func waitForWorker(signals <-chan os.Signal, done <-chan error, cancel context.CancelFunc, timeout time.Duration) error {
	select {
	case err := <-done:
		if err == nil {
			err = errors.New("consumer stopped unexpectedly")
		}
		return err
	case <-signals:
		log.Println("shutting down DB worker")
		cancel()
	}
	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		return errors.New("DB worker drain timed out")
	}
}
