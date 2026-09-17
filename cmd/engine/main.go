package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"predix/internal/engine"
	"predix/internal/observability"
	"predix/internal/partition"
	"predix/pkg/config"
	"predix/pkg/redis"
)

func main() {
	cfg := config.Load()

	redisManager := redis.NewRedisManager(
		cfg.RedisURL,
		"",
	)
	defer redisManager.Close()

	eng, err := engine.NewEngine(redisManager)
	if err != nil {
		log.Fatal("failed to create engine:", err)
	}

	eng.SetRouter(partition.NewRouter(
		cfg.PartitionCount,
		cfg.CommandStreamPrefix,
	))

	eng.SetLeaseSettings(
		cfg.EngineID,
		cfg.LeaseTTL,
		cfg.LeaseRenewInterval,
	)

	eng.SetStreamNames(cfg.DBStream, cfg.WSStream)

	for pid := 0; pid < cfg.PartitionCount; pid++ {
		if err := eng.AddPartition(pid); err != nil {
			log.Fatal("failed to add partition:", err)
		}
	}

	eng.Start()

	log.Println("Orderbook engine is running.")

	// P3.4/P3.5: metrics + health for the engine process.
	ops := observability.NewOps(eng.MetricsRegistry())

	ops.AddCheck("redis", func(ctx context.Context) error {
		return redisManager.GetClient().Ping(ctx).Err()
	})

	ops.AddCheck("partition-ownership", func(context.Context) error {
		if eng.OwnedPartitions() == 0 {
			return errors.New("no partition lease held")
		}
		return nil
	})

	opsCtx, opsCancel := context.WithCancel(context.Background())
	defer opsCancel()

	go func() {
		if err := ops.Run(opsCtx, ":"+cfg.OpsPort); err != nil {
			log.Printf("ops server stopped: %v", err)
		}
	}()

	sigChan := make(chan os.Signal, 1)

	signal.Notify(
		sigChan,
		syscall.SIGINT,
		syscall.SIGTERM,
	)

	<-sigChan

	opsCancel()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancel()

	eng.Shutdown(ctx)
}
