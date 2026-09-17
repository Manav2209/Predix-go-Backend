package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"predix/internal/engine"
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

	for pid := 0; pid < cfg.PartitionCount; pid++ {
		if err := eng.AddPartition(pid); err != nil {
			log.Fatal("failed to add partition:", err)
		}
	}

	eng.Start()

	log.Println("Orderbook engine is running.")

	sigChan := make(chan os.Signal, 1)

	signal.Notify(
		sigChan,
		syscall.SIGINT,
		syscall.SIGTERM,
	)

	<-sigChan

	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancel()

	eng.Shutdown(ctx)
}