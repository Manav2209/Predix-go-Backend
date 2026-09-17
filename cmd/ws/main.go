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

	"predix/internal/observability"
	"predix/internal/websocket"
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

	hub := websocket.NewHub()

	wsServer := websocket.NewServer(hub)

	redisSubscriber :=
		websocket.NewRedisSubscriber(
			redisManager.GetClient(),
			hub,
			cfg.WSStream,
		)

	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	go func() {

		if err := redisSubscriber.Run(ctx); err != nil {
			log.Printf(
				"redis subscriber stopped: %v",
				err,
			)
		}

	}()

	// P3.4/P3.5: metrics + health for the WS process.
	ops := observability.NewOps(nil)

	ops.AddCheck("redis", func(ctx context.Context) error {
		return redisManager.GetClient().Ping(ctx).Err()
	})

	opsCtx, opsCancel := context.WithCancel(context.Background())
	defer opsCancel()

	go func() {
		if err := ops.Run(opsCtx, ":"+cfg.OpsPort); err != nil {
			log.Printf("ops server stopped: %v", err)
		}
	}()

	mux := http.NewServeMux()

	mux.HandleFunc(
		"/ws",
		wsServer.Handle,
	)

	server := &http.Server{
		Addr:    ":" + cfg.WSPort,
		Handler: mux,

		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {

		log.Printf(
			"WebSocket server running on :%s",
			cfg.WSPort,
		)

		if err := server.ListenAndServe(); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {

			log.Fatal(err)
		}
	}()

	sig := make(
		chan os.Signal,
		1,
	)

	signal.Notify(
		sig,
		syscall.SIGINT,
		syscall.SIGTERM,
	)

	<-sig

	log.Println(
		"Shutting down WebSocket server",
	)

	opsCancel()
	cancel()

	shutdownCtx, shutdownCancel :=
		context.WithTimeout(
			context.Background(),
			10*time.Second,
		)

	defer shutdownCancel()

	if err := server.Shutdown(
		shutdownCtx,
	); err != nil {

		log.Printf(
			"HTTP shutdown error: %v",
			err,
		)
	}
}
