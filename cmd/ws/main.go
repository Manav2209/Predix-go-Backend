package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

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

	ctx, cancel := context.WithCancel(
		context.Background(),
	)

	defer cancel()

	go func() {

		if err := redisSubscriber.Run(ctx); err != nil {
			log.Printf(
				"redis subscriber stopped: %v",
				err,
			)
		}

	}()

	mux := http.NewServeMux()

	mux.HandleFunc(
		"/ws",
		wsServer.Handle,
	)

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,

		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {

		log.Println(
			"WebSocket server running on :8080",
		)

		if err := server.ListenAndServe(); err != nil &&
			err != http.ErrServerClosed {

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