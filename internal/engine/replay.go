package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"predix/pkg/redis"

	rd "github.com/redis/go-redis/v9"
)

// replayCommandLog re-executes every command in the partition's durable
// command stream, in stream order, to reconstruct engine state (markets,
// orderbooks, orders and the ledger) after a restart.
//
// Determinism relies on the command handlers being idempotent: re-creating an
// event or order that already exists is a no-op, and duplicate cancellations
// are rejected. Event emission is suppressed during replay because the DB
// projection already contains those effects.
func (e *Engine) replayCommandLog(ctx context.Context, partitionID int) error {
	if e.redisManager == nil {
		return fmt.Errorf("replay requires a Redis manager")
	}

	client := e.redisManager.GetClient()

	if client == nil {
		return fmt.Errorf("replay requires a Redis client")
	}

	entries, err := client.XRange(
		ctx,
		e.router.Stream(partitionID),
		"-",
		"+",
	).Result()

	if err != nil {
		return err
	}

	// Validate the complete log before mutating state. Corrupt durable input
	// blocks recovery instead of silently creating a partial order book.
	commands := make([]redis.CommandEnvelope, 0, len(entries))
	for _, msg := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		env, err := decodeReplayCommand(msg)
		if err != nil {
			return err
		}
		commands = append(commands, env)
	}

	e.mu.Lock()
	e.replaying[partitionID] = true
	e.mu.Unlock()

	defer func() {
		e.mu.Lock()
		delete(e.replaying, partitionID)
		e.mu.Unlock()
	}()

	e.logEvent("event_replay_start", "partitionId", partitionID)

	start := time.Now()

	for _, env := range commands {
		if err := ctx.Err(); err != nil {
			return err
		}
		e.applyCommand(partitionID, env)
	}

	e.metrics.ReplayLatency.Observe(time.Since(start).Seconds())

	elapsed := time.Since(start)

	e.logEvent("event_replay_complete",
		"partitionId", partitionID,
		"commands", len(entries),
		"elapsedMs", elapsed.Milliseconds(),
	)

	log.Printf(
		"replayed %d command(s) from stream (partition %d)",
		len(entries),
		partitionID,
	)

	return nil
}

func decodeReplayCommand(msg rd.XMessage) (redis.CommandEnvelope, error) {
	var env redis.CommandEnvelope
	raw, ok := msg.Values["command"].(string)
	if !ok || raw == "" {
		return env, fmt.Errorf("replay entry %s: missing command", msg.ID)
	}
	if err := json.Unmarshal([]byte(raw), &env); err != nil {
		return env, fmt.Errorf("replay entry %s: invalid envelope: %w", msg.ID, err)
	}
	switch redis.CommandType(env.Type) {
	case redis.CreateOrderCommand, redis.CancelOrderCommand, redis.GetDepthCommand, redis.GetOpenOrdersCommand, redis.CreateEventCommand, redis.UserCreatedCommand:
	default:
		return env, fmt.Errorf("replay entry %s: unknown command type %q", msg.ID, env.Type)
	}
	var payload struct {
		OrderID   string    `json:"orderId"`
		EventID   string    `json:"eventId"`
		UserID    string    `json:"userId"`
		OrderType string    `json:"orderType"`
		Outcome   string    `json:"outcome"`
		Side      string    `json:"side"`
		Quantity  int64     `json:"quantity"`
		Price     int64     `json:"price"`
		ExpiresAt time.Time `json:"expiresAt"`
	}
	if len(env.Payload) == 0 || string(env.Payload) == "null" {
		return env, fmt.Errorf("replay entry %s: missing payload", msg.ID)
	}
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		return env, fmt.Errorf("replay entry %s: invalid payload: %w", msg.ID, err)
	}
	if env.Type == string(redis.CreateOrderCommand) && payload.OrderID == "" {
		return env, fmt.Errorf("replay entry %s: orderId required for deterministic recovery", msg.ID)
	}
	// Business rejections are valid commands in the log; handlers must reproduce
	// them. They are distinct from unreadable input or unsupported commands.
	return env, nil
}

// applyCommand re-executes a single command envelope. Replay and live
// consumption share this entry point so behaviour cannot drift.
func (e *Engine) applyCommand(partitionID int, env redis.CommandEnvelope) {
	if env.Type == "" {
		return
	}

	e.handleMessage(partitionID, redis.MessageToEngine{
		Type:    env.Type,
		Payload: env.Payload,
	})
}
