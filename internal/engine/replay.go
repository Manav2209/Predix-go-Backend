package engine

import (
	"encoding/json"
	"log"

	"predix/pkg/redis"
)

// replayCommandLog re-executes every command in the durable commands stream,
// in stream order, to reconstruct engine state (markets, orderbooks, orders
// and the ledger) after a restart.
//
// Determinism relies on the command handlers being idempotent: re-creating an
// event or order that already exists is a no-op, and duplicate cancellations
// are rejected. Event emission is suppressed during replay because the DB
// projection already contains those effects.
func (e *Engine) replayCommandLog() error {
	if e.redisManager == nil {
		return nil
	}

	client := e.redisManager.GetClient()

	if client == nil {
		return nil
	}

	entries, err := client.XRange(
		e.ctx,
		redis.CommandStream,
		"-",
		"+",
	).Result()

	if err != nil {
		return err
	}

	e.mu.Lock()
	e.replaying = true
	e.mu.Unlock()

	defer func() {
		e.mu.Lock()
		e.replaying = false
		e.mu.Unlock()
	}()

	for _, msg := range entries {

		raw, _ := msg.Values["command"].(string)

		if raw == "" {
			continue
		}

		var env redis.CommandEnvelope

		if err := json.Unmarshal([]byte(raw), &env); err != nil {
			log.Printf("replay: skip unreadable entry %s: %v", msg.ID, err)
			continue
		}

		e.applyCommand(env)
	}

	log.Printf("replayed %d command(s) from stream", len(entries))

	return nil
}

// applyCommand re-executes a single command envelope. Replay and live
// consumption share this entry point so behaviour cannot drift.
func (e *Engine) applyCommand(env redis.CommandEnvelope) {
	if env.Type == "" {
		return
	}

	e.handleMessage(redis.MessageToEngine{
		Type:    env.Type,
		Payload: env.Payload,
	})
}