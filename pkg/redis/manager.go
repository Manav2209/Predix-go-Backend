package redis

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"time"

	"predix/internal/partition"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type MessageToEngine struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// CommandType enumerates the command verbs the engine accepts. These are the
// durable record of every state-changing operation: the partition command
// stream is the source of truth the engine replays to reconstruct its state.
type CommandType string

const (
	CreateOrderCommand   CommandType = "CREATE_ORDER"
	CancelOrderCommand   CommandType = "CANCEL_ORDER"
	GetDepthCommand      CommandType = "GET_DEPTH"
	GetOpenOrdersCommand CommandType = "GET_OPEN_ORDERS"
	CreateEventCommand   CommandType = "CREATE_EVENT"
	UserCreatedCommand   CommandType = "USER_CREATED"
)

// CommandEnvelope is the durable wrapper written to a partition command
// stream. The engine re-executes these in stream order on startup.
type CommandEnvelope struct {
	CommandID string          `json:"commandId"`
	Type      string          `json:"type"`
	EventID   string          `json:"eventId,omitempty"`
	RequestID string          `json:"requestId,omitempty"`
	Sequence  uint64          `json:"sequence"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

type EngineResponse struct {
	Success   bool            `json:"success"`
	Error     string          `json:"error,omitempty"`
	RequestID string          `json:"requestId,omitempty"`
	Data      json.RawMessage `json:"data,omitempty"`
}

type RedisManager struct {
	client    *redis.Client
	publisher *redis.Client

	// router routes commands by eventId to their partition stream.
	router *partition.Router
}

func NewRedisManager(addr, password string) *RedisManager {
	opts := &redis.Options{
		Addr:     addr,
		Password: password,
		DB:       0,
	}
	return &RedisManager{
		client:    redis.NewClient(opts),
		publisher: redis.NewClient(opts),
	}
}

// SetRouter installs the partition router used by every command send.
func (r *RedisManager) SetRouter(router *partition.Router) {
	r.router = router
}

func (r *RedisManager) Router() *partition.Router {
	return r.router
}

// streamFor returns the command stream an envelope must be appended to.
// A nil router conservatively falls back to the legacy unpartitioned stream.
func (r *RedisManager) streamFor(partitionID int) string {
	if r.router == nil {
		return "commands"
	}
	return r.router.Stream(partitionID)
}

// groupFor returns the consumer group for a partition.
func (r *RedisManager) groupFor(partitionID int) string {
	if r.router == nil {
		return "engines"
	}
	return r.router.Group(partitionID)
}

// seqKeyFor returns the per-command sequence counter for a partition.
func (r *RedisManager) seqKeyFor(partitionID int) string {
	if r.router == nil {
		return "commands:sequence"
	}
	return r.router.SequenceKey(partitionID)
}

// partitionFor routes an event (or, in its absence, partition 0) to a
// partition. USER_CREATED has no eventId and is fanned out instead.
func (r *RedisManager) partitionFor(eventID string) int {
	if r.router == nil || eventID == "" {
		return 0
	}
	return r.router.Partition(eventID)
}

func (r *RedisManager) createEnvelope(
	typ CommandType,
	eventID string,
	payload json.RawMessage,
	partitionID int,
) CommandEnvelope {

	seq, err := r.client.Incr(
		context.Background(),
		r.seqKeyFor(partitionID),
	).Uint64()

	if err != nil {
		seq = 0
	}

	requestID := uuid.NewString()

	return CommandEnvelope{
		CommandID: requestID,
		Type:      string(typ),
		EventID:   eventID,
		RequestID: requestID,
		Sequence:  seq,
		Timestamp: time.Now().UTC(),
		Payload:   payload,
	}
}

// RPC reliability knobs (P3.9).
const (
	// DefaultRPCDeadline bounds a request/response round-trip.
	DefaultRPCDeadline = 5 * time.Second
	// MaxAppendRetries bounds how often a durable command append is retried
	// on a transient Redis error. Commands are idempotent, so a retried
	// append is safe.
	MaxAppendRetries = 3
	// AppendRetryBackoff is the delay between append retries.
	AppendRetryBackoff = 200 * time.Millisecond
)

// SendAndAwait writes the command to the partition of eventID and waits for
// the owning engine's response within DefaultRPCDeadline.
func (r *RedisManager) SendAndAwait(
	ctx context.Context,
	eventID string,
	msg MessageToEngine,
) (*EngineResponse, error) {
	return r.SendAndAwaitWithTimeout(ctx, eventID, msg, DefaultRPCDeadline)
}

// SendAndAwaitWithTimeout is SendAndAwait with an explicit response deadline
// (P3.9). The command is appended with a detached (non-cancelable) context so
// it is not lost when the caller gives up; only the reply wait observes ctx.
// Each request carries a requestId echoed back by the engine for correlation.
func (r *RedisManager) SendAndAwaitWithTimeout(
	ctx context.Context,
	eventID string,
	msg MessageToEngine,
	timeout time.Duration,
) (*EngineResponse, error) {

	partitionID := r.partitionFor(eventID)
	clientID := generateClientID()

	// Detached context: the command is appended regardless; only the wait
	// for the reply observes ctx.
	bg := context.Background()

	sub := r.client.Subscribe(bg, clientID)
	defer sub.Close()

	// Confirm the subscription is actually active before publishing, so a
	// fast engine response cannot be published before we are subscribed.
	confirmCtx, confirmCancel := context.WithTimeout(bg, 5*time.Second)
	defer confirmCancel()

	if _, err := sub.Receive(confirmCtx); err != nil {
		return nil, err
	}

	// The envelope stores the raw inner payload (never the MessageToEngine
	// wrapper): the engine reconstructs the message from env.Type +
	// env.Payload, so the wrapper must not leak in as payload.
	envelope := r.createEnvelope(
		CommandType(msg.Type),
		eventID,
		msg.Payload,
		partitionID,
	)

	if err := r.appendCommand(bg, clientID, envelope, partitionID); err != nil {
		return nil, err
	}

	return r.awaitResponse(ctx, sub, envelope.RequestID, timeout)
}

// awaitResponse listens on the request's private channel for the engine
// reply correlated by requestId.
func (r *RedisManager) awaitResponse(
	ctx context.Context,
	sub *redis.PubSub,
	requestID string,
	timeout time.Duration,
) (*EngineResponse, error) {

	msgChan := sub.Channel()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case redisMsg, ok := <-msgChan:
			if !ok {
				return nil, errors.New("response channel closed")
			}

			var resp EngineResponse
			if err := json.Unmarshal([]byte(redisMsg.Payload), &resp); err != nil {
				return nil, err
			}

			// Guard against a stray or replayed response: only the reply
			// carrying this request's id counts (P3.9 correlation).
			if resp.RequestID != requestID {
				continue
			}

			return &resp, nil

		case <-ctx.Done():
			return nil, ctx.Err()

		case <-timer.C:
			return nil, errors.New("timeout waiting for engine response")
		}
	}
}

// appendCommand writes a command envelope to its partition stream with a
// bounded number of retries.
func (r *RedisManager) appendCommand(
	ctx context.Context,
	clientID string,
	env CommandEnvelope,
	partitionID int,
) error {

	envBytes, err := json.Marshal(env)
	if err != nil {
		return err
	}

	args := &redis.XAddArgs{
		Stream: r.streamFor(partitionID),
		Values: map[string]interface{}{
			"clientId": clientID,
			"command":  string(envBytes),
		},
	}

	var lastErr error

	for attempt := 0; attempt < MaxAppendRetries; attempt++ {
		if err := r.publisher.XAdd(ctx, args).Err(); err == nil {
			return nil
		} else {
			lastErr = err
		}

		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(AppendRetryBackoff):
		}
	}

	return lastErr
}

// SendCommand writes a durable command envelope to the partition of eventID
// without waiting for a response.
func (r *RedisManager) SendCommand(
	ctx context.Context,
	eventID string,
	typ CommandType,
	payload json.RawMessage,
) error {

	partitionID := r.partitionFor(eventID)

	env := r.createEnvelope(typ, eventID, payload, partitionID)

	envBytes, err := json.Marshal(env)
	if err != nil {
		return err
	}

	return r.publisher.XAdd(ctx, &redis.XAddArgs{
		Stream: r.streamFor(partitionID),
		Values: map[string]interface{}{
			"clientId": "",
			"command":  string(envBytes),
		},
	}).Err()
}

// SendCommandFanout writes a durable command envelope to EVERY partition
// stream. Used for global control commands that carry no eventId and must be
// visible to every partition (USER_CREATED: each partition grants its own
// copy of the starting balance).
func (r *RedisManager) SendCommandFanout(
	ctx context.Context,
	typ CommandType,
	payload json.RawMessage,
) error {

	if r.router == nil {
		return r.SendCommand(ctx, "", typ, payload)
	}

	for p := 0; p < r.router.Partitions; p++ {

		env := r.createEnvelope(typ, "", payload, p)

		envBytes, err := json.Marshal(env)
		if err != nil {
			return err
		}

		if err := r.publisher.XAdd(ctx, &redis.XAddArgs{
			Stream: r.router.Stream(p),
			Values: map[string]interface{}{
				"clientId": "",
				"command":  string(envBytes),
			},
		}).Err(); err != nil {
			return err
		}
	}

	return nil
}

func generateClientID() string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 20)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return string(b)
}

func (r *RedisManager) Close() error {
	if err := r.client.Close(); err != nil {
		return err
	}
	return r.publisher.Close()
}

// GetClient returns the underlying Redis client (for engine)
func (r *RedisManager) GetClient() *redis.Client {
	return r.client
}
