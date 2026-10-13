package dbworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/big"
	"strings"
	"sync"
	"time"

	"predix/internal/engine"
	"predix/internal/events"
	"predix/internal/partition"
	"predix/internal/repository"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type Worker struct {
	redis    *redis.Client
	pool     *pgxpool.Pool
	queries  *repository.Queries
	consumer string

	// stream/group identify the durable event stream and its consumer
	// group. Configurable through NewWithStreams (P3.7).
	stream string
	group  string

	mu            sync.RWMutex
	running       bool
	progress      time.Time
	failure       error
	claimCursor   string
	claimInterval time.Duration
	claimIdle     time.Duration
	readBlock     time.Duration
}

func New(
	redisClient *redis.Client,
	pool *pgxpool.Pool,
	queries *repository.Queries,
) *Worker {
	return NewWithStreams(
		redisClient,
		pool,
		queries,
		events.EventStream,
		events.EventGroup,
	)
}

func NewWithStreams(
	redisClient *redis.Client,
	pool *pgxpool.Pool,
	queries *repository.Queries,
	stream string,
	group string,
) *Worker {
	return &Worker{
		redis:         redisClient,
		pool:          pool,
		queries:       queries,
		consumer:      uuid.NewString(),
		stream:        stream,
		group:         group,
		claimCursor:   "0-0",
		claimInterval: 5 * time.Second,
		claimIdle:     10 * time.Second,
		readBlock:     500 * time.Millisecond,
	}
}

// Ready verifies that the consumer is running and making progress, including
// successful empty reads when the stream is idle.
func (w *Worker) Ready(context.Context) error {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.failure != nil {
		return fmt.Errorf("consumer failed: %w", w.failure)
	}
	if !w.running || w.progress.IsZero() {
		return errors.New("consumer is not running")
	}
	if time.Since(w.progress) > 30*time.Second {
		return errors.New("consumer progress stalled")
	}
	return nil
}

func (w *Worker) markProgress() {
	w.mu.Lock()
	w.progress = time.Now()
	w.mu.Unlock()
}

func (w *Worker) Run(ctx context.Context) (runErr error) {
	w.mu.Lock()
	w.running = true
	w.progress = time.Time{}
	w.failure = nil
	w.mu.Unlock()
	defer func() {
		if ctx.Err() != nil {
			runErr = nil
		}
		w.mu.Lock()
		w.running = false
		w.failure = runErr
		w.mu.Unlock()
	}()
	log.Println("DB worker started")

	if err := w.ensureGroup(ctx); err != nil {
		return fmt.Errorf("ensure event group: %w", err)
	}

	nextClaim := time.Time{}
	for ctx.Err() == nil {
		if time.Now().After(nextClaim) {
			if err := w.reclaimPending(ctx); err != nil {
				return fmt.Errorf("reclaim pending events: %w", err)
			}
			nextClaim = time.Now().Add(w.claimInterval)
		}
		streams, err := w.redis.XReadGroup(
			ctx,
			&redis.XReadGroupArgs{
				Group:    w.group,
				Consumer: w.consumer,
				Streams:  []string{w.stream, ">"},
				Count:    10,
				Block:    w.readBlock,
			},
		).Result()

		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, redis.Nil) {
				w.markProgress()
				continue
			}
			return fmt.Errorf("read event stream: %w", err)
		}
		w.markProgress()

		for _, s := range streams {
			for _, msg := range s.Messages {
				if err := w.processWithRetry(ctx, msg); err != nil {
					// Leave the entry pending so it is retried after a redelivery.
					return fmt.Errorf("process stream entry %s: %w", msg.ID, err)
				}
			}
		}
	}
	return nil
}

// Scan a bounded number of pages per pass so new events are not starved. The
// cursor carries over to the next pass until Redis reports the scan complete.
func (w *Worker) reclaimPending(ctx context.Context) error {
	for page := 0; page < 10; page++ {
		messages, cursor, err := w.redis.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream: w.stream, Group: w.group, Consumer: w.consumer,
			MinIdle: w.claimIdle, Start: w.claimCursor, Count: 100,
		}).Result()
		if err != nil {
			return err
		}
		for _, msg := range messages {
			if err := w.processWithRetry(ctx, msg); err != nil {
				return fmt.Errorf("entry %s: %w", msg.ID, err)
			}
		}
		w.claimCursor = cursor
		w.markProgress()
		if cursor == "0-0" {
			break
		}
	}
	return nil
}

type invalidEvent struct{ error }

func (w *Worker) processWithRetry(ctx context.Context, msg redis.XMessage) error {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err = w.processStreamEntry(ctx, msg.ID, msg.Values)
		if err == nil {
			w.markProgress()
			return nil
		}
		var malformed invalidEvent
		if errors.As(err, &malformed) {
			return w.deadLetter(ctx, msg, err)
		}
		if attempt < 2 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(1<<attempt) * 100 * time.Millisecond):
			}
		}
	}
	return fmt.Errorf("failed after 3 attempts: %w", err)
}

// Persist the original entry before acknowledging it. A failed DLQ write
// leaves the source pending. Operational failures are never dead-lettered.
func (w *Worker) deadLetter(ctx context.Context, msg redis.XMessage, cause error) error {
	raw, err := json.Marshal(msg.Values)
	if err != nil {
		return err
	}
	err = w.redis.Eval(ctx, `
redis.call('XADD', KEYS[2], '*', 'source_stream', KEYS[1], 'source_id', ARGV[2], 'entry', ARGV[3], 'error', ARGV[4])
return redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
`, []string{w.stream, w.stream + ":dead-letter"}, w.group, msg.ID, string(raw), cause.Error()).Err()
	if err != nil {
		return fmt.Errorf("dead-letter entry %s: %w", msg.ID, err)
	}
	log.Printf("event dead-lettered stream=%s entry=%s reason=%v", w.stream, msg.ID, cause)
	w.markProgress()
	return nil
}

func (w *Worker) ensureGroup(ctx context.Context) error {
	err := w.redis.XGroupCreateMkStream(
		ctx,
		w.stream,
		w.group,
		"0-0",
	).Err()

	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}

	return nil
}

func (w *Worker) processStreamEntry(
	ctx context.Context,
	entryID string,
	values map[string]interface{},
) error {

	rawEvent, _ := values["event"].(string)

	var envelope events.EventEnvelope

	if err := json.Unmarshal(
		[]byte(rawEvent),
		&envelope,
	); err != nil {
		return invalidEvent{fmt.Errorf("decode event envelope: %w", err)}
	}
	if err := validateEnvelope(envelope); err != nil {
		return invalidEvent{err}
	}

	if err := w.handleEnvelope(ctx, envelope); err != nil {
		return err
	}

	// Acknowledge only after the event has been durably applied.
	return w.redis.XAck(
		ctx,
		w.stream,
		w.group,
		entryID,
	).Err()
}

func (w *Worker) handleEnvelope(
	ctx context.Context,
	envelope events.EventEnvelope,
) error {

	decision, err := w.fencingDecision(ctx, envelope)
	if err != nil {
		return fmt.Errorf("read fencing token: %w", err)
	}

	if decision == fencingStale {
		// A stale engine (one that lost its lease and failed to notice)
		// must not influence durable state. Drop the event and ack it so the
		// poison record does not block the worker (P2.5).
		log.Printf(
			"stale event dropped event=%s type=%s partition=%d token=%d",
			envelope.ID, envelope.Type, envelope.PartitionID, envelope.FencingToken,
		)
		return nil
	}

	switch envelope.Type {

	case events.EventTradeExecuted:
		return w.handleTradeExecuted(ctx, envelope)

	case events.EventOrderCreated:
		return w.handleOrderCreated(ctx, envelope)

	case events.EventOrderCanceled:
		return w.handleOrderCanceled(ctx, envelope)
	case events.EventOrderStatus, events.EventDepthUpdated:
		return nil

	default:
		return invalidEvent{fmt.Errorf("unknown event type: %s", envelope.Type)}
	}
}

func (w *Worker) handleTradeExecuted(
	ctx context.Context,
	envelope events.EventEnvelope,
) error {

	var trade engine.Trade

	if err := json.Unmarshal(envelope.Data, &trade); err != nil {
		return fmt.Errorf("decode trade: %w", err)
	}

	if err := validateTrade(&trade); err != nil {
		return err
	}

	return w.settleTrade(ctx, &trade)
}

func (w *Worker) handleOrderCreated(
	ctx context.Context,
	envelope events.EventEnvelope,
) error {

	var order engine.Order

	if err := json.Unmarshal(envelope.Data, &order); err != nil {
		return fmt.Errorf("decode order: %w", err)
	}

	oid, err := uuid.Parse(order.ID)
	if err != nil {
		return fmt.Errorf("invalid order id: %w", err)
	}

	eid, err := uuid.Parse(order.EventID)
	if err != nil {
		return fmt.Errorf("invalid event id: %w", err)
	}

	uid, err := uuid.Parse(order.UserID)
	if err != nil {
		return fmt.Errorf("invalid user id: %w", err)
	}

	err = w.queries.InsertOrder(ctx, repository.InsertOrderParams{
		ID:                oid,
		EventID:           eid,
		UserID:            uid,
		OrderType:         order.OrderType,
		Outcome:           order.Outcome,
		Side:              order.Side,
		Quantity:          numericFromInt(order.Quantity),
		Price:             numericFromScaled(order.Price),
		Status:            order.Status,
		FilledQuantity:    numericFromInt(order.FilledQuantity),
		RemainingQuantity: numericFromInt(order.RemainingQuantity),
	})

	if err != nil {
		// Redelivery of the same order is a no-op.
		if isUniqueViolation(err) {
			return nil
		}

		return fmt.Errorf("insert order: %w", err)
	}

	return nil
}

func (w *Worker) handleOrderCanceled(
	ctx context.Context,
	envelope events.EventEnvelope,
) error {

	var payload struct {
		OrderID string `json:"orderId"`
	}

	if err := json.Unmarshal(envelope.Data, &payload); err != nil {
		return fmt.Errorf("decode cancel payload: %w", err)
	}

	oid, err := uuid.Parse(payload.OrderID)
	if err != nil {
		return fmt.Errorf("invalid order id: %w", err)
	}

	if err := w.queries.UpdateOrderStatus(ctx, repository.UpdateOrderStatusParams{
		ID:     oid,
		Status: engine.StatusCanceled,
	}); err != nil {
		return fmt.Errorf("update order status: %w", err)
	}

	return nil
}

func validateTrade(trade *engine.Trade) error {
	if trade.ID == "" {
		return errors.New("trade id is required")
	}

	if trade.EventID == "" {
		return errors.New("event id is required")
	}

	if trade.OrderID == "" {
		return errors.New("taker order id is required")
	}

	if trade.MatchOrderID == "" {
		return errors.New("maker order id is required")
	}

	if trade.BuyerID == "" {
		return errors.New("buyer id is required")
	}

	if trade.SellerID == "" {
		return errors.New("seller id is required")
	}

	if trade.Outcome != "YES" &&
		trade.Outcome != "NO" {
		return errors.New("invalid outcome")
	}

	if trade.Quantity <= 0 {
		return errors.New("quantity must be greater than zero")
	}

	if trade.Price < 0 {
		return errors.New("price cannot be negative")
	}
	if trade.Price > engine.PriceScale {
		return errors.New("price exceeds prediction market range")
	}
	if trade.TakerSide != engine.SideBuy && trade.TakerSide != engine.SideSell {
		return errors.New("invalid taker side")
	}
	if trade.Price > 0 && trade.Quantity > int64(^uint64(0)>>1)/trade.Price {
		return errors.New("trade amount overflows int64")
	}

	return nil
}

func (w *Worker) settleTrade(
	ctx context.Context,
	trade *engine.Trade,
) error {

	// The engine mutates its own state when a match happens; this worker is
	// the single writer that reflects it in Postgres. All or nothing.
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	defer func() {
		_ = tx.Rollback(ctx)
	}()

	qtx := w.queries.WithTx(tx)

	tradeID, err := uuid.Parse(trade.ID)
	if err != nil {
		return fmt.Errorf("invalid trade id: %w", err)
	}

	eventID, err := uuid.Parse(trade.EventID)
	if err != nil {
		return fmt.Errorf("invalid event id: %w", err)
	}

	buyerID, err := uuid.Parse(trade.BuyerID)
	if err != nil {
		return fmt.Errorf("invalid buyer id: %w", err)
	}

	sellerID, err := uuid.Parse(trade.SellerID)
	if err != nil {
		return fmt.Errorf("invalid seller id: %w", err)
	}

	takerOrderID, err := uuid.Parse(trade.OrderID)
	if err != nil {
		return fmt.Errorf("invalid taker order id: %w", err)
	}

	makerOrderID, err := uuid.Parse(trade.MatchOrderID)
	if err != nil {
		return fmt.Errorf("invalid maker order id: %w", err)
	}

	/*
		1. Insert trade.

		The engine supplies the trade ID. If the same trade is delivered
		again (crash between commit and XACK), the primary key rejects it
		and we treat it as already settled.
	*/
	if _, err := qtx.InsertTrade(ctx, repository.InsertTradeParams{
		ID:           tradeID,
		EventID:      eventID,
		TakerOrderID: takerOrderID,
		MakerOrderID: makerOrderID,
		BuyerID:      buyerID,
		SellerID:     sellerID,
		Outcome:      trade.Outcome,
		TakerSide:    trade.TakerSide,
		Quantity:     numericFromInt(trade.Quantity),
		Price:        numericFromScaled(trade.Price),
	}); err != nil {
		if isUniqueViolation(err) {
			return nil
		}

		return fmt.Errorf("insert trade: %w", err)
	}

	/*
		2. Reflect the fill on both orders.
	*/
	if err := w.applyOrderFill(ctx, qtx, takerOrderID, trade.Quantity); err != nil {
		return fmt.Errorf("update taker order: %w", err)
	}

	if err := w.applyOrderFill(ctx, qtx, makerOrderID, trade.Quantity); err != nil {
		return fmt.Errorf("update maker order: %w", err)
	}

	/*
		3. Buyer pays, seller receives.

		Cash is kept in 1/10000 units; qty * price is exact integer math.
	*/
	total := trade.Quantity * trade.Price

	tag, err := qtx.DeductBalance(ctx, repository.DeductBalanceParams{
		ID:      buyerID,
		Balance: numericFromScaled(total),
	})
	if err != nil {
		return fmt.Errorf("deduct buyer balance: %w", err)
	}

	if tag == 0 {
		return errors.New("insufficient buyer balance")
	}

	if err := qtx.AddBalance(ctx, repository.AddBalanceParams{
		ID:      sellerID,
		Balance: numericFromScaled(total),
	}); err != nil {
		return fmt.Errorf("credit seller balance: %w", err)
	}

	/*
		4. Positions: the buyer accumulates, the seller shorts.
	*/
	if err := qtx.UpsertPosition(ctx, repository.UpsertPositionParams{
		UserID:   buyerID,
		EventID:  eventID,
		Outcome:  trade.Outcome,
		Shares:   numericFromInt(trade.Quantity),
		AvgPrice: numericFromScaled(trade.Price),
	}); err != nil {
		return fmt.Errorf("update buyer position: %w", err)
	}

	if err := qtx.UpsertPosition(ctx, repository.UpsertPositionParams{
		UserID:   sellerID,
		EventID:  eventID,
		Outcome:  trade.Outcome,
		Shares:   numericFromInt(-trade.Quantity),
		AvgPrice: numericFromScaled(trade.Price),
	}); err != nil {
		return fmt.Errorf("update seller position: %w", err)
	}

	/*
		5. Ledger entries.
	*/
	if err := w.insertTransaction(ctx, qtx, repository.InsertTransactionParams{
		UserID:   buyerID,
		OrderID:  pgtype.UUID{Bytes: takerOrderID, Valid: true},
		TradeID:  pgtype.UUID{Bytes: tradeID, Valid: true},
		Type:     "BUY",
		Outcome:  pgtype.Text{String: trade.Outcome, Valid: true},
		Quantity: numericFromInt(trade.Quantity),
		Price:    numericFromScaled(trade.Price),
		Amount:   numericFromScaled(total),
	}); err != nil {
		return fmt.Errorf("insert buyer transaction: %w", err)
	}

	if err := w.insertTransaction(ctx, qtx, repository.InsertTransactionParams{
		UserID:   sellerID,
		OrderID:  pgtype.UUID{Bytes: makerOrderID, Valid: true},
		TradeID:  pgtype.UUID{Bytes: tradeID, Valid: true},
		Type:     "SELL",
		Outcome:  pgtype.Text{String: trade.Outcome, Valid: true},
		Quantity: numericFromInt(trade.Quantity),
		Price:    numericFromScaled(trade.Price),
		Amount:   numericFromScaled(total),
	}); err != nil {
		return fmt.Errorf("insert seller transaction: %w", err)
	}

	/*
		6. Aggregate event volume.
	*/
	if err := qtx.IncrementEventVolume(ctx, repository.IncrementEventVolumeParams{
		ID:     eventID,
		Volume: numericFromScaled(total),
	}); err != nil {
		return fmt.Errorf("update event volume: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit settlement: %w", err)
	}

	log.Printf(
		"trade settled trade=%s quantity=%d price=%d",
		trade.ID,
		trade.Quantity,
		trade.Price,
	)

	return nil
}

func (w *Worker) applyOrderFill(
	ctx context.Context,
	qtx *repository.Queries,
	orderID uuid.UUID,
	fillQuantity int64,
) error {

	order, err := qtx.GetOrderByID(ctx, orderID)
	if err != nil {
		return fmt.Errorf("load order %s: %w", orderID, err)
	}

	current, err := numericToInt64(order.FilledQuantity)
	if err != nil {
		return fmt.Errorf("read order fill %s: %w", orderID, err)
	}

	quantity, err := numericToInt64(order.Quantity)
	if err != nil {
		return fmt.Errorf("read order quantity %s: %w", orderID, err)
	}

	newFilled := current + fillQuantity

	status := engine.StatusPartial

	if newFilled >= quantity {
		status = engine.StatusFilled
	}

	return qtx.UpdateOrderFill(ctx, repository.UpdateOrderFillParams{
		ID:             orderID,
		FilledQuantity: numericFromInt(fillQuantity),
		Status:         status,
	})
}

func (w *Worker) insertTransaction(
	ctx context.Context,
	qtx *repository.Queries,
	params repository.InsertTransactionParams,
) error {
	_, err := qtx.InsertTransaction(ctx, params)
	return err
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError

	return errors.As(err, &pgErr) &&
		pgErr.Code == "23505"
}

type fencingDecision int

const (
	fencingAccept fencingDecision = iota
	fencingStale
)

// fencingDecision classifies an envelope against the engine's fencing token
// (P2.5). An envelope is stale when its token is older than the one the
// current partition holder acquired; such writes must not reach Postgres.
func (w *Worker) fencingDecision(
	ctx context.Context,
	envelope events.EventEnvelope,
) (fencingDecision, error) {

	current, err := w.redis.Get(ctx, partition.FencingKey(envelope.PartitionID)).Uint64()

	if errors.Is(err, redis.Nil) {
		// No engine has claimed the partition yet; nothing to compare against.
		return fencingAccept, nil
	}

	if err != nil {
		return fencingAccept, err
	}

	return fencingDecisionForToken(envelope.FencingToken, current), nil
}

// fencingDecisionForToken is the pure decision rule; kept separate so the
// fencing behavior is unit-testable without Redis.
func fencingDecisionForToken(token, current uint64) fencingDecision {
	if token == 0 {
		// Legacy envelope produced before fencing tokens existed; no fence
		// to cross, so accept it for backward-compatible replay.
		return fencingAccept
	}

	if token < current {
		return fencingStale
	}

	return fencingAccept
}

// numericFromInt builds an exact DECIMAL from a whole-share integer count.
func numericFromInt(v int64) pgtype.Numeric {
	return pgtype.Numeric{
		Int:   big.NewInt(v),
		Exp:   0,
		Valid: true,
	}
}

// numericFromScaled builds an exact DECIMAL from a fixed-point value in
// 1/10000 units, e.g. 4370 -> 0.4370.
func numericFromScaled(v int64) pgtype.Numeric {
	return pgtype.Numeric{
		Int:   big.NewInt(v),
		Exp:   -4,
		Valid: true,
	}
}

// numericToInt64 reads a whole-share integer back out of a Numeric column.
func numericToInt64(n pgtype.Numeric) (int64, error) {
	if !n.Valid {
		return 0, nil
	}

	// Our columns only ever hold whole-share counts at read time, so a
	// decimal conversion is exact.
	f, err := n.Float64Value()
	if err != nil {
		return 0, err
	}

	if !f.Valid {
		return 0, nil
	}

	return int64(f.Float64), nil
}
