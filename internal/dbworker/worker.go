package dbworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/big"
	"strings"
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
}

func New(
	redisClient *redis.Client,
	pool *pgxpool.Pool,
	queries *repository.Queries,
) *Worker {
	return &Worker{
		redis:    redisClient,
		pool:     pool,
		queries:  queries,
		consumer: uuid.NewString(),
	}
}

func (w *Worker) Run(ctx context.Context) error {
	log.Println("DB worker started")

	if err := w.ensureGroup(ctx); err != nil {
		return fmt.Errorf("ensure event group: %w", err)
	}

	// Reclaim events a crashed worker left pending in its consumer group.
	claimed, _, err := w.redis.XAutoClaim(
		ctx,
		&redis.XAutoClaimArgs{
			Stream:   events.EventStream,
			Group:    events.EventGroup,
			Consumer: w.consumer,
			MinIdle:  10 * time.Second,
			Start:    "0-0",
		},
	).Result()

	if err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("XAutoClaim error: %v", err)
	}

	for _, msg := range claimed {
		if err := w.processStreamEntry(ctx, msg.ID, msg.Values); err != nil {
			return err
		}
	}

	for {
		streams, err := w.redis.XReadGroup(
			ctx,
			&redis.XReadGroupArgs{
				Group:    events.EventGroup,
				Consumer: w.consumer,
				Streams:  []string{events.EventStream, ">"},
				Count:    10,
				Block:    0,
			},
		).Result()

		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}

			log.Printf("XReadGroup error: %v", err)

			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Second):
			}

			continue
		}

		for _, s := range streams {
			for _, msg := range s.Messages {
				if err := w.processStreamEntry(ctx, msg.ID, msg.Values); err != nil {
					// Leave the entry pending so it is retried after a redelivery.
					return fmt.Errorf("process stream entry %s: %w", msg.ID, err)
				}
			}
		}
	}
}

func (w *Worker) ensureGroup(ctx context.Context) error {
	err := w.redis.XGroupCreateMkStream(
		ctx,
		events.EventStream,
		events.EventGroup,
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
		return fmt.Errorf("decode event envelope: %w", err)
	}

	if err := w.handleEnvelope(ctx, envelope); err != nil {
		return err
	}

	// Acknowledge only after the event has been durably applied.
	return w.redis.XAck(
		ctx,
		events.EventStream,
		events.EventGroup,
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

	default:
		log.Printf("unknown event type: %s", envelope.Type)
		return nil
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
		UserID:  buyerID,
		OrderID: pgtype.UUID{Bytes: takerOrderID, Valid: true},
		TradeID: pgtype.UUID{Bytes: tradeID, Valid: true},
		Type:    "BUY",
		Outcome: pgtype.Text{String: trade.Outcome, Valid: true},
		Quantity: numericFromInt(trade.Quantity),
		Price:    numericFromScaled(trade.Price),
		Amount:   numericFromScaled(total),
	}); err != nil {
		return fmt.Errorf("insert buyer transaction: %w", err)
	}

	if err := w.insertTransaction(ctx, qtx, repository.InsertTransactionParams{
		UserID:  sellerID,
		OrderID: pgtype.UUID{Bytes: makerOrderID, Valid: true},
		TradeID: pgtype.UUID{Bytes: tradeID, Valid: true},
		Type:    "SELL",
		Outcome: pgtype.Text{String: trade.Outcome, Valid: true},
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