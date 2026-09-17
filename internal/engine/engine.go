package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"predix/internal/events"
	"predix/internal/partition"

	"predix/pkg/redis"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	rd "github.com/redis/go-redis/v9"
)

var (
	ErrEventNotFound      = errors.New("event not found")
	ErrOrderNotFound      = errors.New("order not found")
	ErrOrderAlreadyExists = errors.New("order already exists")
	ErrOrderNotCancelable = errors.New("order cannot be canceled")
	ErrInvalidOrder       = errors.New("invalid order")

	ErrInsufficientFunds  = errors.New("insufficient balance for order")
	ErrInsufficientShares = errors.New("insufficient shares for order")
)

type Engine struct {
	// Single synchronization boundary.
	//
	// Every access to:
	// - markets
	// - orders
	// - orderbooks
	// - ledger (balances, positions)
	// happens through this mutex.
	mu sync.RWMutex

	markets map[string]*Market
	orders  map[string]*Order

	// Ledger: the engine is the authoritative owner of user funds and shares.
	balances  map[string]*Balance
	positions map[string]*Position

	redisManager *redis.RedisManager
	metrics      *Metrics

	// consumerName identifies this engine instance to the stream consumer
	// group. It is used for XREADGROUP and for reclaiming pending entries
	// after a crash.
	consumerName string

	// engineID is the stable instance identifier stamped on partition
	// leases. A unique value per instance ensures a lease can never be
	// renewed by a different process.
	engineID string

	leaseTTL           time.Duration
	leaseRenewInterval time.Duration

	// router owns partition routing and stream/group/sequence naming.
	router *partition.Router

	// partitionSeq holds the per-partition event sequence counters (P2.8).
	// Indexed by partitionID, sized to router.Partitions.
	partitionSeq []atomic.Uint64

	// partitionToken holds the active fencing token per partition (P2.5).
	// Read/Write under mu.
	partitionToken map[int]uint64

	// partitions tracks the configured partition workers (P2.3).
	partitions map[int]*partitionWorker

	// replaying marks the partitions currently re-executing their command
	// log at startup. Emitted events are suppressed so the DB projection is
	// not double-written.
	replaying map[int]bool

	ctx    context.Context
	cancel context.CancelFunc

	// emitMu serializes the full emit cycle (outbox append → XAdd →
	// publish → outbox reset) so concurrent partition workers cannot
	// truncate an envelope another worker has not yet published.
	emitMu sync.Mutex

	// eventStream is the durable event stream the DB worker consumes;
	// wsChannel is the fan-out channel for real-time listening. Both are
	// configurable through SetStreamNames (P3.7).
	eventStream string
	wsChannel   string

	// workersWG tracks the per-partition consumer goroutines. Matching is
	// synchronous inside each consumer, so no separate processor is needed.
	workersWG sync.WaitGroup

	outbox *Outbox

	startOnce sync.Once
	stopOnce  sync.Once
}

// defaultLease values are conservative for a single-engine deployment; they
// are tuned through SetLeaseSettings / config in a distributed deployment.
const (
	defaultLeaseTTL           = 15 * time.Second
	defaultLeaseRenewInterval = 5 * time.Second
)

func NewEngine(rm *redis.RedisManager) (*Engine, error) {
	return newEngine(rm, "data/outbox.log")
}

// newEngine is NewEngine with an explicit outbox path (tests use a temp dir).
func newEngine(rm *redis.RedisManager, outboxPath string) (*Engine, error) {
	if rm == nil {
		return nil, errors.New("redis manager is required")
	}

	if err := os.MkdirAll(filepath.Dir(outboxPath), 0o755); err != nil {
		return nil, fmt.Errorf("create outbox dir: %w", err)
	}

	outbox, err := NewOutbox(outboxPath)
	if err != nil {
		return nil, fmt.Errorf("create outbox: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Engine{
		markets:   make(map[string]*Market),
		orders:    make(map[string]*Order),
		balances:  make(map[string]*Balance),
		positions: make(map[string]*Position),

		redisManager: rm,
		outbox:       outbox,
		metrics:      NewMetrics(),

		consumerName: uuid.NewString(),
		engineID:     "engine-0",

		leaseTTL:           defaultLeaseTTL,
		leaseRenewInterval: defaultLeaseRenewInterval,

		router:         partition.NewRouter(1, "commands"),
		partitionSeq:   []atomic.Uint64{{}},
		partitionToken: make(map[int]uint64),
		partitions:     make(map[int]*partitionWorker),
		replaying:      make(map[int]bool),

		eventStream: events.EventStream,
		wsChannel:   events.WSChannel,

		ctx:    ctx,
		cancel: cancel,
	}, nil
}

// SetStreamNames overrides the durable event stream and the WebSocket fan-out
// channel this engine publishes to (P3.7). Empty values keep the defaults.
func (e *Engine) SetStreamNames(eventStream, wsChannel string) {
	e.emitMu.Lock()
	defer e.emitMu.Unlock()

	if eventStream != "" {
		e.eventStream = eventStream
	}
	if wsChannel != "" {
		e.wsChannel = wsChannel
	}
}

// SetRouter installs the partition router this engine uses for stream, group
// and sequence-key naming. It must be called before any partition is added.
func (e *Engine) SetRouter(r *partition.Router) {
	if r == nil {
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	seq := make([]atomic.Uint64, r.Partitions)
	copy(seq, e.partitionSeq)
	e.partitionSeq = seq
	e.router = r
}

// SetLeaseSettings configures lease ownership parameters. ENGINE_ID must be
// unique per process.
func (e *Engine) SetLeaseSettings(engineID string, ttl, renew time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if engineID != "" {
		e.engineID = engineID
	}

	if ttl > 0 {
		e.leaseTTL = ttl
	}

	if renew > 0 {
		e.leaseRenewInterval = renew
	}
}

func (e *Engine) Start() {
	e.startOnce.Do(func() {
		if e.redisManager != nil {
			if err := e.outbox.Recover(
				e.ctx,
				e.redisManager.GetClient(),
				e.eventStream,
				e.wsChannel,
			); err != nil {
				log.Println("outbox recovery error:", err)
			}
		}

		for pid, worker := range e.partitions {
			e.workersWG.Add(1)

			go func(pid int, w *partitionWorker) {
				defer e.workersWG.Done()
				e.runPartition(pid, w)
			}(pid, worker)
		}

		log.Printf(
			"Engine started: %d partition worker(s) running",
			len(e.partitions),
		)
	})
}

func (e *Engine) Shutdown(ctx context.Context) {
	e.stopOnce.Do(func() {
		log.Println("Shutting down engine...")

		e.cancel()
		e.workersWG.Wait()

		if e.outbox != nil {
			if err := e.outbox.Close(); err != nil {
				log.Println("outbox close error:", err)
			}
		}

		log.Println("Engine stopped.")
	})

	_ = ctx
}

// isReplaying reports whether partitionID is currently re-executing its
// command stream. Live-only observations (metrics, event emission) must gate
// on it so a restart does not double-count.
func (e *Engine) isReplaying(partitionID int) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.replaying[partitionID]
}

// OwnedPartitions returns how many partition leases this engine currently
// holds. Used by the readiness probe (P3.5).
func (e *Engine) OwnedPartitions() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.partitionToken)
}

// MetricsRegistry exposes the engine's Prometheus registry (P3.4).
func (e *Engine) MetricsRegistry() *prometheus.Registry {
	return e.metrics.Registry
}

func (e *Engine) consumeMessages(ctx context.Context, partitionID int) {
	client := e.redisManager.GetClient()

	if err := e.ensureCommandGroup(ctx, partitionID); err != nil {
		log.Println("stream group setup error:", err)
		return
	}

	stream := e.router.Stream(partitionID)
	group := e.router.Group(partitionID)

	// Reclaim pending entries left by a crashed instance within our group
	// (MinIdle ensures we never steal work a live engine is still processing).
	claimed, _, err := client.XAutoClaim(
		ctx,
		&rd.XAutoClaimArgs{
			Stream:   stream,
			Group:    group,
			Consumer: e.consumerName,
			MinIdle:  10 * time.Second,
			Start:    "0-0",
		},
	).Result()

	if err != nil && !errors.Is(err, context.Canceled) {
		log.Println("XAutoClaim error:", err)
	}

	for _, msg := range claimed {
		e.processStreamEntry(ctx, partitionID, client, msg.ID, msg.Values)
	}

	for {
		// Block for the next command in the group. The block time is
		// bounded so the loop observes context cancellation and lease
		// loss promptly; shutdown does not wait for an outstanding
		// blocking read on the server.
		streams, err := client.XReadGroup(
			ctx,
			&rd.XReadGroupArgs{
				Group:    group,
				Consumer: e.consumerName,
				Streams:  []string{stream, ">"},
				Count:    10,
				Block:    500 * time.Millisecond,
			},
		).Result()

		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}

			log.Println("XReadGroup error:", err)

			select {
			case <-time.After(time.Second):
			case <-ctx.Done():
				return
			}

			continue
		}

		for _, s := range streams {
			for _, msg := range s.Messages {
				e.processStreamEntry(ctx, partitionID, client, msg.ID, msg.Values)
			}
		}
	}
}

func (e *Engine) ensureCommandGroup(ctx context.Context, partitionID int) error {
	err := e.redisManager.GetClient().XGroupCreateMkStream(
		ctx,
		e.router.Stream(partitionID),
		e.router.Group(partitionID),
		"0-0",
	).Err()

	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}

	return nil
}

func (e *Engine) processStreamEntry(
	ctx context.Context,
	partitionID int,
	client *rd.Client,
	entryID string,
	values map[string]interface{},
) {
	defer func() {
		// Acknowledge once the command has been handled and the reply sent.
		// If we crash in between, the entry stays pending and is redelivered
		// on restart (XAUTOCLAIM), making the command stream at-least-once.
		if ackErr := client.XAck(
			ctx,
			e.router.Stream(partitionID),
			e.router.Group(partitionID),
			entryID,
		).Err(); ackErr != nil && !errors.Is(ackErr, context.Canceled) {
			log.Println("XAck error:", ackErr)
		}
	}()

	clientID, _ := values["clientId"].(string)

	rawCommand, _ := values["command"].(string)

	if rawCommand == "" {
		log.Println("message missing command envelope")
		return
	}

	var env redis.CommandEnvelope

	if err := json.Unmarshal(
		[]byte(rawCommand),
		&env,
	); err != nil {
		log.Println("command envelope unmarshal error:", err)
		return
	}

	if env.Type == "" {
		log.Println("command envelope missing type")
		return
	}

	log.Printf(
		"Received command type=%s clientId=%s sequence=%d partition=%d",
		env.Type,
		clientID,
		env.Sequence,
		partitionID,
	)

	response := e.handleMessage(partitionID, redis.MessageToEngine{
		Type:    env.Type,
		Payload: env.Payload,
	})

	if clientID == "" {
		// Fire-and-forget command; no reply expected.
		return
	}

	respBytes, err := json.Marshal(response)
	if err != nil {
		log.Println("response marshal error:", err)
		return
	}

	if err := client.Publish(
		ctx,
		clientID,
		respBytes,
	).Err(); err != nil {
		// During shutdown this is expected.
		if !errors.Is(err, context.Canceled) {
			log.Println("response publish error:", err)
		}
	}
}

func (e *Engine) handleMessage(
	partitionID int,
	msg redis.MessageToEngine,
) *redis.EngineResponse {

	switch msg.Type {

	case string(redis.CreateOrderCommand):
		return e.handleCreateOrderP(partitionID, msg.Payload)

	case string(redis.CancelOrderCommand):
		return e.handleCancelOrderP(partitionID, msg.Payload)

	case string(redis.GetDepthCommand):
		return e.handleGetDepth(msg.Payload)

	case string(redis.GetOpenOrdersCommand):
		return e.handleGetOpenOrders(msg.Payload)

	case string(redis.CreateEventCommand):
		return e.handleCreateEvent(msg.Payload)

	case string(redis.UserCreatedCommand):
		return e.handleUserCreated(msg.Payload)

	default:
		return &redis.EngineResponse{
			Success: false,
			Error:   "unknown message type",
		}
	}
}

func (e *Engine) handleCreateEvent(
	payload json.RawMessage,
) *redis.EngineResponse {

	var req struct {
		EventID string `json:"eventId"`
	}

	if err := json.Unmarshal(payload, &req); err != nil {
		return failure("invalid payload")
	}

	if req.EventID == "" {
		return failure("eventId is required")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.markets[req.EventID]; exists {
		// Idempotent behavior.
		return successJSON(map[string]any{
			"status": "already_exists",
		})
	}

	e.markets[req.EventID] = NewMarket()

	return successJSON(map[string]any{
		"status": "created",
	})
}

// handleCreateOrder is the partition-agnostic entry point used by tests. The
// partition-aware path (handleCreateOrderP) is used by live consumers.
func (e *Engine) handleCreateOrder(
	payload json.RawMessage,
) *redis.EngineResponse {
	return e.handleCreateOrderP(0, payload)
}

func (e *Engine) handleCreateOrderP(
	partitionID int,
	payload json.RawMessage,
) *redis.EngineResponse {

	start := time.Now()

	resp := e.handleCreateOrderCore(partitionID, payload)

	// Order lifecycle counts and latencies must not be inflated by startup
	// replay.
	if !e.isReplaying(partitionID) {
		e.metrics.OrderLatency.Observe(time.Since(start).Seconds())

		if resp.Success {
			e.metrics.OrdersAccepted.Inc()
		} else {
			e.metrics.OrdersRejected.Inc()
		}
	}

	return resp
}

func (e *Engine) handleCreateOrderCore(
	partitionID int,
	payload json.RawMessage,
) *redis.EngineResponse {

	var req struct {
		OrderID   string `json:"orderId"`
		EventID   string `json:"eventId"`
		UserID    string `json:"userId"`
		OrderType string `json:"orderType"`
		Outcome   string `json:"outcome"`
		Side      string `json:"side"`
		Quantity  int64  `json:"quantity"`
		Price     int64  `json:"price"`
	}

	if err := json.Unmarshal(payload, &req); err != nil {
		return failure("invalid payload")
	}

	orderID := req.OrderID

	if orderID == "" {
		orderID = uuid.NewString()
	}

	quantity := req.Quantity

	if quantity <= 0 {
		return failure("quantity must be greater than zero")
	}

	price := req.Price

	order := &Order{
		ID:        orderID,
		EventID:   req.EventID,
		UserID:    req.UserID,
		OrderType: req.OrderType,
		Outcome:   req.Outcome,
		Side:      req.Side,

		Quantity:          quantity,
		FilledQuantity:    0,
		RemainingQuantity: quantity,

		Price:     price,
		Status:    StatusPending,
		CreatedAt: time.Now().UTC(),
	}

	if err := validateOrder(order); err != nil {
		return failure(err.Error())
	}

	e.mu.Lock()

	if existing, exists := e.orders[order.ID]; exists {
		e.mu.Unlock()

		// A client resubmitting the same order confirms instead of failing.
		return successJSON(map[string]any{
			"orderId": existing.ID,
			"status":  existing.Status,
		})
	}

	if _, exists := e.markets[order.EventID]; !exists {
		e.mu.Unlock()

		order.Status = StatusRejected

		return failure(ErrEventNotFound.Error())
	}

	if err := e.reserveOrderLocked(order); err != nil {
		e.mu.Unlock()

		order.Status = StatusRejected

		return failure(err.Error())
	}

	e.orders[order.ID] = order

	e.mu.Unlock()

	// The command stream (partition commands:{partition}) is the durable
	// log. Replaying that stream on startup reconstructs the ledger, so
	// no separate WAL is needed.

	// Notify the DB worker BEFORE the order can be matched, so the events
	// stream always sees ORDER_CREATED ahead of any TRADE_EXECUTED for it.
	orderData, err := json.Marshal(order)
	if err != nil {
		e.mu.Lock()
		delete(e.orders, order.ID)
		order.Status = StatusRejected
		e.mu.Unlock()

		return failure("failed to serialize order")
	}

	if err := e.emitEventP(
		partitionID,
		events.EventOrderCreated,
		orderData,
		false,
	); err != nil {
		e.mu.Lock()
		delete(e.orders, order.ID)
		order.Status = StatusRejected
		e.mu.Unlock()

		return failure("failed to stream order creation")
	}

	// Match synchronously so the response reflects the resulting state
	// instead of a stale PENDING.
	matchStart := time.Now()
	e.matchOrder(order, partitionID)
	e.metrics.MatchingLatency.Observe(time.Since(matchStart).Seconds())

	e.metrics.OrdersProcessed.Inc()

	return successJSON(map[string]any{
		"orderId":           order.ID,
		"status":            order.Status,
		"filledQuantity":    order.FilledQuantity,
		"remainingQuantity": order.RemainingQuantity,
	})
}

func validateOrder(order *Order) error {
	if order == nil {
		return ErrInvalidOrder
	}

	if order.EventID == "" {
		return errors.New("eventId is required")
	}

	if order.UserID == "" {
		return errors.New("userId is required")
	}

	if order.Quantity <= 0 {
		return errors.New("quantity must be greater than zero")
	}

	switch order.Side {
	case SideBuy, SideSell:
	default:
		return errors.New("invalid side")
	}

	switch order.Outcome {
	case OutcomeYes, OutcomeNo:
	default:
		return errors.New("invalid outcome")
	}

	switch order.OrderType {
	case OrderTypeLimit:
		if order.Price <= 0 {
			return errors.New("limit order price must be greater than zero")
		}

	case OrderTypeMarket:
		// Price is ignored for market orders.

	default:
		return errors.New("invalid order type")
	}

	return nil
}

func (e *Engine) matchOrder(order *Order, partitionID int) []*Trade {
	e.mu.Lock()

	// Cancellation can happen while the order is waiting
	// before it processes. Since cancellation uses the same mutex,
	// this check makes ordering deterministic.
	if order.Status == StatusCanceled {
		e.mu.Unlock()
		return nil
	}

	market, exists := e.markets[order.EventID]

	if !exists {
		order.Status = StatusRejected
		e.mu.Unlock()
		return nil
	}

	book := market.Book(order.Outcome)

	if book == nil {
		order.Status = StatusRejected
		e.mu.Unlock()
		return nil
	}

	trades := e.matchAgainstBook(book, order)

	if order.RemainingQuantity == 0 {
		order.Status = StatusFilled
	} else if order.FilledQuantity > 0 {
		// Partially filled. The unfilled remainder rests on the book only
		// for limit orders; a market order walks the book once and the
		// unfilled portion is canceled.
		if order.OrderType == OrderTypeLimit {
			order.Status = StatusPartial
			e.addToBook(book, order)
		} else {
			order.Status = StatusCanceled
		}
	} else {
		// Nothing matched.
		if order.OrderType == OrderTypeMarket {
			// Market order disappears if no liquidity exists.
			order.Status = StatusCanceled
		} else {
			order.Status = StatusPending
			e.addToBook(book, order)
		}
	}

	// Apply fills to the ledger. This runs both live and during replay so
	// the reconstructed state is identical.
	for _, trade := range trades {
		e.settleTradeLocked(trade)
	}

	// Orders that are no longer resting (filled or canceled) have nothing
	// left to protect; release their over-lock.
	if order.Status == StatusFilled || order.Status == StatusCanceled {
		e.releaseOrderLocked(order)
	}

	e.mu.Unlock()

	// Never perform Redis I/O while holding the engine lock. During replay
	// event emission is suppressed: the DB projection already contains the
	// effects.
	e.mu.RLock()
	replaying := e.replaying[partitionID]
	e.mu.RUnlock()

	if replaying {
		return trades
	}

	if order.Status == StatusFilled {
		e.metrics.OrdersFilled.Inc()
	}

	for _, trade := range trades {
		e.metrics.TradesExecuted.Inc()

		if err := e.publishTrade(partitionID, trade); err != nil {
			log.Printf(
				"failed to publish trade %s: %v",
				trade.ID,
				err,
			)
		}
	}

	return trades
}

func (e *Engine) matchAgainstBook(
	book *OrderBook,
	incoming *Order,
) []*Trade {

	var trades []*Trade

	for incoming.RemainingQuantity > 0 {

		if incoming.Side == SideBuy {

			// Remove invalid/filled orders sitting at front.
			e.removeInvalidAsks(book)

			if len(book.Asks) == 0 {
				break
			}

			idx := e.findCounterparty(book.Asks, incoming)
			if idx < 0 {
				break
			}

			e.executePair(incoming, book.Asks[idx], &book.Asks, idx, &trades)

		} else {

			e.removeInvalidBids(book)

			if len(book.Bids) == 0 {
				break
			}

			idx := e.findCounterparty(book.Bids, incoming)
			if idx < 0 {
				break
			}

			e.executePair(incoming, book.Bids[idx], &book.Bids, idx, &trades)
		}
	}

	return trades
}

// findCounterparty returns the index of the first resting order that can
// trade against incoming. Same-user orders are skipped (self-trade
// prevention policy: skip resting order) and dead orders are ignored.
func (e *Engine) findCounterparty(
	orders []*Order,
	incoming *Order,
) int {

	for i, resting := range orders {

		if resting.Status == StatusFilled ||
			resting.Status == StatusCanceled ||
			resting.Status == StatusRejected ||
			resting.RemainingQuantity <= 0 {
			continue
		}

		if resting.UserID == incoming.UserID {
			continue
		}

		if canMatch(incoming, resting) {
			return i
		}
	}

	return -1
}

func (e *Engine) executePair(
	incoming *Order,
	resting *Order,
	orders *[]*Order,
	idx int,
	trades *[]*Trade,
) {

	matchQuantity := min(
		incoming.RemainingQuantity,
		resting.RemainingQuantity,
	)

	if matchQuantity <= 0 {
		return
	}

	trade := buildTrade(
		incoming,
		resting,
		matchQuantity,
	)

	*trades = append(*trades, trade)

	// Update incoming.
	incoming.RemainingQuantity -= matchQuantity
	incoming.FilledQuantity += matchQuantity

	// Update resting.
	resting.RemainingQuantity -= matchQuantity
	resting.FilledQuantity += matchQuantity

	if resting.RemainingQuantity <= 0 {
		resting.RemainingQuantity = 0
		resting.Status = StatusFilled
		removeOrderIndex(orders, idx)
	} else {
		resting.Status = StatusPartial
	}
}

// removeOrderIndex removes the element at idx from a price-time sorted slice,
// preserving the relative order of the remaining orders.
func removeOrderIndex(orders *[]*Order, idx int) {
	items := *orders

	copy(items[idx:], items[idx+1:])
	items[len(items)-1] = nil
	*orders = items[:len(items)-1]
}

func canMatch(
	incoming *Order,
	resting *Order,
) bool {

	// Outcome must always match.
	if incoming.Outcome != resting.Outcome {
		return false
	}

	// Market order matches any available price.
	if incoming.OrderType == OrderTypeMarket {
		return true
	}

	if incoming.Side == SideBuy {
		return resting.Price <= incoming.Price
	}

	if incoming.Side == SideSell {
		return resting.Price >= incoming.Price
	}

	return false
}

func (e *Engine) addToBook(
	book *OrderBook,
	order *Order,
) {
	if order.Side == SideBuy {

		book.Bids = append(book.Bids, order)

		sort.SliceStable(
			book.Bids,
			func(i, j int) bool {
				return book.Bids[i].Price >
					book.Bids[j].Price
			},
		)

		return
	}

	book.Asks = append(book.Asks, order)

	sort.SliceStable(
		book.Asks,
		func(i, j int) bool {
			return book.Asks[i].Price <
				book.Asks[j].Price
		},
	)
}

func (e *Engine) removeInvalidBids(book *OrderBook) {
	for len(book.Bids) > 0 {

		order := book.Bids[0]

		if order.Status == StatusFilled ||
			order.Status == StatusCanceled ||
			order.RemainingQuantity <= 0 {

			book.Bids = book.Bids[1:]
			continue
		}

		break
	}
}

func (e *Engine) removeInvalidAsks(book *OrderBook) {
	for len(book.Asks) > 0 {

		order := book.Asks[0]

		if order.Status == StatusFilled ||
			order.Status == StatusCanceled ||
			order.RemainingQuantity <= 0 {

			book.Asks = book.Asks[1:]
			continue
		}

		break
	}
}

func buildTrade(
	taker *Order,
	maker *Order,
	quantity int64,
) *Trade {

	trade := &Trade{
		ID:           uuid.NewString(),
		OrderID:      taker.ID,
		MatchOrderID: maker.ID,

		EventID: taker.EventID,
		Outcome: taker.Outcome,

		TakerSide: taker.Side,

		Quantity: quantity,

		// Trade executes at maker/resting price.
		Price: maker.Price,

		CreatedAt: time.Now().UTC(),
	}

	if taker.Side == SideBuy {
		trade.BuyerID = taker.UserID
		trade.SellerID = maker.UserID
	} else {
		trade.BuyerID = maker.UserID
		trade.SellerID = taker.UserID
	}

	return trade
}

func (e *Engine) handleCancelOrder(
	payload json.RawMessage,
) *redis.EngineResponse {
	return e.handleCancelOrderP(0, payload)
}

func (e *Engine) handleCancelOrderP(
	partitionID int,
	payload json.RawMessage,
) *redis.EngineResponse {

	var req struct {
		OrderID string `json:"orderId"`
		EventID string `json:"eventId"`
	}

	if err := json.Unmarshal(payload, &req); err != nil {
		return failure("invalid payload")
	}

	if req.OrderID == "" {
		return failure("orderId is required")
	}

	e.mu.Lock()

	order, exists := e.orders[req.OrderID]

	if !exists {
		e.mu.Unlock()

		return failure(ErrOrderNotFound.Error())
	}

	if req.EventID != "" && order.EventID != req.EventID {
		e.mu.Unlock()

		return failure("order does not belong to event")
	}

	if order.Status == StatusFilled ||
		order.Status == StatusCanceled ||
		order.Status == StatusRejected {

		e.mu.Unlock()

		return failure(ErrOrderNotCancelable.Error())
	}

	market, exists := e.markets[order.EventID]

	if !exists {
		e.mu.Unlock()

		return failure(ErrEventNotFound.Error())
	}

	book := market.Book(order.Outcome)

	if order.Side == SideBuy {
		removeOrderFromSlice(
			&book.Bids,
			order.ID,
		)
	} else {
		removeOrderFromSlice(
			&book.Asks,
			order.ID,
		)
	}

	order.Status = StatusCanceled

	// Frozen liquidity for the unfilled remainder is returned to the user.
	e.releaseOrderLocked(order)

	e.mu.Unlock()

	if !e.isReplaying(partitionID) {
		e.metrics.OrdersCanceled.Inc()
	}

	cancelData, _ := json.Marshal(map[string]any{
		"orderId": order.ID,
	})

	if err := e.emitEventP(
		partitionID,
		events.EventOrderCanceled,
		cancelData,
		false,
	); err != nil {
		log.Printf(
			"failed to emit order canceled %s: %v",
			order.ID,
			err,
		)
	}

	return successJSON(map[string]any{
		"orderId": order.ID,
		"status":  StatusCanceled,
	})
}

func removeOrderFromSlice(
	orders *[]*Order,
	orderID string,
) bool {

	items := *orders

	for i, order := range items {

		if order.ID != orderID {
			continue
		}

		copy(
			items[i:],
			items[i+1:],
		)

		items[len(items)-1] = nil

		*orders = items[:len(items)-1]

		return true
	}

	return false
}

func (e *Engine) handleGetDepth(
	payload json.RawMessage,
) *redis.EngineResponse {

	var req struct {
		EventID string `json:"eventId"`
	}

	if err := json.Unmarshal(payload, &req); err != nil {
		return failure("invalid payload")
	}

	e.mu.RLock()

	market, exists := e.markets[req.EventID]

	if !exists {
		e.mu.RUnlock()
		return failure(ErrEventNotFound.Error())
	}

	depth := Depth{
		EventID: req.EventID,
	}

	depth.Yes.Bids =
		aggregateBids(market.YES.Bids)

	depth.Yes.Asks =
		aggregateAsks(market.YES.Asks)

	depth.No.Bids =
		aggregateBids(market.NO.Bids)

	depth.No.Asks =
		aggregateAsks(market.NO.Asks)

	e.mu.RUnlock()

	return successJSON(depth)
}

func aggregateBids(
	orders []*Order,
) []OrderBookEntry {

	type level struct {
		quantity int64
		count    int64
	}

	levels := make(map[int64]*level)

	for _, order := range orders {

		if order.Status != StatusPending &&
			order.Status != StatusPartial {
			continue
		}

		if order.RemainingQuantity <= 0 {
			continue
		}

		l, ok := levels[order.Price]
		if !ok {
			l = &level{}
			levels[order.Price] = l
		}

		l.quantity += order.RemainingQuantity
		l.count++
	}

	result := make(
		[]OrderBookEntry,
		0,
		len(levels),
	)

	for price, l := range levels {

		result = append(
			result,
			OrderBookEntry{
				Price:      price,
				Quantity:   l.quantity,
				Total:      price * l.quantity,
				OrderCount: l.count,
			},
		)
	}

	sort.Slice(
		result,
		func(i, j int) bool {
			return result[i].Price >
				result[j].Price
		},
	)

	return result
}

func aggregateAsks(
	orders []*Order,
) []OrderBookEntry {

	type level struct {
		quantity int64
		count    int64
	}

	levels := make(map[int64]*level)

	for _, order := range orders {

		if order.Status != StatusPending &&
			order.Status != StatusPartial {
			continue
		}

		if order.RemainingQuantity <= 0 {
			continue
		}

		l, ok := levels[order.Price]
		if !ok {
			l = &level{}
			levels[order.Price] = l
		}

		l.quantity += order.RemainingQuantity
		l.count++
	}

	result := make(
		[]OrderBookEntry,
		0,
		len(levels),
	)

	for price, l := range levels {

		result = append(
			result,
			OrderBookEntry{
				Price:      price,
				Quantity:   l.quantity,
				Total:      price * l.quantity,
				OrderCount: l.count,
			},
		)
	}

	sort.Slice(
		result,
		func(i, j int) bool {
			return result[i].Price <
				result[j].Price
		},
	)

	return result
}

func (e *Engine) handleGetOpenOrders(
	payload json.RawMessage,
) *redis.EngineResponse {

	var req struct {
		EventID string `json:"eventId"`
		UserID  string `json:"userId"`
	}

	if err := json.Unmarshal(payload, &req); err != nil {
		return failure("invalid payload")
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	orders := make([]*Order, 0)

	for _, order := range e.orders {

		if order.EventID != req.EventID {
			continue
		}

		if order.UserID != req.UserID {
			continue
		}

		if order.Status != StatusPending &&
			order.Status != StatusPartial {
			continue
		}

		copy := *order
		orders = append(orders, &copy)
	}

	sort.SliceStable(
		orders,
		func(i, j int) bool {
			return orders[i].CreatedAt.Before(
				orders[j].CreatedAt,
			)
		},
	)

	return successJSON(orders)
}

func (e *Engine) publishTrade(
	partitionID int,
	trade *Trade,
) error {

	data, err := json.Marshal(trade)
	if err != nil {
		return err
	}

	return e.emitEventP(
		partitionID,
		events.EventTradeExecuted,
		data,
		true,
	)
}

// emitEvent is the partition-agnostic entry point used by tests. Live
// consumers go through emitEventP.
func (e *Engine) emitEvent(
	envType events.EventType,
	data json.RawMessage,
	broadcast bool,
) error {
	return e.emitEventP(0, envType, data, broadcast)
}

// emitEventP stamps an envelope with the processing partition, its fencing
// token (P2.5) and a per-partition sequence (P2.8), appends it to the
// events:out stream for persistence, and optionally publishes the same
// envelope to the WebSocket channel for fan-out.
//
// It never runs while the engine mutex is held.
func (e *Engine) emitEventP(
	partitionID int,
	envType events.EventType,
	data json.RawMessage,
	broadcast bool,
) error {

	e.mu.RLock()
	replaying := e.replaying[partitionID]
	token := e.partitionToken[partitionID]
	e.mu.RUnlock()

	// During startup replay the DB projection already holds these effects;
	// re-emitting them would double-write. This gate makes replay
	// non-mutating minus the in-memory state.
	if replaying {
		return nil
	}

	if e.redisManager == nil {
		return nil
	}

	envelope := events.NewEnvelope(events.NewEnvelopeParams{
		Type:         envType,
		Data:         data,
		PartitionID:  partitionID,
		FencingToken: token,
		Sequence:     e.partitionSeq[partitionID].Add(1),
	})

	envBytes, err := json.Marshal(envelope)
	if err != nil {
		return err
	}

	client := e.redisManager.GetClient()

	// Serialize the emit cycle (see emitMu). Envelopes are written to the
	// durable outbox first: if Redis is unreachable the envelope survives
	// on disk and is republished after a restart.
	e.emitMu.Lock()
	defer e.emitMu.Unlock()

	if e.outbox != nil {
		if err := e.outbox.Append(envBytes); err != nil {
			return err
		}
	}

	// Persistence consumer: the DB worker consumes e.eventStream via a
	// consumer group, so a crashed worker does not lose events.
	if err := client.XAdd(
		context.Background(),
		&rd.XAddArgs{
			Stream: e.eventStream,
			Values: map[string]interface{}{
				"event": string(envBytes),
			},
		},
	).Err(); err != nil {
		return err
	}

	// WebSocket consumer: real-time fan-out to browsers.
	if broadcast {
		if err := client.Publish(
			context.Background(),
			e.wsChannel,
			envBytes,
		).Err(); err != nil {
			return err
		}
	}

	// Both targets accepted the envelope. Drop it from the journal.
	if e.outbox != nil {
		_ = e.outbox.Reset()
	}

	log.Printf(
		"event emitted type=%s sequence=%d",
		envType,
		envelope.Sequence,
	)

	return nil
}
func min(a, b int64) int64 {
	if a < b {
		return a
	}

	return b
}

func successJSON(value any) *redis.EngineResponse {
	data, err := json.Marshal(value)

	if err != nil {
		return &redis.EngineResponse{
			Success: false,
			Error:   err.Error(),
		}
	}

	return &redis.EngineResponse{
		Success: true,
		Data:    data,
	}
}

func failure(message string) *redis.EngineResponse {
	return &redis.EngineResponse{
		Success: false,
		Error:   message,
	}
}
