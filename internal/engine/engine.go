package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"predix/internal/events"
	"predix/internal/partition"

	"predix/pkg/redis"

	"github.com/google/uuid"
	rd "github.com/redis/go-redis/v9"
)

var (
	ErrEventNotFound      = errors.New("event not found")
	ErrOrderNotFound      = errors.New("order not found")
	ErrOrderAlreadyExists = errors.New("order already exists")
	ErrOrderNotCancelable = errors.New("order cannot be canceled")
	ErrInvalidOrder       = errors.New("invalid order")

	ErrInsufficientFunds = errors.New("insufficient balance for order")
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
	wal          *WAL
	metrics      *Metrics

	// consumerName identifies this engine instance to the stream consumer
	// group. It is used for XREADGROUP and for reclaiming pending entries
	// after a crash.
	consumerName string

	// partitionID is this engine's partition. Phase 1 uses a single
	// partition (0); the value is stamped on every emitted envelope.
	partitionID int

	// router owns partition routing and stream/group/sequence naming.
	router *partition.Router

	// eventSequence is a monotonically increasing counter per envelope,
	// giving downstream consumers a stable ordering hint.
	eventSequence atomic.Uint64

	// replaying is true while the command log is being re-executed at
	// startup. Emitted events are suppressed so the DB projection is not
	// double-written.
	replaying bool

	ctx    context.Context
	cancel context.CancelFunc

	// consumerWG tracks the Redis stream consumer goroutine. Matching is
	// synchronous inside the consumer, so no separate processor is needed.
	consumerWG sync.WaitGroup

	outbox *Outbox

	startOnce sync.Once
	stopOnce  sync.Once
}

func NewEngine(rm *redis.RedisManager) (*Engine, error) {
	if rm == nil {
		return nil, errors.New("redis manager is required")
	}

	wal, err := NewWAL("data/wal.log")
	if err != nil {
		return nil, fmt.Errorf("create WAL: %w", err)
	}

	outbox, err := NewOutbox("data/outbox.log")
	if err != nil {
		return nil, fmt.Errorf("create outbox: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Engine{
		markets:      make(map[string]*Market),
		orders:       make(map[string]*Order),
		balances:     make(map[string]*Balance),
		positions:    make(map[string]*Position),

		redisManager: rm,
		wal:          wal,
		outbox:       outbox,
		metrics:      NewMetrics(),

		consumerName: uuid.NewString(),
		partitionID:  0,
		router:       partition.NewRouter(1, "commands"),

		ctx:    ctx,
		cancel: cancel,
	}, nil
}

func (e *Engine) Start() {
	e.startOnce.Do(func() {
		log.Println("Engine started. Replaying command log...")

		if err := e.replayCommandLog(); err != nil {
			log.Println("replay error:", err)
		}

		if e.redisManager != nil {
			if err := e.outbox.Recover(e.ctx, e.redisManager.GetClient()); err != nil {
				log.Println("outbox recovery error:", err)
			}
		}

		log.Println("Engine replay complete. Waiting for orders...")

		e.consumerWG.Add(1)

		go func() {
			defer e.consumerWG.Done()
			e.consumeMessages()
		}()
	})
}

func (e *Engine) Shutdown(ctx context.Context) {
	e.stopOnce.Do(func() {
		log.Println("Shutting down engine...")

		e.cancel()
		e.consumerWG.Wait()

		if e.wal != nil {
			if err := e.wal.Close(); err != nil {
				log.Println("WAL close error:", err)
			}
		}

		if e.outbox != nil {
			if err := e.outbox.Close(); err != nil {
				log.Println("outbox close error:", err)
			}
		}

		log.Println("Engine stopped.")
	})

	_ = ctx
}


func (e *Engine) consumeMessages() {
	client := e.redisManager.GetClient()

	if err := e.ensureCommandGroup(client); err != nil {
		log.Println("stream group setup error:", err)
		return
	}

	stream := e.router.Stream(e.partitionID)
	group := e.router.Group(e.partitionID)

	// Reclaim pending entries left by a crashed instance within our group
	// (MinIdle ensures we never steal work a live engine is still processing).
	claimed, _, err := client.XAutoClaim(
		e.ctx,
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
		e.processStreamEntry(client, msg.ID, msg.Values)
	}

	for {
		// Block for the next command in the group.
		streams, err := client.XReadGroup(
			e.ctx,
			&rd.XReadGroupArgs{
				Group:    group,
				Consumer: e.consumerName,
				Streams:  []string{stream, ">"},
				Count:    10,
				Block:    0,
			},
		).Result()

		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}

			log.Println("XReadGroup error:", err)

			select {
			case <-time.After(time.Second):
			case <-e.ctx.Done():
				return
			}

			continue
		}

		for _, s := range streams {
			for _, msg := range s.Messages {
				e.processStreamEntry(client, msg.ID, msg.Values)
			}
		}
	}
}

func (e *Engine) ensureCommandGroup(client *rd.Client) error {
	err := client.XGroupCreateMkStream(
		e.ctx,
		e.router.Stream(e.partitionID),
		e.router.Group(e.partitionID),
		"0-0",
	).Err()

	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}

	return nil
}

func (e *Engine) processStreamEntry(
	client *rd.Client,
	entryID string,
	values map[string]interface{},
) {
	defer func() {
		// Acknowledge once the command has been handled and the reply sent.
		// If we crash in between, the entry stays pending and is redelivered
		// on restart (XAUTOCLAIM), making the command stream at-least-once.
		if ackErr := client.XAck(
			e.ctx,
			e.router.Stream(e.partitionID),
			e.router.Group(e.partitionID),
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
		"Received command type=%s clientId=%s sequence=%d",
		env.Type,
		clientID,
		env.Sequence,
	)

	response := e.handleMessage(redis.MessageToEngine{
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
		e.ctx,
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
	msg redis.MessageToEngine,
) *redis.EngineResponse {

	switch msg.Type {

	case string(redis.CreateOrderCommand):
		return e.handleCreateOrder(msg.Payload)

	case string(redis.CancelOrderCommand):
		return e.handleCancelOrder(msg.Payload)

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

func (e *Engine) handleCreateOrder(
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

	// WAL must succeed before the order enters
	// the matching queue.
	if e.wal != nil {
		// Replay re-applies reservations from the reconstructed ledger,
		// so WAL writes are skipped during replay to avoid re-appending.
		if !e.replaying {
			if err := e.wal.Write(order); err != nil {
				e.mu.Lock()
				delete(e.orders, order.ID)
				order.Status = StatusRejected
				e.mu.Unlock()

				return failure(
					fmt.Sprintf("failed to persist order: %v", err),
				)
			}
		}
	}

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

	if err := e.emitEvent(
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
	e.matchOrder(order)

	return successJSON(map[string]any{
		"orderId":          order.ID,
		"status":           order.Status,
		"filledQuantity":   order.FilledQuantity,
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

func (e *Engine) matchOrder(order *Order) []*Trade {
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
	replaying := e.replaying
	e.mu.RUnlock()

	if replaying {
		return trades
	}

	for _, trade := range trades {
		if err := e.publishTrade(trade); err != nil {
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

	cancelData, _ := json.Marshal(map[string]any{
		"orderId": order.ID,
	})

	if err := e.emitEvent(
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
	trade *Trade,
) error {

	data, err := json.Marshal(trade)
	if err != nil {
		return err
	}

	return e.emitEvent(
		events.EventTradeExecuted,
		data,
		true,
	)
}

// emitEvent stamps an envelope with this engine's partition and sequence,
// appends it to the events:out stream for persistence, and optionally
// publishes the same envelope to the WebSocket channel for fan-out.
//
// It never runs while the engine mutex is held.
func (e *Engine) emitEvent(
	envType events.EventType,
	data json.RawMessage,
	broadcast bool,
) error {

	e.mu.RLock()
	replaying := e.replaying
	e.mu.RUnlock()

	// During startup replay the DB projection already holds these effects;
	// re-emitting them would double-write. This is the single gate that
	// makes replay non-mutating minus the in-memory state.
	if replaying {
		return nil
	}

	if e.redisManager == nil {
		return nil
	}

	envelope := events.NewEnvelope(events.NewEnvelopeParams{
		Type:        envType,
		Data:        data,
		PartitionID: e.partitionID,
		Sequence:    e.eventSequence.Add(1),
	})

	envBytes, err := json.Marshal(envelope)
	if err != nil {
		return err
	}

	client := e.redisManager.GetClient()

	// Append to the durable outbox first: if Redis is unreachable the
	// envelope survives on disk and is republished after a restart.
	if e.outbox != nil {
		if err := e.outbox.Append(envBytes); err != nil {
			return err
		}
	}

	// Persistence consumer: the DB worker consumes events:out via a
	// consumer group, so a crashed worker does not lose events.
	if err := client.XAdd(
		context.Background(),
		&rd.XAddArgs{
			Stream: events.EventStream,
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
			events.WSChannel,
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