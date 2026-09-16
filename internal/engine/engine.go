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
)

type Engine struct {
	// Single synchronization boundary.
	//
	// Every access to:
	// - markets
	// - orders
	// - orderbooks
	// happens through this mutex.
	mu sync.RWMutex

	markets map[string]*Market
	orders  map[string]*Order

	pendingQueue chan *Order

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

	// eventSequence is a monotonically increasing counter per envelope,
	// giving downstream consumers a stable ordering hint.
	eventSequence atomic.Uint64

	ctx    context.Context
	cancel context.CancelFunc

	// consumerWG tracks the Redis stream consumer goroutine.
	// processorWG tracks the order-matching goroutine.
	//
	// They are separate so Shutdown can stop the consumer first, then close
	// the pending queue, and only then wait for the processor to drain it.
	consumerWG  sync.WaitGroup
	processorWG sync.WaitGroup

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

	ctx, cancel := context.WithCancel(context.Background())

	return &Engine{
		markets:      make(map[string]*Market),
		orders:       make(map[string]*Order),
		pendingQueue: make(chan *Order, 10000),

		redisManager: rm,
		wal:          wal,
		metrics:      NewMetrics(),

		consumerName: uuid.NewString(),
		partitionID:  0,

		ctx:    ctx,
		cancel: cancel,
	}, nil
}

func (e *Engine) Start() {
	e.startOnce.Do(func() {
		log.Println("Engine started. Waiting for orders...")

		e.consumerWG.Add(1)

		go func() {
			defer e.consumerWG.Done()
			e.consumeMessages()
		}()

		e.processorWG.Add(1)

		go func() {
			defer e.processorWG.Done()
			e.processOrders()
		}()
	})
}

func (e *Engine) Shutdown(ctx context.Context) {
	e.stopOnce.Do(func() {
		log.Println("Shutting down engine...")

		// Stop the Redis consumer. The processor is still alive at this
		// point and keeps draining the pending queue.
		e.cancel()
		e.consumerWG.Wait()

		// Close only after the consumer is gone, so nothing else can write
		// to the queue. Closing lets the processor's range loop drain the
		// remaining buffered orders and then exit.
		close(e.pendingQueue)

		e.processorWG.Wait()

		if e.wal != nil {
			if err := e.wal.Close(); err != nil {
				log.Println("WAL close error:", err)
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

	// Reclaim pending entries left by a crashed instance within our group
	// (MinIdle ensures we never steal work a live engine is still processing).
	claimed, _, err := client.XAutoClaim(
		e.ctx,
		&rd.XAutoClaimArgs{
			Stream:   redis.CommandStream,
			Group:    redis.CommandGroup,
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
				Group:    redis.CommandGroup,
				Consumer: e.consumerName,
				Streams:  []string{redis.CommandStream, ">"},
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
		redis.CommandStream,
		redis.CommandGroup,
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
			redis.CommandStream,
			redis.CommandGroup,
			entryID,
		).Err(); ackErr != nil && !errors.Is(ackErr, context.Canceled) {
			log.Println("XAck error:", ackErr)
		}
	}()

	clientID, _ := values["clientId"].(string)

	if clientID == "" {
		log.Println("message missing clientId")
		return
	}

	rawMessage, _ := values["message"].(string)

	var msg redis.MessageToEngine

	if err := json.Unmarshal(
		[]byte(rawMessage),
		&msg,
	); err != nil {
		log.Println("message unmarshal error:", err)
		return
	}

	log.Printf(
		"Received message type=%s clientId=%s",
		msg.Type,
		clientID,
	)

	response := e.handleMessage(msg)

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

	case "CREATE_ORDER":
		return e.handleCreateOrder(msg.Payload)

	case "CANCEL_ORDER":
		return e.handleCancelOrder(msg.Payload)

	case "GET_DEPTH":
		return e.handleGetDepth(msg.Payload)

	case "GET_OPEN_ORDERS":
		return e.handleGetOpenOrders(msg.Payload)

	case "CREATE_EVENT":
		return e.handleCreateEvent(msg.Payload)

	default:
		return &redis.EngineResponse{
			Success: false,
			Error:   "unknown message type",
		}
	}
}

func (e *Engine) processOrders() {
	for order := range e.pendingQueue {
		if order == nil {
			continue
		}

		e.matchOrder(order)

		if e.metrics != nil {
			e.metrics.OrdersProcessed.Inc()
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

	e.orders[order.ID] = order

	e.mu.Unlock()

	// WAL must succeed before the order enters
	// the matching queue.
	if e.wal != nil {
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

	// Queue order for matching.
	select {
	case e.pendingQueue <- order:

		return successJSON(map[string]any{
			"orderId": order.ID,
			"status":  StatusPending,
		})

	case <-e.ctx.Done():
		return failure("engine shutting down")
	}
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

func (e *Engine) matchOrder(order *Order) {
	e.mu.Lock()

	// Cancellation can happen while the order is waiting
	// in pendingQueue. Since cancellation uses the same mutex,
	// this check makes ordering deterministic.
	if order.Status == StatusCanceled {
		e.mu.Unlock()
		return
	}

	market, exists := e.markets[order.EventID]

	if !exists {
		order.Status = StatusRejected
		e.mu.Unlock()
		return
	}

	book := market.Book(order.Outcome)

	if book == nil {
		order.Status = StatusRejected
		e.mu.Unlock()
		return
	}

	trades := e.matchAgainstBook(book, order)

	if order.RemainingQuantity == 0 {
		order.Status = StatusFilled
	} else if order.FilledQuantity > 0 {
		order.Status = StatusPartial

		// Remaining quantity rests on the book
		// only for limit orders.
		if order.OrderType == OrderTypeLimit {
			e.addToBook(book, order)
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

	e.mu.Unlock()

	// Never perform Redis I/O while holding the engine lock.
	for _, trade := range trades {
		if err := e.publishTrade(trade); err != nil {
			log.Printf(
				"failed to publish trade %s: %v",
				trade.ID,
				err,
			)
		}
	}
}

func (e *Engine) matchAgainstBook(
	book *OrderBook,
	incoming *Order,
) []*Trade {

	var trades []*Trade

	for incoming.RemainingQuantity > 0 {

		var resting *Order

		if incoming.Side == SideBuy {

			// Remove invalid/filled orders sitting at front.
			e.removeInvalidAsks(book)

			if len(book.Asks) == 0 {
				break
			}

			resting = book.Asks[0]

		} else {

			e.removeInvalidBids(book)

			if len(book.Bids) == 0 {
				break
			}

			resting = book.Bids[0]
		}

		if !canMatch(incoming, resting) {
			break
		}

		matchQuantity := min(
			incoming.RemainingQuantity,
			resting.RemainingQuantity,
		)

		if matchQuantity <= 0 {
			break
		}

		trade := buildTrade(
			incoming,
			resting,
			matchQuantity,
		)

		trades = append(trades, trade)

		// Update incoming.
		incoming.RemainingQuantity -= matchQuantity
		incoming.FilledQuantity += matchQuantity

		// Update resting.
		resting.RemainingQuantity -= matchQuantity
		resting.FilledQuantity += matchQuantity

		if resting.RemainingQuantity <= 0 {

			resting.RemainingQuantity = 0
			resting.Status = StatusFilled

			if incoming.Side == SideBuy {
				book.Asks = book.Asks[1:]
			} else {
				book.Bids = book.Bids[1:]
			}

		} else {
			resting.Status = StatusPartial
		}
	}

	return trades
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

	e.mu.Unlock()

	// Never perform Redis I/O while holding the engine lock.
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