package engine

import "time"

const (
	SideBuy  = "BUY"
	SideSell = "SELL"

	OutcomeYes = "YES"
	OutcomeNo  = "NO"

	OrderTypeLimit  = "LIMIT"
	OrderTypeMarket = "MARKET"

	StatusPending  = "PENDING"
	StatusPartial  = "PARTIAL"
	StatusFilled   = "FILLED"
	StatusCanceled = "CANCELED"
	StatusRejected = "REJECTED"
)

// PriceScale is the fixed-point multiplier for prices.
//
// An int64 price is stored in units of 1/10000, so 0.4370 -> 4370 and 1.00 -> 10000.
// Conversion between float64 and int64 only happens at the API boundary.
const PriceScale int64 = 10000

// ScalePrice converts a float64 price into fixed-point units.
func ScalePrice(f float64) int64 {
	return int64(f*float64(PriceScale) + 0.5)
}

// UnscalePrice converts fixed-point units back to a float64 price.
func UnscalePrice(v int64) float64 {
	return float64(v) / float64(PriceScale)
}

type Order struct {
	ID        string    `json:"id"`
	EventID   string    `json:"eventId"`
	UserID    string    `json:"userId"`
	OrderType string    `json:"orderType"`
	Outcome   string    `json:"outcome"`
	Side      string    `json:"side"`

	// Quantity is the original requested quantity (whole shares).
	Quantity int64 `json:"quantity"`

	// FilledQuantity tracks how much has been executed.
	FilledQuantity int64 `json:"filledQuantity"`

	// RemainingQuantity is what is still available to match.
	RemainingQuantity int64 `json:"remainingQuantity"`

	// Price is a fixed-point price in units of 1/10000.
	Price  int64     `json:"price"`
	Status string    `json:"status"`

	CreatedAt time.Time `json:"createdAt"`
}

type Trade struct {
	ID string `json:"id"`

	// Incoming/taker order.
	OrderID string `json:"orderId"`

	// Existing/resting/maker order.
	MatchOrderID string `json:"matchOrderId"`

	EventID string `json:"eventId"`
	Outcome string `json:"outcome"`

	BuyerID  string `json:"buyerId"`
	SellerID string `json:"sellerId"`

	// BUY or SELL of the taker.
	TakerSide string `json:"takerSide"`

	// Quantity is whole shares.
	Quantity int64 `json:"quantity"`

	// Price is a fixed-point price in units of 1/10000.
	Price int64 `json:"price"`

	CreatedAt time.Time `json:"createdAt"`
}

type OrderBook struct {
	// Bids: highest price first.
	// Same-price orders remain FIFO.
	Bids []*Order

	// Asks: lowest price first.
	// Same-price orders remain FIFO.
	Asks []*Order
}

type Market struct {
	YES *OrderBook
	NO  *OrderBook
}

func NewMarket() *Market {
	return &Market{
		YES: &OrderBook{},
		NO:  &OrderBook{},
	}
}

func (m *Market) Book(outcome string) *OrderBook {
	switch outcome {
	case OutcomeYes:
		return m.YES
	case OutcomeNo:
		return m.NO
	default:
		return nil
	}
}

type OrderBookEntry struct {
	Price    int64 `json:"price"`
	Quantity int64 `json:"quantity"`
	Total    int64 `json:"total"`

	// OrderCount is the number of individual resting orders at this price level.
	OrderCount int64 `json:"orderCount"`
}

type Depth struct {
	EventID string `json:"eventId"`

	Yes DepthSide `json:"yes"`
	No  DepthSide `json:"no"`
}

type DepthSide struct {
	Bids []OrderBookEntry `json:"bids"`
	Asks []OrderBookEntry `json:"asks"`
}