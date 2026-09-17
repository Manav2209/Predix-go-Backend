package websocket

const (
	SubscribeMethod   = "SUBSCRIBE"
	UnsubscribeMethod = "UNSUBSCRIBE"
)

type IncomingMessage struct {
	Method string   `json:"method"`
	Params []string `json:"params"`
}

type OutgoingMessage struct {
	Type string `json:"type"`
	Data any    `json:"data"`
}

type TradeData struct {
	Price     int64  `json:"price"`
	Quantity  int64  `json:"quantity"`
	Timestamp int64  `json:"timestamp"`
	EventID   string `json:"eventId"`
	UserID    string `json:"userId"`
	Outcome   string `json:"outcome"`
	Side      string `json:"side"`
}

type TradeMessage struct {
	Type string    `json:"type"`
	Data TradeData `json:"data"`
}

type DepthEntry struct {
	Price    int64 `json:"price"`
	Quantity int64 `json:"quantity"`
	Total    int64 `json:"total"`
}

type DepthData struct {
	YES struct {
		Asks []DepthEntry `json:"asks"`
		Bids []DepthEntry `json:"bids"`
	} `json:"YES"`

	NO struct {
		Asks []DepthEntry `json:"asks"`
		Bids []DepthEntry `json:"bids"`
	} `json:"NO"`
}

type DepthMessage struct {
	Type string    `json:"type"`
	Data DepthData `json:"data"`
}
