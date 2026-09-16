package dto

type OrderbookDepthResponse struct {
	Yes struct {
		Buy  []OrderBookEntry `json:"BUY"`
		Sell []OrderBookEntry `json:"SELL"`
	} `json:"YES"`
	No struct {
		Buy  []OrderBookEntry `json:"BUY"`
		Sell []OrderBookEntry `json:"SELL"`
	} `json:"NO"`
}

type OrderBookEntry struct {
	Price    int64 `json:"price"`
	Quantity int64 `json:"quantity"`
	Total    int64 `json:"total"`
}