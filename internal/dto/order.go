package dto

type CreateOrderRequest struct {
	OrderID   string  `json:"orderId" binding:"omitempty"`
	EventID   string  `json:"eventId" binding:"required"`
	OrderType string  `json:"orderType" binding:"required,oneof=LIMIT MARKET"`
	Outcome   string  `json:"outcome" binding:"required,oneof=YES NO"`
	Side      string  `json:"side" binding:"required,oneof=BUY SELL"`
	Quantity  int64   `json:"quantity" binding:"required,gt=0"`
	Price     float64 `json:"price" binding:"omitempty,gt=0"`
}

type OrderResponse struct {
	ID        string      `json:"id"`
	EventID   string      `json:"eventId"`
	UserID    string      `json:"userId"`
	OrderType string      `json:"orderType"`
	Outcome   string      `json:"outcome"`
	Side      string      `json:"side"`
	Quantity  int64       `json:"quantity"`
	Price     int64       `json:"price"`
	Status    string      `json:"status"`
	CreatedAt string      `json:"createdAt"`
}