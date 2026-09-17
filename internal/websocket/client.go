package websocket

import (
	"encoding/json"
	"github.com/gorilla/websocket"
	"log"
	"sync"
	"time"
)

const (
	writeWait = 10 * time.Second

	pongWait = 60 * time.Second

	pingPeriod = (pongWait * 9) / 10

	maxMessageSize = 64 * 1024

	sendBufferSize = 256
)

type Client struct {
	hub *Hub

	conn *websocket.Conn

	send chan []byte

	mu sync.RWMutex

	// event subscriptions.
	subscriptions map[string]struct{}

	closed bool
}

func NewClient(
	hub *Hub,
	conn *websocket.Conn,
) *Client {

	return &Client{
		hub:  hub,
		conn: conn,
		send: make(chan []byte, sendBufferSize),

		subscriptions: make(map[string]struct{}),
	}
}

func (c *Client) Subscribe(eventID string) {
	if eventID == "" {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.subscriptions[eventID] = struct{}{}
}

func (c *Client) Unsubscribe(eventID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.subscriptions, eventID)
}

func (c *Client) IsSubscribed(eventID string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	_, ok := c.subscriptions[eventID]

	return ok
}

func (c *Client) ReadPump() {
	defer func() {
		c.hub.Unregister(c)
		c.close()
	}()

	c.conn.SetReadLimit(maxMessageSize)

	_ = c.conn.SetReadDeadline(
		time.Now().Add(pongWait),
	)

	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(
			time.Now().Add(pongWait),
		)
	})

	for {
		_, message, err := c.conn.ReadMessage()

		if err != nil {
			return
		}

		c.handleMessage(message)
	}
}

func (c *Client) handleMessage(message []byte) {

	var msg IncomingMessage

	if err := unmarshalJSON(
		message,
		&msg,
	); err != nil {

		c.sendError(
			"invalid message",
		)

		return
	}

	switch msg.Method {

	case SubscribeMethod:
		c.handleSubscribe(msg.Params)

	case UnsubscribeMethod:
		c.handleUnsubscribe(msg.Params)

	default:
		c.sendError(
			"unknown method",
		)
	}
}

func (c *Client) WritePump() {

	ticker := time.NewTicker(
		pingPeriod,
	)

	defer func() {
		ticker.Stop()
		c.close()
	}()

	for {

		select {

		case message, ok := <-c.send:

			if !ok {
				_ = c.conn.WriteMessage(
					websocket.CloseMessage,
					[]byte{},
				)

				return
			}

			_ = c.conn.SetWriteDeadline(
				time.Now().Add(writeWait),
			)

			if err := c.conn.WriteMessage(
				websocket.TextMessage,
				message,
			); err != nil {
				return
			}

		case <-ticker.C:

			_ = c.conn.SetWriteDeadline(
				time.Now().Add(writeWait),
			)

			if err := c.conn.WriteMessage(
				websocket.PingMessage,
				nil,
			); err != nil {
				return
			}
		}
	}
}

func (c *Client) handleSubscribe(
	params []string,
) {
	if len(params) == 0 {
		c.sendError("subscription is required")
		return
	}

	for _, eventID := range params {

		if eventID == "" {
			continue
		}

		c.hub.Subscribe(
			c,
			eventID,
		)
	}
}

func (c *Client) handleUnsubscribe(
	params []string,
) {
	if len(params) == 0 {
		c.sendError("subscription is required")
		return
	}

	for _, eventID := range params {

		if eventID == "" {
			continue
		}

		c.hub.Unsubscribe(
			c,
			eventID,
		)
	}
}

func unmarshalJSON(
	data []byte,
	v any,
) error {
	return json.Unmarshal(data, v)
}

func (c *Client) Send(
	message []byte,
) {
	select {

	case c.send <- message:

	default:
		// Client is too slow.
		log.Printf(
			"websocket client buffer full",
		)

		c.close()
	}
}

func (c *Client) sendError(
	message string,
) {

	payload := map[string]any{
		"type": "error",
		"data": map[string]string{
			"error": message,
		},
	}

	data, err := json.Marshal(payload)

	if err != nil {
		return
	}

	c.Send(data)
}

func (c *Client) close() {

	c.mu.Lock()

	if c.closed {
		c.mu.Unlock()
		return
	}

	c.closed = true

	c.mu.Unlock()

	close(c.send)

	_ = c.conn.Close()
}
