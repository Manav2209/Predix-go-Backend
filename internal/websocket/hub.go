package websocket

import (
	"sync"
)

type Hub struct {
	mu sync.RWMutex

	clients map[*Client]struct{}

	// eventID → connected clients
	subscriptions map[string]map[*Client]struct{}
}

func NewHub() *Hub {
	return &Hub{
		clients: make(
			map[*Client]struct{},
		),

		subscriptions: make(
			map[string]map[*Client]struct{},
		),
	}
}

func (h *Hub) Register(c *Client) {

	h.mu.Lock()
	defer h.mu.Unlock()

	h.clients[c] = struct{}{}
}

func (h *Hub) Unregister(c *Client) {

	h.mu.Lock()
	defer h.mu.Unlock()

	delete(
		h.clients,
		c,
	)

	for eventID, clients := range h.subscriptions {

		delete(
			clients,
			c,
		)

		if len(clients) == 0 {
			delete(
				h.subscriptions,
				eventID,
			)
		}
	}
}

func (h *Hub) Subscribe(
	c *Client,
	eventID string,
) {

	h.mu.Lock()
	defer h.mu.Unlock()

	clients, exists :=
		h.subscriptions[eventID]

	if !exists {

		clients = make(
			map[*Client]struct{},
		)

		h.subscriptions[eventID] = clients
	}

	clients[c] = struct{}{}

	c.Subscribe(eventID)
}

func (h *Hub) Unsubscribe(
	c *Client,
	eventID string,
) {

	h.mu.Lock()
	defer h.mu.Unlock()

	clients, exists :=
		h.subscriptions[eventID]

	if !exists {
		return
	}

	delete(
		clients,
		c,
	)

	if len(clients) == 0 {
		delete(
			h.subscriptions,
			eventID,
		)
	}

	c.Unsubscribe(eventID)
}

func (h *Hub) BroadcastEvent(
	eventID string,
	message []byte,
) {

	h.mu.RLock()

	clients :=
		h.subscriptions[eventID]

	// Copy clients while holding the read lock.
	targets :=
		make([]*Client, 0, len(clients))

	for client := range clients {
		targets = append(
			targets,
			client,
		)
	}

	h.mu.RUnlock()

	for _, client := range targets {
		client.Send(message)
	}
}
