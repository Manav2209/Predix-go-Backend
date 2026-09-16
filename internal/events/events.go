// Package events defines the unified, versioned envelope that every
// downstream consumer (DB worker, WebSocket) decodes.
//
// The engine is the only producer. It writes envelopes to the events:out
// stream (consumed by the DB worker) and publishes the same envelope to the
// ws:updates channel (consumed by the WebSocket server).
package events

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// EventStream is the Redis Stream carrying settled domain events to the
// DB worker.
const EventStream = "events:out"

// EventGroup is the consumer group name shared by DB worker replicas.
const EventGroup = "db-workers"

// WSChannel is the pub/sub channel the WebSocket server consumes for
// real-time fan-out. Pub/sub is fine here because it is a notification
// channel, not a queue.
const WSChannel = "ws:updates"

type EventType string

const (
	EventTradeExecuted EventType = "trade_executed"
	EventOrderCreated  EventType = "order_created"
	EventOrderStatus   EventType = "order_status"
	EventOrderCanceled EventType = "order_canceled"
	EventDepthUpdated  EventType = "depth_updated"
)

// EventEnvelope wraps every domain event.
type EventEnvelope struct {
	ID          string          `json:"id"`
	Type        EventType       `json:"type"`
	PartitionID int             `json:"partitionId"`
	Sequence    uint64          `json:"sequence"`
	CreatedAt   time.Time       `json:"createdAt"`
	Data        json.RawMessage `json:"data"`
}

type NewEnvelopeParams struct {
	Type        EventType
	Data        []byte
	PartitionID int
	Sequence    uint64
}

// NewEnvelope builds a fully-populated envelope.
func NewEnvelope(p NewEnvelopeParams) EventEnvelope {
	return EventEnvelope{
		ID:          uuid.NewString(),
		Type:        p.Type,
		PartitionID: p.PartitionID,
		Sequence:    p.Sequence,
		CreatedAt:   time.Now().UTC(),
		Data:        p.Data,
	}
}