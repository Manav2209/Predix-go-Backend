package engine

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"sync"

	"predix/internal/events"

	rd "github.com/redis/go-redis/v9"
)

// Outbox is a simple append-only, fsynced journal of events that have been
// produced but not yet durably published to the events:out stream and the
// WebSocket channel.
//
// emitEvent appends before publishing and truncates once both targets accept
// the envelope. If Redis disappears mid-publish, the entry survives on disk and
// is republished on the next start (Recover). This guarantees no trade is lost
// because Redis blipped, without blocking the engine indefinitely.
type Outbox struct {
	mu   sync.Mutex
	path string
	file *os.File
}

func NewOutbox(path string) (*Outbox, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &Outbox{path: path, file: file}, nil
}

// Append writes one envelope line durably.
func (o *Outbox) Append(data []byte) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if _, err := o.file.Write(append(data, '\n')); err != nil {
		return err
	}
	return o.file.Sync()
}

// Reset truncates the journal back to zero bytes, keeping the file handle
// open for future appends. Called once every appended envelope has been
// accepted by both Redis targets.
func (o *Outbox) Reset() error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.file == nil {
		return nil
	}

	if err := o.file.Truncate(0); err != nil {
		return err
	}

	_, err := o.file.Seek(0, 0)
	return err
}

// ReadAll returns the pending envelopes currently on disk.
func (o *Outbox) ReadAll() ([][]byte, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	data, err := os.ReadFile(o.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}

	var lines [][]byte
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		if len(line) > 0 {
			lines = append(lines, line)
		}
	}
	return lines, scanner.Err()
}

// Recover republishes any envelopes left in the outbox (e.g. after a Redis
// outage) and clears the journal. A Redis outage keeps the entries safe on
// disk; they are only dropped once every target accepts them.
func (o *Outbox) Recover(
	ctx context.Context,
	client *rd.Client,
	stream string,
	wsChannel string,
) error {
	lines, err := o.ReadAll()
	if err != nil {
		return err
	}

	if len(lines) == 0 {
		return nil
	}

	log.Printf("outbox: republishing %d pending event(s)", len(lines))

	for _, line := range lines {
		if err := republishEnvelope(
			ctx,
			client,
			line,
			stream,
			wsChannel,
		); err != nil {
			return err
		}
	}

	return o.Reset()
}

// republishEnvelope decodes and pushes one stored envelope to both the event
// stream and the WebSocket channel.
func republishEnvelope(
	ctx context.Context,
	client *rd.Client,
	line []byte,
	stream string,
	wsChannel string,
) error {
	var env events.EventEnvelope

	if err := json.Unmarshal(line, &env); err != nil {
		log.Printf("outbox: skip corrupt entry: %v", err)
		return nil
	}

	envBytes, err := json.Marshal(env)
	if err != nil {
		log.Printf("outbox: skip marshal error: %v", err)
		return nil
	}

	return publishEnvelope(
		ctx,
		client,
		envBytes,
		env.Type == events.EventTradeExecuted,
		stream,
		wsChannel,
	)
}

// publishEnvelope writes an envelope to the durable event stream and, when
// requested, to the WebSocket fan-out channel.
func publishEnvelope(
	ctx context.Context,
	client *rd.Client,
	envBytes []byte,
	broadcast bool,
	stream string,
	wsChannel string,
) error {

	if err := client.XAdd(ctx, &rd.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{
			"event": string(envBytes),
		},
	}).Err(); err != nil {
		return err
	}

	if broadcast {
		if err := client.Publish(ctx, wsChannel, envBytes).Err(); err != nil {
			return err
		}
	}

	return nil
}

func (o *Outbox) Close() error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.file == nil {
		return nil
	}
	err := o.file.Close()
	o.file = nil
	return err
}