package partition

import (
	"fmt"
	"hash/fnv"
)

// Router maps an event ID to a stable partition and stream name.
//
// The hash MUST remain identical across process restarts and deployments.
// It deliberately uses a fixed FNV-1a seed (no per-process randomization),
// satisfying the spec's requirement: "Do not use a random hash seed."
type Router struct {
	// Partitions is the total number of partitions (PARTITION_COUNT).
	Partitions int

	// StreamPrefix is the command stream base name, defaults to
	// COMMAND_STREAM_PREFIX (or "commands").
	StreamPrefix string
}

func NewRouter(partitions int, streamPrefix string) *Router {
	if partitions < 1 {
		partitions = 1
	}

	if streamPrefix == "" {
		streamPrefix = "commands"
	}

	return &Router{
		Partitions:   partitions,
		StreamPrefix: streamPrefix,
	}
}

// Partition returns the partition an event is routed to.
func (r *Router) Partition(eventID string) int {
	return int(hash64(eventID) % uint64(r.Partitions))
}

// Stream returns the command stream name for a partition.
func (r *Router) Stream(partition int) string {
	return fmt.Sprintf("%s:partition:%d", r.StreamPrefix, partition)
}

// StreamForEvent returns the command stream name an event must use.
func (r *Router) StreamForEvent(eventID string) string {
	return r.Stream(r.Partition(eventID))
}

// Group returns the consumer group name for a partition. Group names are
// per-partition so replicas cannot load-balance one partition between them.
func (r *Router) Group(partitionID int) string {
	return fmt.Sprintf("engines:p:%d", partitionID)
}

// SequenceKey returns the durable per-command sequence counter for a
// partition (P2.8).
func (r *Router) SequenceKey(partitionID int) string {
	return fmt.Sprintf("%s:%d:sequence", r.StreamPrefix, partitionID)
}

// FencingKey returns the durable fencing-token counter for a partition.
func (r *Router) FencingKey(partitionID int) string {
	return FencingKey(partitionID)
}

// LeaseKey returns the ownership lease key for a partition (P2.3).
func (r *Router) LeaseKey(partitionID int) string {
	return LeaseKey(partitionID)
}

// FencingKey is the stable Redis key backing per-partition fencing tokens.
// It doubles as the DB worker's read fence: rejects writes from a stale
// engine token (P2.5).
func FencingKey(partitionID int) string {
	return fmt.Sprintf("engine:fencing:%d", partitionID)
}

// LeaseKey is the stable Redis key backing per-partition ownership (P2.3).
func LeaseKey(partitionID int) string {
	return fmt.Sprintf("engine:lease:%d", partitionID)
}

// PartitionStreams returns every partition command stream name in order.
func (r *Router) PartitionStreams() []string {
	streams := make([]string, 0, r.Partitions)

	for p := 0; p < r.Partitions; p++ {
		streams = append(streams, r.Stream(p))
	}

	return streams
}

// hash64 hashes a string to a uint64 using FNV-1a.
func hash64(s string) uint64 {
	h := fnv.New64a()

	// Write never fails per hash.Hash contract.
	_, _ = h.Write([]byte(s))

	return h.Sum64()
}