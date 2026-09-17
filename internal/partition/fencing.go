package partition

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// FencingTokens generates monotonically increasing tokens for a partition.
// A new token is produced every time the lease is acquired; the DB worker
// rejects writes whose token is lower than the last accepted one (P2.5).
type FencingTokens struct {
	client *redis.Client
}

func NewFencingTokens(client *redis.Client) *FencingTokens {
	return &FencingTokens{client: client}
}

// Next produces and returns the next fencing token for the given Redis key
// (typically router.FencingKey(partitionID)).
func (f *FencingTokens) Next(ctx context.Context, key string) (uint64, error) {
	return f.client.Incr(ctx, key).Uint64()
}

// Current returns the latest fencing token without incrementing, returning 0
// if the key does not yet exist.
func (f *FencingTokens) Current(ctx context.Context, key string) (uint64, error) {
	val, err := f.client.Get(ctx, key).Uint64()
	if err == redis.Nil {
		return 0, nil
	}

	return val, err
}
