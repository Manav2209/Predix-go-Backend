package partition

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Lease is a Redis-backed distributed lock that guarantees at most one owner
// per partition (P2.3). Ownership is lost after ttl unless renewed (P2.4).
//
// Acquire:  SET key instanceID NX PX ttl
// Renew:    if GET == instanceID then PEXPIRE ttl  (Lua, atomic)
// Release:  if GET == instanceID then DEL key      (Lua, atomic)
type Lease struct {
	client     *redis.Client
	key        string
	instanceID string
	ttl        time.Duration
}

func NewLease(client *redis.Client, key string, instanceID string, ttl time.Duration) *Lease {
	return &Lease{
		client:     client,
		key:        key,
		instanceID: instanceID,
		ttl:        ttl,
	}
}

// Acquire attempts to gain ownership of the lease. Returns true on success,
// false if another instance already holds it, or an error on failure.
func (l *Lease) Acquire(ctx context.Context) (bool, error) {
	return l.client.SetNX(ctx, l.key, l.instanceID, l.ttl).Result()
}

// compareAndRenew is a Lua script that atomically extends the lease only if
// the caller is still the owner (P2.4: "Verify ownership ... Do not blindly
// SET key instanceID").
var compareAndRenew = redis.NewScript(`
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		redis.call("PEXPIRE", KEYS[1], ARGV[2])
		return 1
	end
	return 0
`)

// Renew extends the lease TTL only if we are still the owner.
func (l *Lease) Renew(ctx context.Context) (bool, error) {
	n, err := compareAndRenew.Run(
		ctx,
		l.client,
		[]string{l.key},
		l.instanceID,
		l.ttl.Milliseconds(),
	).Int()

	if err != nil {
		return false, err
	}

	return n == 1, nil
}

// compareAndRelease is a Lua script that removes the lease only if the
// caller is still the owner.
var compareAndRelease = redis.NewScript(`
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("DEL", KEYS[1])
	end
	return 0
`)

// Release relinquishes ownership only if we are still the owner. Safe to
// call after a failover when a newer engine already holds the lease.
func (l *Lease) Release(ctx context.Context) error {
	_, err := compareAndRelease.Run(
		ctx,
		l.client,
		[]string{l.key},
		l.instanceID,
	).Result()

	return err
}
