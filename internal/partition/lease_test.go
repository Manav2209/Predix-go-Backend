package partition

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func startTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()

	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}

	t.Cleanup(srv.Close)

	client := redis.NewClient(&redis.Options{
		Addr: srv.Addr(),
	})

	t.Cleanup(func() { client.Close() })

	return srv, client
}

func TestLeaseAcquireAndRenew(t *testing.T) {
	_, client := startTestRedis(t)

	ctx := context.Background()
	lease := NewLease(client, "engine:lease:0", "inst-A", 100*time.Millisecond)

	ok, err := lease.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	if !ok {
		t.Fatal("first acquire should succeed")
	}

	// Same instance can renew.
	renewed, err := lease.Renew(ctx)
	if err != nil {
		t.Fatalf("renew: %v", err)
	}

	if !renewed {
		t.Fatal("renewal should succeed for the owner")
	}
}

func TestLeaseRejectsForeignRenew(t *testing.T) {
	_, client := startTestRedis(t)

	ctx := context.Background()
	leaseA := NewLease(client, "engine:lease:0", "inst-A", 1*time.Second)
	leaseB := NewLease(client, "engine:lease:0", "inst-B", 1*time.Second)

	ok, err := leaseA.Acquire(ctx)
	if err != nil || !ok {
		t.Fatal("inst-A should acquire")
	}

	renewed, err := leaseB.Renew(ctx)
	if err != nil {
		t.Fatalf("renew: %v", err)
	}

	if renewed {
		t.Fatal("foreign renewal must fail")
	}
}

func TestLeaseExpireAndReacquire(t *testing.T) {
	srv, client := startTestRedis(t)

	ctx := context.Background()
	leaseA := NewLease(client, "engine:lease:0", "inst-A", 50*time.Millisecond)

	ok, err := leaseA.Acquire(ctx)
	if err != nil || !ok {
		t.Fatal("inst-A should acquire")
	}

	// Fast-forward past TTL.
	srv.FastForward(100 * time.Millisecond)

	leaseB := NewLease(client, "engine:lease:0", "inst-B", 50*time.Millisecond)
	ok, err = leaseB.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire after expire: %v", err)
	}

	if !ok {
		t.Fatal("inst-B should acquire after inst-A's lease expired")
	}
}

func TestLeaseRelease(t *testing.T) {
	_, client := startTestRedis(t)

	ctx := context.Background()
	lease := NewLease(client, "engine:lease:0", "inst-A", 1*time.Second)

	_, _ = lease.Acquire(ctx)

	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}

	// Should be acquirable now.
	other := NewLease(client, "engine:lease:0", "inst-B", 1*time.Second)
	ok, err := other.Acquire(ctx)
	if err != nil || !ok {
		t.Fatal("should be able to acquire after release")
	}
}

func TestLeaseReleaseDoesNotStealForeign(t *testing.T) {
	_, client := startTestRedis(t)

	ctx := context.Background()

	// Inst-A owns the lease.
	leaseA := NewLease(client, "engine:lease:0", "inst-A", 1*time.Second)
	_, _ = leaseA.Acquire(ctx)

	// Inst-B tries to release it — should be a no-op.
	leaseB := NewLease(client, "engine:lease:0", "inst-B", 1*time.Second)
	if err := leaseB.Release(ctx); err != nil {
		t.Fatalf("release error: %v", err)
	}

	// Lease still belongs to A.
	ok, _ := leaseA.Renew(ctx)
	if !ok {
		t.Fatal("inst-A should still own the lease")
	}
}

func TestFencingTokensMonotonic(t *testing.T) {
	_, client := startTestRedis(t)

	ctx := context.Background()
	ft := NewFencingTokens(client)
	key := "engine:fencing:0"

	var prev uint64
	for i := 0; i < 10; i++ {
		n, err := ft.Next(ctx, key)
		if err != nil {
			t.Fatalf("next: %v", err)
		}

		if n <= prev {
			t.Fatalf("token %d <= %d (must be monotonically increasing)", n, prev)
		}

		prev = n
	}
}

func TestFencingTokensCurrent(t *testing.T) {
	_, client := startTestRedis(t)

	ctx := context.Background()
	ft := NewFencingTokens(client)
	key := "engine:fencing:0"

	// Key doesn't exist yet: current returns 0.
	n, err := ft.Current(ctx, key)
	if err != nil {
		t.Fatalf("current: %v", err)
	}

	if n != 0 {
		t.Fatalf("expected 0, got %d", n)
	}

	_, _ = ft.Next(ctx, key)
	_, _ = ft.Next(ctx, key)

	n, err = ft.Current(ctx, key)
	if err != nil {
		t.Fatalf("current: %v", err)
	}

	if n != 2 {
		t.Fatalf("expected 2, got %d", n)
	}
}
