package engine

import (
	"context"
	"fmt"
	"log"
	"time"

	"predix/internal/partition"
)

// partitionWorker tracks the runtime state of one partition this engine
// instance is responsible for.
type partitionWorker struct {
	partitionID int
}

// AddPartition registers partitionID with this engine as a candidate owner.
// The worker goroutine starts once Start() is called. Only the lease holder
// ever processes a partition's command stream (P2.3).
func (e *Engine) AddPartition(partitionID int) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if partitionID < 0 || partitionID >= e.router.Partitions {
		return fmt.Errorf("partition %d out of range [0,%d)", partitionID, e.router.Partitions)
	}

	if _, exists := e.partitions[partitionID]; exists {
		return fmt.Errorf("partition %d already configured", partitionID)
	}

	e.partitions[partitionID] = &partitionWorker{
		partitionID: partitionID,
	}

	return nil
}

// runPartition is the per-partition ownership loop:
//
//	acquire lease → obtain fencing token → replay state → consume → on loss, re-acquire
//
// It exits only when the engine shuts down.
func (e *Engine) runPartition(partitionID int, _ *partitionWorker) {
	client := e.redisManager.GetClient()
	fencing := partition.NewFencingTokens(client)
	backoff := e.leaseRenewInterval

	for e.ctx.Err() == nil {
		lease := partition.NewLease(
			client,
			e.router.LeaseKey(partitionID),
			e.engineID,
			e.leaseTTL,
		)

		ok, err := lease.Acquire(e.ctx)
		if err != nil {
			if e.ctx.Err() != nil {
				return
			}

			log.Printf(
				"lease acquire error partition %d: %v",
				partitionID,
				err,
			)

			if !waitPartition(e.ctx, e.leaseRenewInterval) {
				return
			}
			continue
		}

		if !ok {
			// Another engine owns this partition; wait for it to lapse.
			if !waitPartition(e.ctx, e.leaseRenewInterval) {
				return
			}
			continue
		}

		token, err := fencing.Next(e.ctx, e.router.FencingKey(partitionID))
		if err != nil {
			releaseLease(lease)
			if e.ctx.Err() != nil {
				return
			}

			// Without a fencing token we cannot stamp downstream writes;
			// give the lease back and retry.
			log.Printf(
				"fencing token error partition %d: %v",
				partitionID,
				err,
			)

			if !waitPartition(e.ctx, e.leaseRenewInterval) {
				return
			}
			continue
		}

		e.metrics.LeaseAcquire.WithLabelValues(
			e.engineID,
			fmt.Sprintf("%d", partitionID),
		).Inc()

		log.Printf(
			"engine %s acquired partition %d (fencing token %d)",
			e.engineID,
			partitionID,
			token,
		)

		e.logEvent("partition_lease_acquired",
			"partitionId", partitionID,
			"fencingToken", token,
		)

		// partition ctx is canceled when the lease is lost so the consumer
		// stops reading; the outer loop then competes for the lease again.
		pctx, pcancel := context.WithCancel(e.ctx)

		renewDone := make(chan struct{})
		go func() {
			defer close(renewDone)
			e.renewLease(pctx, pcancel, lease, partitionID)
		}()

		replayErr := e.replayCommandLog(pctx, partitionID)
		if replayErr != nil {
			log.Printf("replay failed; partition %d is unavailable: %v", partitionID, replayErr)
			e.logEvent("event_replay_failed", "partitionId", partitionID, "error", replayErr.Error())
		} else if pctx.Err() == nil {
			// Publish readiness only after recovery has completed.
			e.mu.Lock()
			e.partitionToken[partitionID] = token
			e.mu.Unlock()
			e.metrics.AcquiredPartitions.WithLabelValues(e.engineID, fmt.Sprintf("%d", partitionID)).Set(1)
			backoff = e.leaseRenewInterval
			e.consumeMessages(pctx, partitionID)
		}

		pcancel()
		<-renewDone

		// Only release if we still own it; compare-and-release makes this
		// safe even if another engine already took over.
		releaseLease(lease)

		e.mu.Lock()
		delete(e.partitionToken, partitionID)
		e.mu.Unlock()

		e.metrics.AcquiredPartitions.WithLabelValues(
			e.engineID,
			fmt.Sprintf("%d", partitionID),
		).Set(0)

		log.Printf(
			"engine %s released partition %d",
			e.engineID,
			partitionID,
		)
		if replayErr != nil {
			if !waitPartition(e.ctx, backoff) {
				return
			}
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
}

func waitPartition(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func releaseLease(lease *partition.Lease) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := lease.Release(ctx); err != nil {
		log.Printf("lease release error: %v", err)
	}
}

// renewLease periodically re-verifies ownership and extends the lease TTL.
// If renewal fails (the lease was stolen or expired), it cancels the
// partition context so the consumer stops and failover can proceed.
func (e *Engine) renewLease(
	pctx context.Context,
	pcancel context.CancelFunc,
	lease *partition.Lease,
	partitionID int,
) {

	ticker := time.NewTicker(e.leaseRenewInterval)
	defer ticker.Stop()

	for {
		select {
		case <-pctx.Done():
			return

		case <-ticker.C:
			ok, err := lease.Renew(pctx)
			if err != nil {
				if pctx.Err() != nil {
					return
				}

				// Ownership cannot be verified, so stop replay/consumption.
				log.Printf(
					"lease renew error partition %d: %v",
					partitionID,
					err,
				)
				pcancel()
				return
			}

			if !ok {
				log.Printf(
					"lease lost for partition %d; yielding ownership",
					partitionID,
				)

				e.metrics.LeaseLosses.WithLabelValues(
					e.engineID,
					fmt.Sprintf("%d", partitionID),
				).Inc()

				e.logEvent("partition_failover",
					"partitionId", partitionID,
				)

				pcancel()
				return
			}
		}
	}
}
