package wal

import (
	"bytes"
	"context"
	"iter"
	"sync"
)

// followerConfig holds resolved Follower options.
type followerConfig struct {
	follow bool
}

// FollowerOption configures a Follower.
type FollowerOption func(*followerConfig)

// WithFollow enables follow mode: the Records iterator blocks at the tail and
// resumes when new records commit, instead of returning. Default is snapshot mode.
func WithFollow() FollowerOption {
	return func(cfg *followerConfig) { cfg.follow = true }
}

// Follower is a forward cursor over committed records, exposed as a range-over-func
// iterator. In snapshot mode it ends at the tail captured when the Follower was
// created; in follow mode it blocks at the tail and resumes on new commits. A
// Follower is NOT safe for concurrent use; use one per consumer goroutine, and
// call either Records or RecordsChan once.
type Follower struct {
	wal        *WAL
	cancel     context.CancelFunc
	err        error
	maxRecord  int
	endLSN     uint64
	fromLSN    uint64
	mu         sync.Mutex
	closeOnce  sync.Once
	follow     bool
	userClosed bool
}

// Follower creates a Follower yielding committed records with LSN >= fromLSN
// (0 means from the beginning). Options select snapshot (default) vs. follow mode.
// It returns ErrClosed if the WAL is closed.
func (w *WAL) Follower(fromLSN uint64, opts ...FollowerOption) (*Follower, error) {
	w.closeMu.RLock()
	closed := w.isClosed
	w.closeMu.RUnlock()

	if closed {
		return nil, ErrClosed
	}

	cfg := followerConfig{}
	for _, apply := range opts {
		apply(&cfg)
	}

	_, endLSN, _ := w.readerSnapshot()

	return &Follower{
		wal:       w,
		maxRecord: w.opts.maxRecordSize,
		endLSN:    endLSN,
		fromLSN:   fromLSN,
		follow:    cfg.follow,
	}, nil
}

// Records returns a range-over-func iterator yielding (LSN, payload) in order.
// Each payload is a fresh copy owned by the caller. Inspect Err after the loop
// ends. See the Follower doc for snapshot vs. follow semantics.
func (f *Follower) Records(ctx context.Context) iter.Seq2[uint64, []byte] {
	originalCtx := ctx
	ctx, cancel := context.WithCancel(ctx)

	f.mu.Lock()
	f.cancel = cancel
	alreadyClosed := f.userClosed
	f.mu.Unlock()

	return func(yield func(uint64, []byte) bool) {
		defer cancel()

		if alreadyClosed {
			return
		}

		highWater := uint64(0)
		if f.fromLSN > 0 {
			highWater = f.fromLSN - 1
		}

		delivered := false
		for {
			segmentNames, tail, firstLSN := f.wal.readerSnapshot()

			if (delivered || f.fromLSN > 0) && highWater+1 < firstLSN {
				f.err = ErrTruncated

				return
			}

			endLSN := tail
			if !f.follow {
				endLSN = f.endLSN
			}

			reader := newReaderFrom(f.wal.dir, segmentNames, highWater+1, endLSN, f.maxRecord)
			for reader.Next() {
				entry := reader.Entry()
				highWater = entry.LSN
				delivered = true

				if !yield(entry.LSN, bytes.Clone(entry.Payload)) {
					_ = reader.Close()

					return
				}
			}

			scanErr := reader.Err()
			_ = reader.Close()

			if scanErr != nil {
				f.err = scanErr

				return
			}

			if !f.follow {
				return
			}

			if !f.waitForCommit(ctx, originalCtx, highWater) {
				return
			}
		}
	}
}

// waitForCommit blocks until the next commit, returning false (and setting err
// when appropriate) if the loop should end. It is only used in follow mode.
func (f *Follower) waitForCommit(ctx, originalCtx context.Context, highWater uint64) bool {
	switch f.wal.awaitCommit(ctx, highWater) {
	case commitWoken:
		return true
	case commitClosed:
		f.err = ErrClosed

		return false
	case commitCanceled:
		f.mu.Lock()
		closedByUser := f.userClosed
		f.mu.Unlock()

		if !closedByUser {
			f.err = originalCtx.Err()
		}

		return false
	default:
		return false
	}
}

// Err returns the first terminal error, or nil if the loop ended cleanly (a
// snapshot reaching its tail, or Follower.Close). Call it only after the Records
// loop (or RecordsChan channel) has ended.
func (f *Follower) Err() error { return f.err }

// Close stops an in-flight Records loop and releases resources. It is idempotent
// and safe to call more than once.
func (f *Follower) Close() error {
	f.closeOnce.Do(func() {
		f.mu.Lock()
		f.userClosed = true
		if f.cancel != nil {
			f.cancel()
		}
		f.mu.Unlock()
	})

	return nil
}
