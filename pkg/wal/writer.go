package wal

import (
	"context"
	"os"
	"slices"
	"time"

	"github.com/barnowlsnest/go-wallib/internal/record"
	"github.com/barnowlsnest/go-wallib/internal/segment"
)

// appendRequest is a single append handed to the writer goroutine.
type appendRequest struct {
	ctx      context.Context
	resultCh chan appendResult
	payloads [][]byte
}

// appendResult carries the assigned LSNs (or an error) back to the caller.
type appendResult struct {
	err          error
	assignedLSNs []uint64
}

// writerOpRequest carries a boundary operation to the writer goroutine: a
// Truncate (delete whole segments below upTo) or a CutOffset (delete every record
// below upTo, rewriting the boundary segment if upTo falls inside one). resultCh
// receives the outcome. The destination channel selects which operation runs.
type writerOpRequest struct {
	resultCh chan error
	upTo     uint64
}

// wakeup is a follower's one-shot subscription: the writer closes notify when a
// commit advances past from, or on shutdown. from is the highest LSN the follower
// has already consumed, so the writer can signal immediately if it is behind.
type wakeup struct {
	notify chan struct{}
	from   uint64
}

// commitWait is the outcome of awaitCommit.
type commitWait int

const (
	// commitWoken means a commit advanced past the waiter's position (or the WAL
	// is shutting down and closed the waiter); the caller should re-check state.
	commitWoken commitWait = iota
	// commitCanceled means the caller's context was canceled while waiting.
	commitCanceled
	// commitClosed means the WAL was already closing when the follower tried to subscribe.
	commitClosed
)

// awaitCommit parks until a commit advances past from, ctx is canceled, or the
// WAL closes. It is safe to call from any goroutine; the subscription is handled
// on the writer goroutine, so there is no lost-wakeup window.
func (w *WAL) awaitCommit(ctx context.Context, from uint64) commitWait {
	notify := make(chan struct{})

	select {
	case w.subscribeCh <- wakeup{
		notify: notify,
		from:   from,
	}:
	case <-ctx.Done():
		return commitCanceled
	case <-w.closed:
		return commitClosed
	}

	// No <-w.closed case here: on shutdown the writer's wakeFollowers closes the
	// parked notify, releasing a parked waiter as commitWoken (the follower then
	// re-subscribes and the first select returns commitClosed). Removing the
	// w.closed case makes the outcome deterministic rather than racing notify
	// against w.closed when both are ready at shutdown.
	select {
	case <-notify:
		return commitWoken
	case <-ctx.Done():
		return commitCanceled
	}
}

// handleSubscribe runs on the writer goroutine. It signals an already-behind
// waiter immediately, otherwise parks it until the next commit.
func (w *WAL) handleSubscribe(sub wakeup) {
	if w.LastLSN() > sub.from {
		close(sub.notify)

		return
	}

	w.pendingWaiters = append(w.pendingWaiters, sub.notify)
}

// wakeFollowers runs on the writer goroutine. It closes every parked waiter so
// followers re-check state, then clears the list. close never blocks, so a slow
// follower can never stall the writer.
func (w *WAL) wakeFollowers() {
	for _, notify := range w.pendingWaiters {
		close(notify)
	}

	w.pendingWaiters = w.pendingWaiters[:0]
}

// AppendBatch durably writes multiple records as one group commit and returns
// their assigned LSNs in order. See Append for delivery semantics.
func (w *WAL) AppendBatch(ctx context.Context, payloads [][]byte) ([]uint64, error) {
	for _, payload := range payloads {
		if record.EncodedSize(len(payload)) > w.opts.maxRecordSize {
			return nil, ErrRecordTooLarge
		}
	}

	request := &appendRequest{
		ctx:      ctx,
		resultCh: make(chan appendResult, 1),
		payloads: payloads,
	}

	if err := w.enqueue(ctx, request); err != nil {
		return nil, err
	}

	result := <-request.resultCh

	return result.assignedLSNs, result.err
}

// enqueue hands a request to the writer goroutine, rejecting appends on a closed
// WAL and honoring context cancellation while waiting for queue space.
func (w *WAL) enqueue(ctx context.Context, request *appendRequest) error {
	w.closeMu.RLock()
	defer w.closeMu.RUnlock()

	if w.isClosed {
		return ErrClosed
	}

	select {
	case w.requestCh <- request:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// startWriter launches the single writer goroutine (the Singular Update Queue).
func (w *WAL) startWriter() {
	w.wg.Go(w.writerLoop)
}

// writerLoop is the body of the single writer goroutine: it serializes all
// appends and flushes and, on Close, drains queued requests before a final
// flush.
func (w *WAL) writerLoop() {
	for {
		select {
		case request := <-w.requestCh:
			w.commit(w.gather(request))
		case responseCh := <-w.controlCh:
			responseCh <- w.syncActive()
		case request := <-w.truncateCh:
			request.resultCh <- w.doTruncate(request.upTo)
		case request := <-w.cutCh:
			request.resultCh <- w.doCut(request.upTo)
		case sub := <-w.subscribeCh:
			w.handleSubscribe(sub)
		case <-w.closed:
			w.drain()
			w.finalFlush()
			w.wakeFollowers()

			return
		}
	}
}

// gather coalesces a group commit: the triggering request plus any others that
// are (or, for SyncBatched, soon become) available, up to batchSize.
func (w *WAL) gather(first *appendRequest) []*appendRequest {
	batch := []*appendRequest{first}

	if w.opts.syncPolicy == SyncBatched && w.opts.batchTimeout > 0 {
		return w.gatherUntilTimeout(batch)
	}

	return w.gatherNonBlocking(batch)
}

// gatherUntilTimeout lingers up to batchTimeout to accumulate a fuller batch.
func (w *WAL) gatherUntilTimeout(batch []*appendRequest) []*appendRequest {
	timer := time.NewTimer(w.opts.batchTimeout)
	defer timer.Stop()

	for len(batch) < w.opts.batchSize {
		select {
		case request := <-w.requestCh:
			batch = append(batch, request)
		case <-timer.C:
			return batch
		}
	}

	return batch
}

// gatherNonBlocking takes only the requests already queued, without waiting.
func (w *WAL) gatherNonBlocking(batch []*appendRequest) []*appendRequest {
	for len(batch) < w.opts.batchSize {
		select {
		case request := <-w.requestCh:
			batch = append(batch, request)
		default:
			return batch
		}
	}

	return batch
}

// drain processes appends handed off before Close was observed.
//
// This is not a busy loop: Close sets isClosed under closeMu before signaling
// w.closed, and enqueue holds closeMu.RLock across its isClosed check and its
// send. So once shutdown is observed no new request can enter requestCh; drain
// commits the bounded set already buffered and then the default case returns.
func (w *WAL) drain() {
	for {
		select {
		case request := <-w.requestCh:
			w.commit(w.gatherNonBlocking([]*appendRequest{request}))
		default:
			return
		}
	}
}

// commit writes a batch of append requests, fsyncs once per the active policy,
// and delivers each request's result. It runs only on the writer goroutine.
func (w *WAL) commit(batch []*appendRequest) {
	results := make([]appendResult, len(batch))
	wroteAny := false

	for i, request := range batch {
		if err := request.ctx.Err(); err != nil {
			results[i] = appendResult{err: err}

			continue
		}

		assignedLSNs, err := w.writeRequest(request)
		if err != nil {
			results[i] = appendResult{err: err}

			continue
		}

		results[i] = appendResult{assignedLSNs: assignedLSNs}
		wroteAny = true
	}

	if wroteAny {
		w.finishBatch(results)
	}

	for i, request := range batch {
		request.resultCh <- results[i]
	}
}

// finishBatch fsyncs the group commit (unless the interval flusher owns
// durability) and publishes the new bounds. If the fsync fails the durability
// guarantee is broken, so the error replaces the success result of every request
// that had written.
func (w *WAL) finishBatch(results []appendResult) {
	if err := w.flushAfterCommit(); err != nil {
		for i := range results {
			if results[i].err == nil {
				results[i] = appendResult{err: err}
			}
		}

		return
	}

	w.publishLastLSN()
	w.wakeFollowers()
}

// flushAfterCommit fsyncs the active segment unless SyncInterval defers that to
// the background flusher.
func (w *WAL) flushAfterCommit() error {
	if w.opts.syncPolicy == SyncInterval {
		return nil
	}

	return w.active.Sync()
}

// writeRequest appends every payload in one request, assigning gapless LSNs.
func (w *WAL) writeRequest(request *appendRequest) ([]uint64, error) {
	assignedLSNs := make([]uint64, 0, len(request.payloads))
	for _, payload := range request.payloads {
		lsn := w.nextLSN
		if err := w.writeRecord(lsn, payload); err != nil {
			return nil, err
		}

		w.nextLSN++
		assignedLSNs = append(assignedLSNs, lsn)
	}

	return assignedLSNs, nil
}

// writeRecord applies the segment-roll rule, then appends one record to the
// active segment.
func (w *WAL) writeRecord(lsn uint64, payload []byte) error {
	needed := int64(record.EncodedSize(len(payload)))
	if w.active.Size() > segment.HeaderSize && w.active.Size()+needed > w.opts.maxSegmentSize {
		if err := w.roll(lsn); err != nil {
			return err
		}
	}

	_, err := w.active.Append(lsn, payload)

	return err
}

// roll seals the active segment and starts a fresh one based at baseLSN.
func (w *WAL) roll(baseLSN uint64) error {
	if err := w.active.Sync(); err != nil {
		return err
	}

	newSegment, err := segment.Create(w.root, baseLSN)
	if err != nil {
		return err
	}

	sealed := w.active

	w.mu.Lock()
	w.active = newSegment
	w.segmentBaseLSNs = append(w.segmentBaseLSNs, baseLSN)
	w.mu.Unlock()

	w.opts.logger.Debug("wal: rolled to new segment",
		Field{Key: "sealedBaseLSN", Value: sealed.BaseLSN()},
		Field{Key: "newBaseLSN", Value: baseLSN},
	)

	return sealed.Close()
}

// publishLastLSN updates the externally visible LSN bounds after a commit.
func (w *WAL) publishLastLSN() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.lastLSN = w.nextLSN - 1
	if w.firstLSN == 0 {
		w.firstLSN = w.segmentBaseLSNs[0]
	}
}

// syncActive fsyncs the active segment, used by Sync and the interval flusher.
func (w *WAL) syncActive() error {
	if w.active == nil {
		return nil
	}

	return w.active.Sync()
}

// doTruncate deletes whole segments whose entries are all below upTo. It runs on
// the writer goroutine, so it cannot race with appends or rolls. The active
// (last) segment is never deleted.
func (w *WAL) doTruncate(upTo uint64) error {
	w.mu.RLock()
	baseLSNs := slices.Clone(w.segmentBaseLSNs)
	w.mu.RUnlock()

	// Segment i is fully below upTo iff the next segment's base LSN (= segment
	// i's lastLSN + 1) is <= upTo. The last segment is active and never counted.
	deletable := 0
	for i := 0; i+1 < len(baseLSNs); i++ {
		if baseLSNs[i+1] > upTo {
			break
		}

		deletable = i + 1
	}

	if deletable == 0 {
		return nil
	}

	for i := range deletable {
		if err := w.removeSegmentFile(baseLSNs[i]); err != nil {
			return err
		}

		w.opts.logger.Debug("wal: deleted truncated segment",
			Field{Key: "baseLSN", Value: baseLSNs[i]},
		)
	}

	w.mu.Lock()
	w.segmentBaseLSNs = w.segmentBaseLSNs[deletable:]
	newFirstLSN := w.segmentBaseLSNs[0]
	w.firstLSN = newFirstLSN
	w.mu.Unlock()

	w.opts.logger.Info("wal: truncated log",
		Field{Key: "segmentsDeleted", Value: deletable},
		Field{Key: "upTo", Value: upTo},
		Field{Key: "firstLSN", Value: newFirstLSN},
	)

	return nil
}

// removeSegmentFile deletes one (already closed) segment file and fsyncs the
// directory so the removal is durable. A file that is already gone is treated as
// success: the delete's goal — that segment file not existing — is already met.
// This keeps a cut or truncate that failed partway (leaving the in-memory index
// listing a base whose file was already removed) from wedging a later delete of
// that base on a not-exist error, until recovery reconciles the index.
func (w *WAL) removeSegmentFile(baseLSN uint64) error {
	if err := w.root.Remove(segment.Name(baseLSN)); err != nil && !os.IsNotExist(err) {
		return err
	}

	return segment.SyncDir(w.dir)
}

// finalFlush fsyncs the active segment during shutdown, logging any failure.
func (w *WAL) finalFlush() {
	if w.active == nil {
		return
	}

	if err := w.active.Sync(); err != nil {
		w.opts.logger.Error("wal: final flush failed", Field{Key: "error", Value: err.Error()})
	}
}

// startFlusher launches the periodic background fsync goroutine for SyncInterval.
func (w *WAL) startFlusher() {
	w.wg.Go(w.flushLoop)
}

// flushLoop periodically asks the writer goroutine to fsync, until Close.
func (w *WAL) flushLoop() {
	ticker := time.NewTicker(w.opts.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.requestIntervalFlush()
		case <-w.closed:
			return
		}
	}
}

// requestIntervalFlush asks the writer goroutine for a flush, giving up if the
// WAL closes before the request is accepted.
func (w *WAL) requestIntervalFlush() {
	done := make(chan error, 1)
	select {
	case w.controlCh <- done:
		if err := <-done; err != nil {
			w.opts.logger.Error("wal: interval flush failed", Field{Key: "error", Value: err.Error()})
		}
	case <-w.closed:
	}
}

// doCut deletes every record below upTo, running on the writer goroutine so it
// cannot race appends, rolls, or truncates. Whole segments below the cut are
// deleted; the segment containing the cut (possibly the active one) is rewritten
// into seg-<cut>. seg-<cut> is created durably before any delete, so an
// interrupted cut leaves an overlap that recovery reconciles rather than losing
// data.
func (w *WAL) doCut(upTo uint64) error {
	w.mu.RLock()
	baseLSNs := slices.Clone(w.segmentBaseLSNs)
	firstLSN := w.firstLSN
	nextLSN := w.nextLSN
	w.mu.RUnlock()

	cut := min(upTo, nextLSN) // cannot cut LSNs that do not exist yet

	switch {
	case firstLSN == 0 || cut <= firstLSN:
		return nil
	case cut >= nextLSN:
		return w.cutEverything(baseLSNs, nextLSN)
	}

	boundary := boundaryIndex(baseLSNs, cut)
	if baseLSNs[boundary] == cut {
		return w.cutDeleteBelow(baseLSNs, boundary, cut)
	}

	return w.cutRewriteBoundary(baseLSNs, boundary, cut)
}

// boundaryIndex returns the index of the last segment whose base LSN is <= cut,
// i.e. the segment that either starts at or contains cut.
func boundaryIndex(baseLSNs []uint64, cut uint64) int {
	boundary := 0
	for i := range baseLSNs {
		if baseLSNs[i] > cut {
			break
		}

		boundary = i
	}

	return boundary
}

// cutDeleteBelow handles a cut that lands exactly on a segment base: delete every
// segment below the boundary, no rewrite needed.
func (w *WAL) cutDeleteBelow(baseLSNs []uint64, boundary int, cut uint64) error {
	for i := range boundary {
		if err := w.removeSegmentFile(baseLSNs[i]); err != nil {
			return err
		}

		w.opts.logger.Debug("wal: cut deleted segment", Field{Key: "baseLSN", Value: baseLSNs[i]})
	}

	w.mu.Lock()
	w.segmentBaseLSNs = w.segmentBaseLSNs[boundary:]
	w.firstLSN = cut
	w.mu.Unlock()

	w.logCut(cut, boundary, false)

	return nil
}

// cutRewriteBoundary rewrites the segment containing cut into seg-<cut>, then
// deletes every below-cut segment (ascending, the old boundary last, so an
// interrupted cut always leaves seg-<cut> overlapping the old boundary rather
// than a gap). If the boundary was the active segment, seg-<cut> becomes active.
func (w *WAL) cutRewriteBoundary(baseLSNs []uint64, boundary int, cut uint64) error {
	isActive := boundary == len(baseLSNs)-1

	source := w.active
	if !isActive {
		opened, err := segment.Open(w.root, baseLSNs[boundary])
		if err != nil {
			return err
		}
		defer func() { _ = opened.Close() }()

		source = opened
	}

	replacement, err := segment.RewriteFrom(w.root, source, cut, w.opts.maxRecordSize)
	if err != nil {
		return err
	}

	for i := 0; i <= boundary; i++ {
		if err := w.removeSegmentFile(baseLSNs[i]); err != nil {
			_ = replacement.Close()

			return err
		}
	}

	w.mu.Lock()
	oldActive := w.active
	w.segmentBaseLSNs = append([]uint64{cut}, w.segmentBaseLSNs[boundary+1:]...)
	w.firstLSN = cut
	if isActive {
		w.active = replacement
	}
	w.mu.Unlock()

	if isActive {
		err = oldActive.Close()
	} else {
		err = replacement.Close() // sealed replacement: readers reopen on demand
	}

	if err != nil {
		return err
	}

	w.logCut(cut, boundary, true)

	return nil
}

// cutEverything deletes every existing record and installs a fresh empty active
// segment based at nextLSN, so future appends stay monotonic and gapless. The new
// segment is created durably before any delete; an interrupted cut-everything
// reverts to the pre-cut log (its trailing empty segment is pruned on recovery).
func (w *WAL) cutEverything(baseLSNs []uint64, nextLSN uint64) error {
	// A leftover empty seg-<nextLSN> from a previously interrupted cut-everything
	// would block the exclusive create below. It holds no records (cut-everything
	// only ever creates it empty), so removing it first is safe and lets a retry
	// after a partial failure proceed instead of wedging on an already-exists error.
	if err := w.root.Remove(segment.Name(nextLSN)); err != nil && !os.IsNotExist(err) {
		return err
	}

	replacement, err := segment.Create(w.root, nextLSN)
	if err != nil {
		return err
	}

	for i := range baseLSNs {
		if err := w.removeSegmentFile(baseLSNs[i]); err != nil {
			_ = replacement.Close()

			return err
		}
	}

	w.mu.Lock()
	oldActive := w.active
	w.active = replacement
	w.segmentBaseLSNs = []uint64{nextLSN}
	w.firstLSN = 0
	w.mu.Unlock()

	if err := oldActive.Close(); err != nil {
		return err
	}

	w.opts.logger.Info("wal: cut entire log",
		Field{Key: "segmentsDeleted", Value: len(baseLSNs)},
		Field{Key: "newActiveBaseLSN", Value: nextLSN},
	)

	return nil
}

// logCut emits the completion log line shared by the delete-only and rewrite
// cut paths.
func (w *WAL) logCut(cut uint64, boundary int, boundaryRewritten bool) {
	w.opts.logger.Info("wal: cut log",
		Field{Key: "upTo", Value: cut},
		Field{Key: "firstLSN", Value: cut},
		Field{Key: "segmentsDeleted", Value: boundary},
		Field{Key: "boundaryRewritten", Value: boundaryRewritten},
	)
}
