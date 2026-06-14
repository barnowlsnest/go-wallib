package wal

import (
	"context"
	"slices"
	"time"
	
	"github.com/barnowlsnest/go-wal/internal/record"
	"github.com/barnowlsnest/go-wal/internal/segment"
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

// truncateRequest asks the writer goroutine to delete whole segments below upTo.
type truncateRequest struct {
	resultCh chan error
	upTo     uint64
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
		case <-w.closed:
			w.drain()
			w.finalFlush()
			
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
	}
	
	w.mu.Lock()
	defer w.mu.Unlock()
	w.segmentBaseLSNs = w.segmentBaseLSNs[deletable:]
	w.firstLSN = w.segmentBaseLSNs[0]
	
	return nil
}

// removeSegmentFile deletes one (already closed) segment file and fsyncs the
// directory so the removal is durable.
func (w *WAL) removeSegmentFile(baseLSN uint64) error {
	if err := w.root.Remove(segment.Name(baseLSN)); err != nil {
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
