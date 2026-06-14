package wal

import (
	"context"

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
// appends and, on Close, drains any queued requests before a final flush.
func (w *WAL) writerLoop() {
	for {
		select {
		case request := <-w.requestCh:
			w.commit(request)
		case <-w.closed:
			w.drain()
			w.finalFlush()

			return
		}
	}
}

// drain processes any appends that were handed off before Close was observed.
//
// This is not a busy loop: Close sets isClosed under closeMu before signaling
// w.closed, and enqueue holds closeMu.RLock across its isClosed check and its
// send. So once shutdown is observed no new request can enter requestCh; drain
// commits the bounded set already buffered and then the default case returns.
func (w *WAL) drain() {
	for {
		select {
		case request := <-w.requestCh:
			w.commit(request)
		default:
			return
		}
	}
}

// commit writes one append request, fsyncs per policy, and delivers the result.
// It runs only on the writer goroutine.
func (w *WAL) commit(request *appendRequest) {
	if err := request.ctx.Err(); err != nil {
		request.resultCh <- appendResult{err: err}

		return
	}

	assignedLSNs := make([]uint64, 0, len(request.payloads))
	for _, payload := range request.payloads {
		lsn := w.nextLSN
		if err := w.writeRecord(lsn, payload); err != nil {
			request.resultCh <- appendResult{err: err}

			return
		}

		w.nextLSN++
		assignedLSNs = append(assignedLSNs, lsn)
	}

	if w.opts.syncPolicy != SyncInterval {
		if err := w.active.Sync(); err != nil {
			request.resultCh <- appendResult{err: err}

			return
		}
	}

	w.publishLastLSN()
	request.resultCh <- appendResult{assignedLSNs: assignedLSNs}
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

// finalFlush fsyncs the active segment during shutdown, logging any failure.
func (w *WAL) finalFlush() {
	if w.active == nil {
		return
	}

	if err := w.active.Sync(); err != nil {
		w.opts.logger.Error("wal: final flush failed", Field{Key: "error", Value: err.Error()})
	}
}

// startFlusher launches the periodic background fsync goroutine for
// SyncInterval. It is expanded in Task 11.
func (w *WAL) startFlusher() {}
