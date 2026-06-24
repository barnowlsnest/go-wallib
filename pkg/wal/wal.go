package wal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"

	"github.com/barnowlsnest/go-wallib/internal/segment"
)

// openedSeg pairs an open segment with the result of scanning its records during
// recovery.
type openedSeg struct {
	seg *segment.Segment
	res segment.ScanResult
}

// WAL is a Write-Ahead Log. It is safe for concurrent use.
type WAL struct {
	closeErr        error
	active          *segment.Segment
	closed          chan struct{}
	requestCh       chan *appendRequest
	controlCh       chan chan error
	truncateCh      chan *truncateRequest
	subscribeCh     chan wakeup
	root            *os.Root
	dir             string
	segmentBaseLSNs []uint64
	pendingWaiters  []chan struct{}
	opts            options
	wg              sync.WaitGroup
	nextLSN         uint64
	firstLSN        uint64
	lastLSN         uint64
	closeMu         sync.RWMutex
	mu              sync.RWMutex
	closeOnce       sync.Once
	isClosed        bool
}

// Open recovers an existing log in dir or creates a new one. It returns a
// ready-to-use WAL and a RecoveryReport describing what was found and repaired.
func Open(dir string, opts ...Option) (w *WAL, report *RecoveryReport, err error) {
	resolved := defaultOptions()
	for _, apply := range opts {
		apply(&resolved)
	}

	w = &WAL{
		dir:         dir,
		opts:        resolved,
		closed:      make(chan struct{}),
		requestCh:   make(chan *appendRequest, resolved.batchSize*2),
		controlCh:   make(chan chan error), // unbuffered: a send means the writer took it
		truncateCh:  make(chan *truncateRequest),
		subscribeCh: make(chan wakeup),
	}

	report, err = w.recover()
	if err != nil {
		if w.root != nil {
			_ = w.root.Close()
		}

		return nil, nil, err
	}

	w.startWriter()
	if resolved.syncPolicy == SyncInterval {
		w.startFlusher()
	}

	return w, report, nil
}

// recover implements the spec §9 recovery algorithm and installs the writer
// state (active segment, segment index, LSN bounds).
func (w *WAL) recover() (*RecoveryReport, error) {
	root, err := os.OpenRoot(w.dir)
	if err != nil {
		return nil, err
	}

	w.root = root
	paths, err := segment.List(w.dir)
	if err != nil {
		return nil, err
	}

	if len(paths) == 0 {
		return w.initEmptyLog()
	}

	segments, err := w.openAndScanSegments(paths)
	if err != nil {
		return nil, err
	}

	report := &RecoveryReport{}
	if truncErr := truncateTornTail(report, segments); truncErr != nil {
		closeAll(segments)

		return nil, truncErr
	}

	firstLSN, lastLSN, total := computeBounds(segments)
	report.EntriesRecovered = total
	report.FirstLSN = firstLSN
	report.LastLSN = lastLSN

	segments, err = w.pruneEmptyTrailing(report, segments, lastLSN)
	if err != nil {
		return nil, err
	}

	if err := w.installState(segments, firstLSN, lastLSN); err != nil {
		return nil, err
	}

	return report, nil
}

// initEmptyLog creates the first segment of a brand-new log.
func (w *WAL) initEmptyLog() (*RecoveryReport, error) {
	seg, err := segment.Create(w.root, 1)
	if err != nil {
		return nil, err
	}

	w.active = seg
	w.segmentBaseLSNs = []uint64{1}
	w.nextLSN = 1

	return &RecoveryReport{}, nil
}

// openAndScanSegments opens and validates every segment in order, scanning each
// for records. Base LSNs must be contiguous, and only the final segment may end
// in a torn or corrupt record.
func (w *WAL) openAndScanSegments(paths []string) ([]openedSeg, error) {
	var segments []openedSeg
	var expectedBase uint64

	for i, path := range paths {
		base, ok := segment.ParseBaseLSN(filepath.Base(path))
		if !ok {
			closeAll(segments)

			return nil, ErrCorrupt
		}

		seg, err := segment.Open(w.root, base)
		if err != nil {
			closeAll(segments)

			return nil, joinCorrupt(err)
		}

		if i > 0 && seg.BaseLSN() != expectedBase {
			_ = seg.Close()
			closeAll(segments)

			return nil, ErrCorrupt
		}

		res := seg.Scan(w.opts.maxRecordSize)
		if res.Err != nil && i != len(paths)-1 {
			_ = seg.Close()
			closeAll(segments)

			return nil, ErrCorrupt
		}

		segments = append(segments, openedSeg{seg: seg, res: res})
		expectedBase = seg.BaseLSN() + res.Records
	}

	return segments, nil
}

// truncateTornTail chops an incomplete record off the final segment, recording
// how many bytes were removed.
func truncateTornTail(report *RecoveryReport, segments []openedSeg) error {
	last := &segments[len(segments)-1]
	if last.res.Err == nil {
		return nil
	}

	report.BytesTruncated = last.seg.Size() - last.res.ValidEnd

	return last.seg.TruncateTo(last.res.ValidEnd)
}

// computeBounds returns the first/last surviving LSN and the total record count.
func computeBounds(segments []openedSeg) (firstLSN, lastLSN, total uint64) {
	for _, opened := range segments {
		total += opened.res.Records
		if opened.res.Records == 0 {
			continue
		}

		if firstLSN == 0 {
			firstLSN = opened.seg.BaseLSN()
		}
		lastLSN = opened.res.LastLSN
	}

	return firstLSN, lastLSN, total
}

// pruneEmptyTrailing deletes empty trailing segments above the active one, and
// for an all-empty log collapses to a single empty active segment.
func (w *WAL) pruneEmptyTrailing(
	report *RecoveryReport, segments []openedSeg, lastLSN uint64,
) ([]openedSeg, error) {
	activeIdx := len(segments) - 1
	for i := len(segments) - 1; i >= 0; i-- {
		if segments[i].res.Records > 0 {
			activeIdx = i

			break
		}
	}

	for i := len(segments) - 1; i > activeIdx; i-- {
		if err := w.removeOpened(report, segments[i]); err != nil {
			return nil, err
		}
	}
	segments = segments[:activeIdx+1]

	if lastLSN == 0 {
		for i := 1; i < len(segments); i++ {
			if err := w.removeOpened(report, segments[i]); err != nil {
				return nil, err
			}
		}

		segments = segments[:1]
	}

	return segments, nil
}

// removeOpened closes and deletes one opened segment, then fsyncs the directory.
func (w *WAL) removeOpened(report *RecoveryReport, opened openedSeg) error {
	name := opened.seg.Name()
	if err := opened.seg.Close(); err != nil {
		return err
	}

	if err := w.root.Remove(name); err != nil {
		return err
	}

	report.SegmentsRemoved++

	return segment.SyncDir(w.dir)
}

// installState records the recovered segment index and bounds, keeping only the
// active (last) segment open; readers reopen the rest on demand.
func (w *WAL) installState(segments []openedSeg, firstLSN, lastLSN uint64) error {
	lastIdx := len(segments) - 1
	for i := 0; i < lastIdx; i++ {
		if err := segments[i].seg.Close(); err != nil {
			return err
		}
	}

	w.segmentBaseLSNs = make([]uint64, len(segments))
	for i, opened := range segments {
		w.segmentBaseLSNs[i] = opened.seg.BaseLSN()
	}

	w.active = segments[lastIdx].seg
	w.firstLSN = firstLSN
	w.lastLSN = lastLSN
	w.nextLSN = lastLSN + 1

	return nil
}

// closeAll closes every opened segment, used to unwind on a recovery error.
func closeAll(segments []openedSeg) {
	for _, opened := range segments {
		_ = opened.seg.Close()
	}
}

// joinCorrupt maps the segment layer's structural errors onto wal.ErrCorrupt.
func joinCorrupt(err error) error {
	if errors.Is(err, segment.ErrBadMagic) ||
		errors.Is(err, segment.ErrUnknownVersion) ||
		errors.Is(err, segment.ErrCorrupt) {
		return ErrCorrupt
	}

	return err
}

// FirstLSN returns the lowest available LSN, or 0 if the log is empty.
func (w *WAL) FirstLSN() uint64 {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.firstLSN
}

// LastLSN returns the highest committed LSN, or 0 if the log is empty.
func (w *WAL) LastLSN() uint64 {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.lastLSN
}

// Append durably writes payload as a single record and returns its LSN. It
// blocks until the configured SyncPolicy's durability guarantee is met.
//
// Delivery is at-least-once: if Append returns (lsn, nil) the record is durable;
// if it returns an error, or the process dies before it returns, the record may
// or may not be durable, and a retry may create a duplicate with a NEW LSN.
// Deduplicate using a caller key embedded in the payload, not the LSN.
func (w *WAL) Append(ctx context.Context, payload []byte) (uint64, error) {
	assignedLSNs, err := w.AppendBatch(ctx, [][]byte{payload})
	if err != nil {
		return 0, err
	}

	return assignedLSNs[0], nil
}

// Sync forces a flush and fsync of all pending data, blocking until the writer
// goroutine has completed it. It returns ErrClosed if the WAL is closed.
func (w *WAL) Sync() error {
	w.closeMu.RLock()
	closed := w.isClosed
	w.closeMu.RUnlock()

	if closed {
		return ErrClosed
	}

	done := make(chan error, 1)
	select {
	case w.controlCh <- done:
		return <-done
	case <-w.closed:
		return ErrClosed
	}
}

// Truncate advances the low-water mark, deleting whole segments whose entries
// are all below upToLSN. The active segment is never deleted, so entries below
// upToLSN that share the active (or any surviving) segment remain readable;
// truncation is best-effort reclamation, not a precise per-entry delete.
func (w *WAL) Truncate(upToLSN uint64) error {
	w.closeMu.RLock()
	closed := w.isClosed
	w.closeMu.RUnlock()

	if closed {
		return ErrClosed
	}

	done := make(chan error, 1)
	select {
	case w.truncateCh <- &truncateRequest{resultCh: done, upTo: upToLSN}:
		return <-done
	case <-w.closed:
		return ErrClosed
	}
}

// segmentCount reports how many segments make up the log. It is a test helper.
func (w *WAL) segmentCount() int {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return len(w.segmentBaseLSNs)
}

// Close flushes the active segment, stops background goroutines, and releases
// resources. It is idempotent.
func (w *WAL) Close() error {
	w.closeOnce.Do(func() {
		w.closeMu.Lock()
		w.isClosed = true
		w.closeMu.Unlock()

		close(w.closed)
		w.wg.Wait()

		w.mu.Lock()
		defer w.mu.Unlock()

		if w.active != nil {
			w.closeErr = errors.Join(w.closeErr, w.active.Close())
			w.active = nil
		}

		if w.root != nil {
			w.closeErr = errors.Join(w.closeErr, w.root.Close())
			w.root = nil
		}
	})

	return w.closeErr
}
