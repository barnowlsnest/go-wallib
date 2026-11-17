package wal

import (
	"bufio"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/barnowlsnest/go-wallib/internal/record"
	"github.com/barnowlsnest/go-wallib/internal/segment"
)

// Reader is a forward cursor over committed log entries. It is not safe for
// concurrent use by multiple goroutines.
type Reader struct {
	err          error
	handle       *readHandle
	buffered     *bufio.Reader
	scanner      *record.Scanner
	handles      *readHandles
	segmentBases []uint64
	current      Entry
	section      io.SectionReader
	position     readerPosition
	index        int
	fromLSN      uint64
	endLSN       uint64
	maxRecord    int
	startOffset  int64
	scanBase     int64
	done         bool
}

// readBufferSize matches segment.IndexInterval, so the records between the
// index entry a Reader starts from and the one it wants usually arrive in one
// or two reads instead of the sixteen a 4 KiB buffer needs.
const readBufferSize = segment.IndexInterval

// readBuffers holds read buffers between Readers. A scattered read of older data
// otherwise allocates a fresh buffer per segment it enters. Pooling is safe
// because the scanner copies each payload into its own buffer, so no Entry
// points into a read buffer.
var readBuffers = sync.Pool{
	New: func() any { return bufio.NewReaderSize(nil, readBufferSize) },
}

// readerPosition names a segment by base LSN and the byte offset just past the
// last record a Reader consumed in it, so a later Reader can continue from there
// instead of scanning the segment again from its start. The zero value names no
// position; a set one is never below the segment header.
type readerPosition struct {
	baseLSN uint64
	offset  int64
}

// isSet reports whether the position names a segment.
func (p readerPosition) isSet() bool { return p.offset > 0 }

// readerSnapshot captures the segment base LSNs, the last committed LSN, and the
// first available LSN under the read lock, for constructing a point-in-time reader.
func (w *WAL) readerSnapshot() (segmentBases []uint64, endLSN, firstLSN uint64) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.snapshotLocked()
}

// snapshotLocked is readerSnapshot for a caller holding mu. The base LSNs are
// shared with the WAL instead of copied, which would cost every new Reader an
// allocation and a copy that grow with the number of segments. That is safe
// because the writer never overwrites an element of segmentBaseLSNs (see the
// field), and capping the capacity keeps the Reader from appending into it.
func (w *WAL) snapshotLocked() (segmentBases []uint64, endLSN, firstLSN uint64) {
	count := len(w.segmentBaseLSNs)

	return w.segmentBaseLSNs[:count:count], w.lastLSN, w.firstLSN
}

// newReaderFrom builds a Reader over a captured snapshot, yielding entries with
// LSN in [fromLSN, endLSN]. It is the shared core of NewReader and the Follower.
func newReaderFrom(handles *readHandles, segmentBases []uint64, fromLSN, endLSN uint64, maxRecord int) *Reader {
	return &Reader{
		handles:      handles,
		segmentBases: segmentBases,
		index:        startIndex(segmentBases, fromLSN),
		fromLSN:      fromLSN,
		endLSN:       endLSN,
		maxRecord:    maxRecord,
		done:         endLSN == 0 || fromLSN > endLSN,
	}
}

// NewReader returns a cursor yielding entries with LSN >= fromLSN (0 means from
// the beginning), up to the last committed LSN at the time of this call. Entries
// committed after this call are not observed.
func (w *WAL) NewReader(fromLSN uint64) (*Reader, error) {
	if err := w.checkOpen(); err != nil {
		return nil, err
	}

	w.mu.RLock()
	segmentBases, endLSN, _ := w.snapshotLocked()
	start := w.indexedPositionLocked(fromLSN)
	w.mu.RUnlock()

	reader := newReaderFrom(w.handles, segmentBases, fromLSN, endLSN, w.opts.maxRecordSize)
	reader.resumeAt(start)

	return reader, nil
}

// indexedPosition returns where a reader from fromLSN can start: the indexed
// record nearest at or below fromLSN in the segment holding it. It is empty when
// no entry is at or below fromLSN, so reading starts at the segment header.
func (w *WAL) indexedPosition(fromLSN uint64) readerPosition {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.indexedPositionLocked(fromLSN)
}

// indexedPositionLocked is indexedPosition for a caller holding mu.
func (w *WAL) indexedPositionLocked(fromLSN uint64) readerPosition {
	above := sort.Search(len(w.segmentBaseLSNs), func(i int) bool { return w.segmentBaseLSNs[i] > fromLSN })
	if above == 0 {
		return readerPosition{}
	}

	baseLSN := w.segmentBaseLSNs[above-1]
	offset, ok := segment.Lookup(w.indexes[baseLSN], fromLSN)
	if !ok {
		return readerPosition{}
	}

	return readerPosition{baseLSN: baseLSN, offset: offset}
}

// Next advances to the next entry. It returns false at the end of the snapshot
// or on the first error; inspect Err to distinguish.
func (r *Reader) Next() bool {
	for !r.done {
		if r.scanner == nil && !r.openNext() {
			return false
		}

		if r.scanner.Next() {
			scanned := r.scanner.Record()
			if scanned.LSN <= r.endLSN {
				r.position.offset = r.scanBase + r.scanner.Offset()
			}

			if entry, ok := r.accept(scanned); ok {
				r.current = entry

				return true
			}

			continue
		}

		if err := r.scanner.Err(); err != nil && !errors.Is(err, record.ErrTorn) {
			r.fail(ErrCorrupt)

			return false
		}

		r.closeCurrent()
	}

	return false
}

// accept decides whether a scanned record falls within the reader's window. A
// record above endLSN ends the cursor (it is beyond our snapshot). The window's
// lower bound advances past every accepted record, so records a concurrent cut
// briefly duplicates across seg-<cut> and the old boundary are yielded once.
func (r *Reader) accept(scanned record.Record) (Entry, bool) {
	switch {
	case scanned.LSN < r.fromLSN:
		return Entry{}, false
	case scanned.LSN > r.endLSN:
		r.finish()

		return Entry{}, false
	default:
		r.fromLSN = scanned.LSN + 1

		return Entry{LSN: scanned.LSN, Payload: scanned.Payload}, true
	}
}

// openNext opens the next segment. A segment deleted since the snapshot was
// either reclaimed by a concurrent Truncate or rewritten by a concurrent
// CutOffset into a seg-<cut> the snapshot does not name, so the remaining
// segments are re-listed from the directory rather than trusted.
func (r *Reader) openNext() bool {
	for r.index < len(r.segmentBases) {
		baseLSN := r.segmentBases[r.index]
		r.index++

		handle, err := r.handles.acquire(baseLSN)
		if err != nil {
			if os.IsNotExist(err) {
				if err := r.relistAfter(baseLSN); err != nil {
					r.fail(err)

					return false
				}

				continue
			}

			r.fail(err)

			return false
		}

		offset := int64(segment.HeaderSize)
		if r.startOffset > 0 {
			offset, r.startOffset = r.startOffset, 0
		}

		r.handle = handle
		r.section = *io.NewSectionReader(handle.file, offset, math.MaxInt64-offset)
		r.buffered, _ = readBuffers.Get().(*bufio.Reader)
		r.buffered.Reset(&r.section)
		r.scanner = record.NewScanner(r.buffered, r.maxRecord)
		r.scanBase = offset
		r.position = readerPosition{baseLSN: baseLSN, offset: offset}

		return true
	}

	r.finish()

	return false
}

// relistAfter replaces the remaining snapshot with every segment now on disk
// whose base LSN is above the missing one. A cut creates seg-<cut> durably before
// deleting the segment it replaces, so its records are always found here.
func (r *Reader) relistAfter(missingBase uint64) error {
	paths, err := segment.List(r.handles.dir)
	if err != nil {
		return err
	}

	remaining := make([]uint64, 0, len(paths))
	for _, path := range paths {
		if base, _ := segment.ParseBaseLSN(filepath.Base(path)); base > missingBase {
			remaining = append(remaining, base)
		}
	}

	r.segmentBases = remaining
	r.index = startIndex(remaining, r.fromLSN)
	r.startOffset = 0

	return nil
}

// startIndex returns the index of the last segment whose base LSN is <= fromLSN,
// or 0 when there is none. Segment names are sorted by base LSN and a segment
// holds no LSN at or above the next segment's base (a cut's overlap copy lives in
// that next segment too), so every segment before this index can be skipped
// without reading it.
func startIndex(segmentBases []uint64, fromLSN uint64) int {
	above := sort.Search(len(segmentBases), func(i int) bool { return segmentBases[i] > fromLSN })

	return max(above-1, 0)
}

// resumeAt continues from a position an earlier Reader left, when that segment
// is the one this Reader would start in anyway. A segment file is only ever
// appended to under its name (a cut writes its records into a new seg-<cut> and
// deletes the old name), so the offset still lands on a record boundary. In every
// other case, including a segment the snapshot no longer names, the start chosen
// from fromLSN is kept.
func (r *Reader) resumeAt(position readerPosition) {
	if r.done || !position.isSet() || r.index >= len(r.segmentBases) {
		return
	}

	if r.segmentBases[r.index] == position.baseLSN {
		r.startOffset = position.offset
	}
}

// lastPosition reports where this Reader stopped: the segment it last read and
// the offset just past the last record it consumed within its snapshot. It is
// empty if the Reader never opened a segment.
func (r *Reader) lastPosition() readerPosition { return r.position }

// closeCurrent releases the open segment so the next one can be read.
func (r *Reader) closeCurrent() {
	if r.handle != nil {
		r.handles.release(r.handle)
		r.handle = nil
	}

	if r.buffered != nil {
		// Drop the reference to the section so a pooled buffer does not keep
		// this Reader alive.
		r.buffered.Reset(nil)
		readBuffers.Put(r.buffered)
		r.buffered = nil
	}

	r.scanner = nil
}

// fail records the first error and stops the cursor.
func (r *Reader) fail(err error) {
	r.err = err
	r.finish()
}

// finish closes the current segment and marks the cursor exhausted.
func (r *Reader) finish() {
	r.closeCurrent()
	r.done = true
}

// Entry returns the current entry. It is valid after Next returned true and
// until the next call to Next.
func (r *Reader) Entry() Entry { return r.current }

// Err returns the first error encountered, or nil at a clean end of the cursor.
func (r *Reader) Err() error { return r.err }

// Close releases the reader's resources. It is safe to call more than once.
func (r *Reader) Close() error {
	r.finish()

	return nil
}

// Replay invokes fn for each entry with LSN >= fromLSN, in order, stopping on
// and returning the first error fn returns.
func (w *WAL) Replay(fromLSN uint64, fn func(Entry) error) error {
	reader, err := w.NewReader(fromLSN)
	if err != nil {
		return err
	}
	defer func() { _ = reader.Close() }()

	for reader.Next() {
		if err := fn(reader.Entry()); err != nil {
			return err
		}
	}

	return reader.Err()
}
