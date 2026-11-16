package wal

import (
	"bufio"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/barnowlsnest/go-wallib/internal/record"
	"github.com/barnowlsnest/go-wallib/internal/segment"
)

// Reader is a forward cursor over committed log entries. It is not safe for
// concurrent use by multiple goroutines.
type Reader struct {
	err          error
	offered      *os.File
	file         *os.File
	scanner      *record.Scanner
	retained     *os.File
	retainedName string
	dir          string
	offeredName  string
	position     readerPosition
	segmentNames []string
	current      Entry
	fromLSN      uint64
	scanBase     int64
	startOffset  int64
	maxRecord    int
	endLSN       uint64
	index        int
	retain       bool
	done         bool
}

// readerPosition names a segment and the byte offset just past the last record
// a Reader consumed in it, so a later Reader can continue from there instead of
// scanning the segment again from its start.
type readerPosition struct {
	segmentName string
	offset      int64
}

// readerSnapshot captures the segment file names, the last committed LSN, and the
// first available LSN under the read lock, for constructing a point-in-time reader.
func (w *WAL) readerSnapshot() (segmentNames []string, endLSN, firstLSN uint64) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.snapshotLocked()
}

// snapshotLocked is readerSnapshot for a caller holding mu.
func (w *WAL) snapshotLocked() (segmentNames []string, endLSN, firstLSN uint64) {
	segmentNames = make([]string, len(w.segmentBaseLSNs))
	for i, baseLSN := range w.segmentBaseLSNs {
		segmentNames[i] = segment.Name(baseLSN)
	}

	return segmentNames, w.lastLSN, w.firstLSN
}

// newReaderFrom builds a Reader over a captured snapshot, yielding entries with
// LSN in [fromLSN, endLSN]. It is the shared core of NewReader and the Follower.
func newReaderFrom(dir string, segmentNames []string, fromLSN, endLSN uint64, maxRecord int) *Reader {
	return &Reader{
		dir:          dir,
		segmentNames: segmentNames,
		index:        startIndex(segmentNames, fromLSN),
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
	segmentNames, endLSN, _ := w.snapshotLocked()
	start := w.indexedPositionLocked(fromLSN)
	w.mu.RUnlock()

	reader := newReaderFrom(w.dir, segmentNames, fromLSN, endLSN, w.opts.maxRecordSize)
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

	return readerPosition{segmentName: segment.Name(baseLSN), offset: offset}
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

		if r.index >= len(r.segmentNames) {
			r.end()

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
		r.end()

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
	for r.index < len(r.segmentNames) {
		name := r.segmentNames[r.index]
		r.index++

		file, err := r.openSegment(name)
		if err != nil {
			if os.IsNotExist(err) {
				if err := r.relistAfter(name); err != nil {
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

		if _, err := file.Seek(offset, io.SeekStart); err != nil {
			_ = file.Close()
			r.fail(err)

			return false
		}

		r.file = file
		r.scanner = record.NewScanner(bufio.NewReader(file), r.maxRecord)
		r.scanBase = offset
		r.position = readerPosition{segmentName: name, offset: offset}

		return true
	}

	r.finish()

	return false
}

// openSegment returns the offered file when it is for name, and otherwise
// opens the segment.
func (r *Reader) openSegment(name string) (*os.File, error) {
	if r.offered != nil && name == r.offeredName {
		file := r.offered
		r.offered = nil

		return file, nil
	}

	return os.OpenInRoot(r.dir, name)
}

// offer gives the reader an open file of segment name to read through instead
// of opening that segment again. The reader seeks it to where it starts, so the
// file's current offset does not matter. The reader owns the file from here on:
// it closes it like any file it opened, or on Close if it never reads that
// segment.
func (r *Reader) offer(file *os.File, name string) {
	r.offered, r.offeredName = file, name
}

// takeRetained returns the open file of the segment a retaining reader ended
// in, and its name, passing ownership to the caller. It is nil when the reader
// was not asked to retain or stopped on an error or before its end.
func (r *Reader) takeRetained() (file *os.File, name string) {
	file, name = r.retained, r.retainedName
	r.retained, r.retainedName = nil, ""

	return file, name
}

// end stops the cursor at the end of its snapshot. A retaining reader keeps
// the current segment's file for takeRetained instead of closing it.
func (r *Reader) end() {
	if r.retain && r.file != nil {
		r.retained, r.retainedName = r.file, r.position.segmentName
		r.file = nil
	}

	r.finish()
}

// relistAfter replaces the remaining snapshot with every segment now on disk
// whose base LSN is above the missing one. A cut creates seg-<cut> durably before
// deleting the segment it replaces, so its records are always found here.
func (r *Reader) relistAfter(missing string) error {
	missingBase, _ := segment.ParseBaseLSN(missing)

	paths, err := segment.List(r.dir)
	if err != nil {
		return err
	}

	remaining := make([]string, 0, len(paths))
	for _, path := range paths {
		name := filepath.Base(path)
		if base, _ := segment.ParseBaseLSN(name); base > missingBase {
			remaining = append(remaining, name)
		}
	}

	r.segmentNames = remaining
	r.index = startIndex(remaining, r.fromLSN)
	r.startOffset = 0

	return nil
}

// startIndex returns the index of the last segment whose base LSN is <= fromLSN,
// or 0 when there is none. Segment names are sorted by base LSN and a segment
// holds no LSN at or above the next segment's base (a cut's overlap copy lives in
// that next segment too), so every segment before this index can be skipped
// without reading it.
func startIndex(segmentNames []string, fromLSN uint64) int {
	above := sort.Search(len(segmentNames), func(i int) bool {
		base, _ := segment.ParseBaseLSN(segmentNames[i])

		return base > fromLSN
	})

	return max(above-1, 0)
}

// resumeAt continues from a position an earlier Reader left, when that segment
// is the one this Reader would start in anyway. A segment file is only ever
// appended to under its name (a cut writes its records into a new seg-<cut> and
// deletes the old name), so the offset still lands on a record boundary. In every
// other case, including a segment the snapshot no longer names, the start chosen
// from fromLSN is kept.
func (r *Reader) resumeAt(position readerPosition) {
	if r.done || position.segmentName == "" || r.index >= len(r.segmentNames) {
		return
	}

	if r.segmentNames[r.index] == position.segmentName {
		r.startOffset = position.offset
	}
}

// lastPosition reports where this Reader stopped: the segment it last read and
// the offset just past the last record it consumed within its snapshot. It is
// empty if the Reader never opened a segment.
func (r *Reader) lastPosition() readerPosition { return r.position }

// closeCurrent closes the open segment so the next one can be read.
func (r *Reader) closeCurrent() {
	if r.file != nil {
		_ = r.file.Close()
		r.file = nil
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

// Close releases the reader's resources, including an offered file it never
// read. A retained file is left for takeRetained. It is safe to call more than
// once.
func (r *Reader) Close() error {
	r.finish()

	if r.offered != nil {
		_ = r.offered.Close()
		r.offered, r.offeredName = nil, ""
	}

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
