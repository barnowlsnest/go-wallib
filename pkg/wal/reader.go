package wal

import (
	"bufio"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/barnowlsnest/go-wallib/internal/record"
	"github.com/barnowlsnest/go-wallib/internal/segment"
)

// Reader is a forward cursor over committed log entries. It is not safe for
// concurrent use by multiple goroutines.
type Reader struct {
	dir          string
	file         *os.File
	scanner      *record.Scanner
	err          error
	segmentNames []string
	current      Entry
	index        int
	fromLSN      uint64
	endLSN       uint64
	maxRecord    int
	done         bool
}

// readerSnapshot captures the segment file names, the last committed LSN, and the
// first available LSN under the read lock, for constructing a point-in-time reader.
func (w *WAL) readerSnapshot() (segmentNames []string, endLSN, firstLSN uint64) {
	w.mu.RLock()
	defer w.mu.RUnlock()

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
	w.closeMu.RLock()
	closed := w.isClosed
	w.closeMu.RUnlock()

	if closed {
		return nil, ErrClosed
	}

	segmentNames, endLSN, _ := w.readerSnapshot()

	return newReaderFrom(w.dir, segmentNames, fromLSN, endLSN, w.opts.maxRecordSize), nil
}

// Next advances to the next entry. It returns false at the end of the snapshot
// or on the first error; inspect Err to distinguish.
func (r *Reader) Next() bool {
	for !r.done {
		if r.scanner == nil && !r.openNext() {
			return false
		}

		if r.scanner.Next() {
			if entry, ok := r.accept(r.scanner.Record()); ok {
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
	for r.index < len(r.segmentNames) {
		name := r.segmentNames[r.index]
		r.index++

		file, err := os.OpenInRoot(r.dir, name)
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

		if _, err := file.Seek(segment.HeaderSize, io.SeekStart); err != nil {
			_ = file.Close()
			r.fail(err)

			return false
		}

		r.file = file
		r.scanner = record.NewScanner(bufio.NewReader(file), r.maxRecord)

		return true
	}

	r.finish()

	return false
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
	r.index = 0

	return nil
}

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
