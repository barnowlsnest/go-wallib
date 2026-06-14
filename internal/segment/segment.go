package segment

import (
	"bufio"
	"io"
	"os"
	"time"

	"github.com/barnowlsnest/go-wal/internal/record"
)

// Segment is an open segment file: a validated header followed by framed
// records. It is not safe for concurrent use — the WAL writer goroutine owns the
// single writable Segment. All file access is confined to the WAL directory via
// the *os.Root the WAL holds.
type Segment struct {
	file    *os.File
	name    string
	baseLSN uint64
	size    int64  // total bytes written, including the header
	lastLSN uint64 // highest record LSN; baseLSN-1 when the segment is empty
}

// ScanResult reports the outcome of scanning a segment's records.
type ScanResult struct {
	// Err is nil at a clean end of records, or record.ErrTorn/ErrCorrupt/
	// ErrTooLarge when scanning stopped early.
	Err      error
	Records  uint64 // number of valid records read
	LastLSN  uint64 // highest valid record LSN (0 if none)
	ValidEnd int64  // byte offset just past the last valid record
}

// Create makes a new segment file named for baseLSN inside root, writes and
// fsyncs its header, then fsyncs the directory so the new file is durable. It
// fails if a segment with that base LSN already exists.
func Create(root *os.Root, baseLSN uint64) (*Segment, error) {
	name := Name(baseLSN)

	file, err := root.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, err
	}
	// Close the file unless we hand ownership to the caller; on success the
	// returned *Segment keeps it open.
	committed := false
	defer func() {
		if !committed {
			_ = file.Close()
		}
	}()

	header := EncodeHeader(Header{
		Version:   Version,
		BaseLSN:   baseLSN,
		CreatedAt: nowUnixNanos(),
	})
	if _, err := file.WriteAt(header, 0); err != nil {
		return nil, err
	}

	if err := file.Sync(); err != nil {
		return nil, err
	}

	if err := fsyncDir(root); err != nil {
		return nil, err
	}

	committed = true

	return &Segment{
		file:    file,
		name:    name,
		baseLSN: baseLSN,
		size:    HeaderSize,
		lastLSN: baseLSN - 1,
	}, nil
}

// Open opens the existing segment named for baseLSN inside root for read/write,
// validates its header, and verifies the header's base LSN matches the filename
// so a renamed or mislabeled file cannot masquerade as another segment.
func Open(root *os.Root, baseLSN uint64) (*Segment, error) {
	name := Name(baseLSN)

	file, err := root.OpenFile(name, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	// Close the file unless we hand ownership to the caller; on success the
	// returned *Segment owns it.
	committed := false
	defer func() {
		if !committed {
			_ = file.Close()
		}
	}()

	var headerBuf [HeaderSize]byte
	if _, err := io.ReadFull(file, headerBuf[:]); err != nil {
		return nil, ErrCorrupt
	}

	header, err := DecodeHeader(headerBuf[:])
	if err != nil {
		return nil, err
	}

	if header.BaseLSN != baseLSN {
		return nil, ErrCorrupt
	}

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	committed = true

	return &Segment{
		file:    file,
		name:    name,
		baseLSN: baseLSN,
		size:    info.Size(),
		lastLSN: baseLSN - 1,
	}, nil
}

// Name returns the segment's filename (relative to the WAL directory root).
func (s *Segment) Name() string { return s.name }

// BaseLSN returns the LSN of the first record this segment may contain.
func (s *Segment) BaseLSN() uint64 { return s.baseLSN }

// Size returns the total number of bytes in the segment, including the header.
func (s *Segment) Size() int64 { return s.size }

// LastLSN returns the highest record LSN written so far, or baseLSN-1 when empty.
func (s *Segment) LastLSN() uint64 { return s.lastLSN }

// Append encodes and writes a single record at the end of the segment. It does
// not fsync; call Sync to flush.
func (s *Segment) Append(lsn uint64, payload []byte) (int, error) {
	encoded, err := record.Encode(nil, lsn, payload)
	if err != nil {
		return 0, err
	}

	if _, err := s.file.WriteAt(encoded, s.size); err != nil {
		return 0, err
	}

	s.size += int64(len(encoded))
	s.lastLSN = lsn

	return len(encoded), nil
}

// Sync fsyncs the segment file.
func (s *Segment) Sync() error { return s.file.Sync() }

// Scan reads every record after the header, validating each. On a torn or
// corrupt record it stops early; ValidEnd marks the safe truncation point for
// tail recovery.
func (s *Segment) Scan(maxRecordSize int) ScanResult {
	section := io.NewSectionReader(s.file, HeaderSize, s.size-HeaderSize)
	scanner := record.NewScanner(bufio.NewReader(section), maxRecordSize)

	var result ScanResult
	for scanner.Next() {
		result.Records++
		result.LastLSN = scanner.Record().LSN
	}

	result.ValidEnd = HeaderSize + scanner.Offset()
	result.Err = scanner.Err()
	if result.Records > 0 {
		s.lastLSN = result.LastLSN
	}

	return result
}

// TruncateTo truncates the segment to offset bytes, fsyncs it, and updates the
// tracked size. It is used to chop a torn tail during recovery.
func (s *Segment) TruncateTo(offset int64) error {
	if err := s.file.Truncate(offset); err != nil {
		return err
	}

	if err := s.file.Sync(); err != nil {
		return err
	}

	s.size = offset

	return nil
}

// Close closes the underlying file.
func (s *Segment) Close() error { return s.file.Close() }

// fsyncDir fsyncs the WAL directory through root so that prior create/remove
// operations on segment files become durable.
func fsyncDir(root *os.Root) error {
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()

	return dir.Sync()
}

// nowUnixNanos returns the current time as unsigned Unix nanoseconds. The guard
// keeps the int64->uint64 conversion provably non-negative (and thus lossless),
// which matters only for the impossible pre-1970 clock case.
func nowUnixNanos() uint64 {
	nanos := time.Now().UnixNano()
	if nanos < 0 {
		nanos = 0
	}

	return uint64(nanos)
}
