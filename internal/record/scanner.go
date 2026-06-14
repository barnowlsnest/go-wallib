package record

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

// ErrTorn is returned by Scanner.Err when the stream ends mid-record,
// indicating an incomplete (torn) write at the tail of a WAL segment.
// The segment may be safely truncated to Scanner.Offset().
var ErrTorn = errors.New("wal/record: torn record at tail")

// ErrCorrupt is returned by Scanner.Err when a record's CRC32C checksum
// does not match the recorded header CRC, indicating on-disk data corruption.
var ErrCorrupt = errors.New("wal/record: corrupt record (crc mismatch)")

// ErrTooLarge is returned by Scanner.Err when the Length field in a record
// header exceeds the maximum configured for the Scanner.
var ErrTooLarge = errors.New("wal/record: record length exceeds maximum")

// Scanner reads framed WAL records sequentially from an io.Reader.
// The caller advances the stream with Next(); the current decoded record is
// available via Record(). Call Err() after Next() returns false to distinguish
// a clean end-of-stream from a torn write or data corruption.
//
// Payload returned by Record() is valid only until the next call to Next().
// Fields are ordered to minimize struct padding and GC-scanned pointer bytes:
// pointer-bearing fields (interfaces, slices, the Record's slice) are grouped
// first, followed by the scalar counters.
type Scanner struct {
	source   io.Reader
	firstErr error
	// payloadBuf is reused across calls to avoid per-record allocations.
	payloadBuf []byte
	// current holds the most-recently decoded record.
	current Record

	maxRecBytes int
	// validBytes is the sum of HeaderSize+Length for all fully-valid records seen
	// so far. It does NOT advance on error, so it always names the last good
	// truncation point in the segment.
	validBytes int64
}

// NewScanner returns a Scanner that reads from source and rejects any record
// whose declared payload length exceeds maxRecordBytes.
func NewScanner(source io.Reader, maxRecordBytes int) *Scanner {
	return &Scanner{source: source, maxRecBytes: maxRecordBytes}
}

// Next attempts to decode the next record from the stream. It returns true when
// a valid record is available via Record(). It returns false on clean EOF or on
// the first error (which is then accessible via Err()).
func (s *Scanner) Next() bool {
	if s.firstErr != nil {
		return false
	}

	var header [HeaderSize]byte
	if readErr := s.readHeader(header[:]); readErr != nil {
		if !errors.Is(readErr, io.EOF) {
			s.firstErr = readErr
		}
		return false
	}

	payloadLen := binary.LittleEndian.Uint32(header[4:8])
	if int(payloadLen) > s.maxRecBytes {
		s.firstErr = ErrTooLarge
		return false
	}

	lsn := binary.LittleEndian.Uint64(header[8:16])
	storedCRC := binary.LittleEndian.Uint32(header[0:4])

	payload, payloadErr := s.readPayload(int(payloadLen))
	if payloadErr != nil {
		s.firstErr = payloadErr
		return false
	}

	if !crcMatches(storedCRC, header[4:16], payload) {
		s.firstErr = ErrCorrupt
		return false
	}

	s.validBytes += int64(HeaderSize) + int64(payloadLen)
	s.current = Record{LSN: lsn, Payload: payload}
	return true
}

// Record returns the most recently decoded record. The Payload slice is only
// valid until the next call to Next().
func (s *Scanner) Record() Record { return s.current }

// Offset returns the total number of bytes consumed by fully-valid records.
// On a torn or corrupt stream this is the safe truncation point.
func (s *Scanner) Offset() int64 { return s.validBytes }

// Err returns the first non-EOF error encountered, or nil after a clean EOF.
func (s *Scanner) Err() error { return s.firstErr }

// readHeader fills header from the source, mapping a zero-byte EOF to a clean
// boundary (io.EOF) and a partial read to ErrTorn.
func (s *Scanner) readHeader(header []byte) error {
	_, err := io.ReadFull(s.source, header)
	if err != nil {
		switch {
		case errors.Is(err, io.EOF):
			// ReadFull returns plain io.EOF only when zero bytes were read,
			// meaning we are at a clean record boundary.
			return io.EOF
		case errors.Is(err, io.ErrUnexpectedEOF):
			return ErrTorn
		default:
			return err
		}
	}
	return nil
}

// readPayload reads exactly n bytes into the reused buffer, growing it when
// necessary, and returns the slice. A short read maps to ErrTorn.
func (s *Scanner) readPayload(n int) ([]byte, error) {
	if n == 0 {
		return nil, nil
	}
	if cap(s.payloadBuf) < n {
		s.payloadBuf = make([]byte, n)
	}
	s.payloadBuf = s.payloadBuf[:n]

	_, err := io.ReadFull(s.source, s.payloadBuf)
	if err != nil {
		switch {
		case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
			return nil, ErrTorn
		default:
			return nil, err
		}
	}
	return s.payloadBuf, nil
}

// crcMatches computes CRC32C over the header tail (Length||LSN) then payload and
// compares it to storedCRC.
func crcMatches(storedCRC uint32, headerTail, payload []byte) bool {
	crc := crc32.Update(0, crcTable, headerTail)
	crc = crc32.Update(crc, crcTable, payload)
	return crc == storedCRC
}
