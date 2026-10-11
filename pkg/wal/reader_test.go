package wal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/barnowlsnest/go-wallib/internal/segment"
)

// ReaderSuite covers the forward Reader cursor and Replay.
type ReaderSuite struct {
	suite.Suite
	dir string
}

func TestReaderSuite(t *testing.T) {
	suite.Run(t, new(ReaderSuite))
}

func (s *ReaderSuite) SetupTest() {
	s.dir = s.T().TempDir()
}

// payloadForLSN is the deterministic payload written for a given LSN, so the
// reader can verify content as well as ordering.
func payloadForLSN(lsn uint64) []byte {
	return fmt.Appendf(nil, `{"op":"set","key":"row:%d","seq":%d}`, lsn, lsn)
}

// appendSequential writes count records and returns the WAL.
func (s *ReaderSuite) appendSequential(count int, opts ...Option) *WAL {
	s.T().Helper()

	w, _, err := Open(s.dir, append([]Option{WithSyncPolicy(SyncImmediate)}, opts...)...)
	s.Require().NoError(err)

	for i := 1; i <= count; i++ {
		_, appendErr := w.Append(context.Background(), payloadForLSN(uint64(i)))
		s.Require().NoError(appendErr)
	}

	return w
}

func (s *ReaderSuite) collect(reader *Reader) []Entry {
	s.T().Helper()

	var entries []Entry
	for reader.Next() {
		entry := reader.Entry()
		entries = append(entries, Entry{LSN: entry.LSN, Payload: append([]byte(nil), entry.Payload...)})
	}
	s.Require().NoError(reader.Err())

	return entries
}

func (s *ReaderSuite) TestReadFromBeginningAcrossSegments() {
	// Small segments so the 10 records span several files the reader must cross.
	w := s.appendSequential(10, WithMaxSegmentSize(96))
	defer func() { s.Require().NoError(w.Close()) }()
	s.Require().GreaterOrEqual(w.segmentCount(), 2)

	reader, err := w.NewReader(0)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(reader.Close()) }()

	entries := s.collect(reader)
	s.Require().Len(entries, 10)
	for i, entry := range entries {
		wantLSN := uint64(i + 1)
		s.Require().Equal(wantLSN, entry.LSN)
		s.Require().Equal(payloadForLSN(wantLSN), entry.Payload, "payload for LSN %d", wantLSN)
	}
}

func (s *ReaderSuite) TestReadFromMidpoint() {
	w := s.appendSequential(5)
	defer func() { s.Require().NoError(w.Close()) }()

	reader, err := w.NewReader(3)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(reader.Close()) }()

	entries := s.collect(reader)
	s.Require().Len(entries, 3)
	s.Require().Equal(uint64(3), entries[0].LSN)
	s.Require().Equal(uint64(5), entries[2].LSN)
}

func (s *ReaderSuite) TestReadEmptyLog() {
	w, _, err := Open(s.dir)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(w.Close()) }()

	reader, err := w.NewReader(0)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(reader.Close()) }()

	s.Require().Empty(s.collect(reader))
}

func (s *ReaderSuite) TestNewReaderAfterCloseFails() {
	w, _, err := Open(s.dir)
	s.Require().NoError(err)
	s.Require().NoError(w.Close())

	_, err = w.NewReader(0)
	s.Require().ErrorIs(err, ErrClosed)
}

func (s *ReaderSuite) TestReplayVisitsEveryEntry() {
	w := s.appendSequential(4)
	defer func() { s.Require().NoError(w.Close()) }()

	var sum uint64
	err := w.Replay(0, func(entry Entry) error {
		sum += entry.LSN

		return nil
	})
	s.Require().NoError(err)
	s.Require().Equal(uint64(1+2+3+4), sum)
}

func (s *ReaderSuite) TestReplayStopsOnError() {
	w := s.appendSequential(4)
	defer func() { s.Require().NoError(w.Close()) }()

	stop := errors.New("stop replay")
	visited := 0
	err := w.Replay(0, func(entry Entry) error {
		visited++
		if entry.LSN == 2 {
			return stop
		}

		return nil
	})

	s.Require().ErrorIs(err, stop)
	s.Require().Equal(2, visited)
}

func (s *ReaderSuite) TestErrTruncatedIsDistinctSentinel() {
	s.Require().Error(ErrTruncated)
	s.Assert().False(errors.Is(ErrTruncated, ErrCorrupt))
	s.Assert().False(errors.Is(ErrTruncated, ErrClosed))
	s.Assert().Contains(ErrTruncated.Error(), "truncated")
}

func (s *ReaderSuite) TestReaderSnapshotReportsBounds() {
	w := s.appendSequential(3)
	defer func() { _ = w.Close() }()

	segmentNames, endLSN, firstLSN := w.readerSnapshot()
	s.Require().NotEmpty(segmentNames)
	s.Assert().Equal(uint64(3), endLSN)
	s.Assert().Equal(uint64(1), firstLSN)
}

func (s *ReaderSuite) TestReaderSeesRecordsMovedByConcurrentCut() {
	w := s.appendSequential(100)
	defer func() { _ = w.Close() }()

	// The snapshot names only the active seg-1; the cut rewrites it into seg-40
	// and deletes seg-1 before the reader opens anything.
	reader, err := w.NewReader(50)
	s.Require().NoError(err)
	defer func() { _ = reader.Close() }()

	s.Require().NoError(w.CutOffset(40))

	entries := s.collect(reader)
	s.Require().Len(entries, 51, "records 50..100 survive the cut and must be read")

	for i, entry := range entries {
		lsn := uint64(50 + i)
		s.Assert().Equal(lsn, entry.LSN)
		s.Assert().Equal(payloadForLSN(lsn), entry.Payload)
	}
}

func (s *ReaderSuite) TestReaderSkipsOverlapLeftByInProgressCut() {
	w := s.appendSequential(10)
	maxRecord := w.opts.maxRecordSize
	s.Require().NoError(w.Close())

	// Mid-cut state: seg-5 has been written but the old boundary seg-1 is not yet
	// deleted, so LSNs 5..10 exist in both files.
	root, err := os.OpenRoot(s.dir)
	s.Require().NoError(err)
	defer func() { _ = root.Close() }()

	source, err := segment.Open(root, 1)
	s.Require().NoError(err)
	replacement, err := segment.RewriteFrom(root, source, 5, maxRecord)
	s.Require().NoError(err)
	s.Require().NoError(replacement.Close())
	s.Require().NoError(source.Close())

	// A snapshot segment that has vanished forces the reader to re-list the
	// directory, where it finds both overlapping segments.
	reader := newReaderFrom(s.dir, []string{segment.Name(0)}, 0, 10, maxRecord)
	defer func() { _ = reader.Close() }()

	entries := s.collect(reader)
	s.Require().Len(entries, 10, "overlapping records must be yielded once")

	for i, entry := range entries {
		s.Assert().Equal(uint64(i+1), entry.LSN)
	}
}
