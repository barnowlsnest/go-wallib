package wal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/barnowlsnest/go-wallib/internal/record"
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

// segmentBases returns a copy of the WAL's segment base LSNs, oldest first.
func (w *WAL) segmentBases() []uint64 {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return slices.Clone(w.segmentBaseLSNs)
}

// corruptFirstPayloadByte flips the first payload byte of the first record in
// the segment based at baseLSN, so any read of that record fails its CRC.
func corruptFirstPayloadByte(t *testing.T, dir string, baseLSN uint64) {
	t.Helper()

	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()

	file, err := root.OpenFile(segment.Name(baseLSN), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()

	offset := int64(segment.HeaderSize + record.HeaderSize)
	current := make([]byte, 1)
	if _, err := file.ReadAt(current, offset); err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{current[0] ^ 0xFF}, offset); err != nil {
		t.Fatal(err)
	}
}

// A reader opens the segment that holds fromLSN and never reads the segments
// before it, so a corrupt earlier segment cannot fail a read that starts later.
func (s *ReaderSuite) TestReaderStartsInSegmentHoldingFromLSN() {
	cases := []struct {
		fromLSN func(bases []uint64, lastLSN uint64) uint64
		name    string
	}{
		{name: "first record of a later segment", fromLSN: func(bases []uint64, _ uint64) uint64 { return bases[2] }},
		{name: "inside a later segment", fromLSN: func(bases []uint64, _ uint64) uint64 { return bases[2] + 1 }},
		{name: "last record", fromLSN: func(_ []uint64, lastLSN uint64) uint64 { return lastLSN }},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			s.dir = s.T().TempDir()
			w := s.appendSequential(20, WithMaxSegmentSize(256))
			defer func() { s.Require().NoError(w.Close()) }()

			bases := w.segmentBases()
			s.Require().GreaterOrEqual(len(bases), 4)
			s.Require().Greater(bases[3], bases[2]+1, "segment 2 must hold at least two records")
			corruptFirstPayloadByte(s.T(), s.dir, bases[0])

			fromLSN := tc.fromLSN(bases, w.LastLSN())
			reader, err := w.NewReader(fromLSN)
			s.Require().NoError(err)
			defer func() { s.Require().NoError(reader.Close()) }()

			actual := s.collect(reader)
			s.Require().Len(actual, int(w.LastLSN()-fromLSN+1))
			for i, entry := range actual {
				expected := fromLSN + uint64(i)
				s.Assert().Equal(expected, entry.LSN)
				s.Assert().Equal(payloadForLSN(expected), entry.Payload)
			}
		})
	}
}

// indexedPayload is payloadForLSN padded to about 1 KiB, so a few hundred
// records span several index entries.
func indexedPayload(lsn uint64) []byte {
	return append(payloadForLSN(lsn), make([]byte, 1000)...)
}

// appendIndexed appends count indexedPayload records in batches of 16.
func (s *ReaderSuite) appendIndexed(w *WAL, count int) {
	s.T().Helper()

	for count > 0 {
		batch := make([][]byte, 0, min(count, 16))
		for lsn := w.LastLSN() + 1; len(batch) < cap(batch); lsn++ {
			batch = append(batch, indexedPayload(lsn))
		}

		_, err := w.AppendBatch(context.Background(), batch)
		s.Require().NoError(err)
		count -= len(batch)
	}
}

// TestReaderStartsFromIndex checks that a reader near the tail starts at the
// indexed offset instead of the segment start, and still reads the right
// records, however the segment and its index came about.
func (s *ReaderSuite) TestReaderStartsFromIndex() {
	testCases := []struct {
		name  string
		build func() *WAL
	}{
		{name: "live appends", build: func() *WAL {
			w := s.appendSequential(0)
			s.appendIndexed(w, 300)

			return w
		}},
		{name: "after reopen", build: func() *WAL {
			w := s.appendSequential(0)
			s.appendIndexed(w, 300)
			s.Require().NoError(w.Close())

			reopened, _, err := Open(s.dir, WithSyncPolicy(SyncImmediate))
			s.Require().NoError(err)

			return reopened
		}},
		{name: "after roll", build: func() *WAL {
			w := s.appendSequential(0, WithMaxSegmentSize(256<<10))
			s.appendIndexed(w, 600)
			s.Require().GreaterOrEqual(w.segmentCount(), 3)

			return w
		}},
		{name: "after cut rewrites the active segment", build: func() *WAL {
			w := s.appendSequential(0)
			s.appendIndexed(w, 300)
			s.Require().NoError(w.CutOffset(100))
			s.appendIndexed(w, 100)

			return w
		}},
		{name: "after truncate", build: func() *WAL {
			w := s.appendSequential(0, WithMaxSegmentSize(128<<10))
			s.appendIndexed(w, 600)
			s.Require().NoError(w.Truncate(400))
			s.Require().Greater(w.FirstLSN(), uint64(1))

			return w
		}},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			s.SetupTest()

			w := tc.build()
			defer func() { s.Require().NoError(w.Close()) }()

			s.requireIndexesMatchSegments(w)

			lastLSN := w.LastLSN()
			fromLSN := lastLSN - 5

			start := w.indexedPosition(fromLSN)
			s.Require().Greater(start.offset, int64(segment.HeaderSize), "no index entry at or below LSN %d", fromLSN)

			reader, err := w.NewReader(fromLSN)
			s.Require().NoError(err)
			defer func() { s.Require().NoError(reader.Close()) }()
			s.Require().Equal(start.offset, reader.startOffset)

			entries := s.collect(reader)
			s.Require().Len(entries, int(lastLSN-fromLSN+1))
			for i, entry := range entries {
				expectedLSN := fromLSN + uint64(i)
				s.Require().Equal(expectedLSN, entry.LSN)
				s.Require().Equal(indexedPayload(expectedLSN), entry.Payload)
			}
		})
	}
}

// A reader from before the first index entry, or from the beginning, still
// starts at the segment header.
func (s *ReaderSuite) TestReaderBeforeFirstIndexEntryStartsAtHeader() {
	w := s.appendSequential(0)
	defer func() { s.Require().NoError(w.Close()) }()
	s.appendIndexed(w, 300)

	for _, fromLSN := range []uint64{0, 1, 2} {
		s.Require().Equal(readerPosition{}, w.indexedPosition(fromLSN), "from LSN %d", fromLSN)

		reader, err := w.NewReader(fromLSN)
		s.Require().NoError(err)
		s.Require().Equal(int64(0), reader.startOffset)
		s.Require().Len(s.collect(reader), 300-int(max(fromLSN, 1))+1)
		s.Require().NoError(reader.Close())
	}
}

// requireIndexesMatchSegments checks that the WAL keeps an index only for
// segments it still has.
func (s *ReaderSuite) requireIndexesMatchSegments(w *WAL) {
	s.T().Helper()

	w.mu.RLock()
	defer w.mu.RUnlock()

	for baseLSN := range w.indexes {
		s.Require().Contains(w.segmentBaseLSNs, baseLSN, "index kept for removed segment %d", baseLSN)
	}
}

// A reader asked to retain its file hands back the open file of the segment it
// ended in, and a later reader offered that file reads through it instead of
// opening the segment again.
func (s *ReaderSuite) TestReaderHandsBackAndReusesSegmentFile() {
	w := s.appendSequential(5)
	defer func() { s.Require().NoError(w.Close()) }()

	segmentNames, endLSN, _ := w.readerSnapshot()
	first := newReaderFrom(w.dir, segmentNames, 1, endLSN, w.opts.maxRecordSize)
	first.retain = true
	s.Require().Len(s.collect(first), 5)
	s.Require().NoError(first.Close())

	held, heldName := first.takeRetained()
	s.Require().NotNil(held)
	s.Require().Equal(segmentNames[len(segmentNames)-1], heldName)

	for lsn := uint64(6); lsn <= 8; lsn++ {
		_, err := w.Append(context.Background(), payloadForLSN(lsn))
		s.Require().NoError(err)
	}

	segmentNames, endLSN, _ = w.readerSnapshot()
	second := newReaderFrom(w.dir, segmentNames, 6, endLSN, w.opts.maxRecordSize)
	second.resumeAt(first.lastPosition())
	second.retain = true
	second.offer(held, heldName)

	var actual []uint64
	for second.Next() {
		s.Require().Same(held, second.file, "reader opened the segment again")
		actual = append(actual, second.Entry().LSN)
	}
	s.Require().NoError(second.Err())
	s.Require().Equal([]uint64{6, 7, 8}, actual)
	s.Require().NoError(second.Close())

	retained, _ := second.takeRetained()
	s.Require().Same(held, retained)
	s.Require().NoError(retained.Close())
}

// A file offered for a segment the reader never reads is closed with the
// reader, and a reader that was not asked to retain closes its own file.
func (s *ReaderSuite) TestReaderClosesFilesItDoesNotHandBack() {
	w := s.appendSequential(5)
	defer func() { s.Require().NoError(w.Close()) }()

	offered, err := os.OpenInRoot(w.dir, segment.Name(1))
	s.Require().NoError(err)

	segmentNames, endLSN, _ := w.readerSnapshot()
	reader := newReaderFrom(w.dir, segmentNames, 1, endLSN, w.opts.maxRecordSize)
	reader.offer(offered, "not-a-segment-in-the-snapshot.wal")

	s.Require().Len(s.collect(reader), 5)
	s.Require().NoError(reader.Close())

	retained, _ := reader.takeRetained()
	s.Require().Nil(retained)
	s.Require().ErrorIs(offered.Close(), os.ErrClosed)
}
