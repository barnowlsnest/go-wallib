package segment

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/barnowlsnest/go-wallib/internal/record"
)

// segMaxRecordBytes is a per-record cap larger than any test payload.
const segMaxRecordBytes = 4 << 20 // 4 MiB

// realistic WAL payloads resembling serialized state-change commands.
var (
	segSetCommand    = []byte(`{"op":"set","key":"account:1001","value":{"balance":250000,"currency":"USD"}}`)
	segDeleteCommand = []byte(`{"op":"delete","key":"session:9f3c1a7e-7b2d-4e51-bc44-6d2a0f1e88aa"}`)
	segCheckpoint    = []byte("checkpoint: replica=eu-west-1 offset=4096 term=7 leader=node-3")
)

// SegmentSuite exercises the segment file lifecycle against a real temp
// directory opened as an os.Root.
type SegmentSuite struct {
	suite.Suite
	root *os.Root
	dir  string
}

func TestSegmentSuite(t *testing.T) {
	suite.Run(t, new(SegmentSuite))
}

func (s *SegmentSuite) SetupTest() {
	s.dir = s.T().TempDir()
	root, err := os.OpenRoot(s.dir)
	s.Require().NoError(err)
	s.root = root
}

func (s *SegmentSuite) TearDownTest() {
	s.Require().NoError(s.root.Close())
}

func (s *SegmentSuite) TestCreateThenReopen() {
	seg, err := Create(s.root, 1)
	s.Require().NoError(err)
	s.Require().Equal(uint64(1), seg.BaseLSN())
	s.Require().Equal(int64(HeaderSize), seg.Size())
	s.Require().Equal(uint64(0), seg.LastLSN(), "empty segment reports baseLSN-1")
	s.Require().Equal(Name(1), seg.Name())
	s.Require().NoError(seg.Close())

	reopened, err := Open(s.root, 1)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(reopened.Close()) }()
	s.Require().Equal(uint64(1), reopened.BaseLSN())
	s.Require().Equal(int64(HeaderSize), reopened.Size())
}

func (s *SegmentSuite) TestCreateRejectsDuplicate() {
	seg, err := Create(s.root, 1)
	s.Require().NoError(err)
	s.Require().NoError(seg.Close())

	_, err = Create(s.root, 1)
	s.Require().Error(err, "creating an existing segment must fail (O_EXCL)")
}

func (s *SegmentSuite) TestOpenRejectsFilenameBaseMismatch() {
	seg, err := Create(s.root, 5)
	s.Require().NoError(err)
	s.Require().NoError(seg.Close())

	// Rename so the filename claims base 7 while the header still says 5.
	s.Require().NoError(os.Rename(
		filepath.Join(s.dir, Name(5)),
		filepath.Join(s.dir, Name(7)),
	))

	_, err = Open(s.root, 7)
	s.Require().ErrorIs(err, ErrCorrupt)
}

func (s *SegmentSuite) TestOpenMissingSegmentErrors() {
	_, err := Open(s.root, 42)
	s.Require().Error(err)
}

func (s *SegmentSuite) TestAppendThenScan() {
	seg, err := Create(s.root, 1)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(seg.Close()) }()

	payloads := [][]byte{segSetCommand, segDeleteCommand, segCheckpoint}
	for i, payload := range payloads {
		written, appendErr := seg.Append(uint64(i+1), payload)
		s.Require().NoError(appendErr)
		s.Require().Equal(record.EncodedSize(len(payload)), written)
	}
	s.Require().Equal(uint64(3), seg.LastLSN())
	s.Require().NoError(seg.Sync())

	result := seg.Scan(segMaxRecordBytes)
	s.Require().NoError(result.Err)
	s.Require().Equal(uint64(3), result.Records)
	s.Require().Equal(uint64(3), result.LastLSN)
	s.Require().Equal(seg.Size(), result.ValidEnd)
}

// Staged records reach the file only on Flush, in one write, and Discard drops
// the ones staged after a mark.
func (s *SegmentSuite) TestStageThenFlush() {
	payloads := [][]byte{segSetCommand, segDeleteCommand, segCheckpoint}
	tests := []struct {
		name string
		keep int // records kept by Discard; len(payloads) keeps all of them
	}{
		{name: "flush all staged", keep: 3},
		{name: "discard after the first", keep: 1},
		{name: "discard everything", keep: 0},
	}

	for i, tc := range tests {
		s.Run(tc.name, func() {
			seg, err := Create(s.root, uint64(i*10+1))
			s.Require().NoError(err)
			defer func() { s.Require().NoError(seg.Close()) }()

			keepBytes := 0
			for n, payload := range payloads {
				if n == tc.keep {
					keepBytes = seg.Staged()
				}
				s.Require().NoError(seg.Stage(seg.BaseLSN()+uint64(n), payload))
			}
			if tc.keep == len(payloads) {
				keepBytes = seg.Staged()
			}
			s.Require().Equal(int64(HeaderSize), seg.Size(), "staging must not write")

			seg.Discard(keepBytes, seg.BaseLSN()+uint64(tc.keep)-1)
			s.Require().NoError(seg.Flush())

			expectedSize := int64(HeaderSize)
			for _, payload := range payloads[:tc.keep] {
				expectedSize += int64(record.EncodedSize(len(payload)))
			}
			s.Require().Equal(0, seg.Staged())
			s.Require().Equal(expectedSize, seg.Size())
			s.Require().Equal(seg.BaseLSN()+uint64(tc.keep)-1, seg.LastLSN())

			actual := seg.Scan(segMaxRecordBytes)
			s.Require().NoError(actual.Err)
			s.Require().Equal(uint64(tc.keep), actual.Records)
			s.Require().Equal(expectedSize, actual.ValidEnd)
		})
	}
}

// A failed Flush drops the staged records and leaves the size and last LSN as
// they were, so the next write lands at the same offset.
func (s *SegmentSuite) TestFlushFailureDropsStaged() {
	seg, err := Create(s.root, 1)
	s.Require().NoError(err)

	s.Require().NoError(seg.Stage(1, segSetCommand))
	s.Require().NoError(seg.Flush())
	expectedSize := seg.Size()

	s.Require().NoError(seg.Stage(2, segDeleteCommand))
	s.Require().NoError(seg.Close())

	s.Require().Error(seg.Flush())
	s.Require().Equal(0, seg.Staged())
	s.Require().Equal(expectedSize, seg.Size())
	s.Require().Equal(uint64(1), seg.LastLSN())
}

func (s *SegmentSuite) TestScanTornTailThenTruncate() {
	seg, err := Create(s.root, 1)
	s.Require().NoError(err)

	_, err = seg.Append(1, segSetCommand)
	s.Require().NoError(err)
	goodSize := seg.Size()

	_, err = seg.Append(2, segDeleteCommand)
	s.Require().NoError(err)
	s.Require().NoError(seg.Sync())

	// Simulate a torn tail by chopping the last few bytes of the second record.
	s.Require().NoError(seg.TruncateTo(seg.Size() - 3))
	s.Require().NoError(seg.Close())

	reopened, err := Open(s.root, 1)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(reopened.Close()) }()

	result := reopened.Scan(segMaxRecordBytes)
	s.Require().ErrorIs(result.Err, record.ErrTorn)
	s.Require().Equal(uint64(1), result.Records, "only the intact first record survives")
	s.Require().Equal(uint64(1), result.LastLSN)
	s.Require().Equal(goodSize, result.ValidEnd, "truncation point is the end of the last good record")
}

func (s *SegmentSuite) TestRewriteFromKeepsRecordsAtOrAboveThreshold() {
	src, err := Create(s.root, 10)
	s.Require().NoError(err)

	payloads := [][]byte{segSetCommand, segDeleteCommand, segCheckpoint}
	for i, payload := range payloads {
		_, appendErr := src.Append(uint64(10+i), payload) // LSNs 10, 11, 12
		s.Require().NoError(appendErr)
	}
	s.Require().NoError(src.Sync())

	dst, err := RewriteFrom(s.root, src, 11, segMaxRecordBytes) // keep 11, 12
	s.Require().NoError(err)
	defer func() { s.Require().NoError(dst.Close()) }()

	s.Require().Equal(uint64(11), dst.BaseLSN())
	s.Require().Equal(Name(11), dst.Name())
	s.Require().Equal(uint64(12), dst.LastLSN())

	result := dst.Scan(segMaxRecordBytes)
	s.Require().NoError(result.Err)
	s.Require().Equal(uint64(2), result.Records, "only records with LSN >= 11 survive")
	s.Require().Equal(uint64(12), result.LastLSN)

	// src is untouched.
	s.Require().Equal(uint64(12), src.LastLSN())
	s.Require().NoError(src.Close())
}

func (s *SegmentSuite) TestRewriteFromAllRecordsBelowThresholdIsEmpty() {
	src, err := Create(s.root, 1)
	s.Require().NoError(err)
	_, err = src.Append(1, segSetCommand)
	s.Require().NoError(err)
	s.Require().NoError(src.Sync())

	dst, err := RewriteFrom(s.root, src, 5, segMaxRecordBytes) // nothing kept
	s.Require().NoError(err)
	defer func() { s.Require().NoError(dst.Close()) }()

	s.Require().Equal(int64(HeaderSize), dst.Size(), "no records copied")
	result := dst.Scan(segMaxRecordBytes)
	s.Require().NoError(result.Err)
	s.Require().Equal(uint64(0), result.Records)
	s.Require().NoError(src.Close())
}

func (s *SegmentSuite) TestRewriteFromPublishesAtomicallyLeavingNoTemp() {
	src, err := Create(s.root, 10)
	s.Require().NoError(err)
	for i, payload := range [][]byte{segSetCommand, segDeleteCommand} {
		_, appendErr := src.Append(uint64(10+i), payload)
		s.Require().NoError(appendErr)
	}
	s.Require().NoError(src.Sync())

	dst, err := RewriteFrom(s.root, src, 10, segMaxRecordBytes)
	s.Require().NoError(err)
	s.Require().NoError(dst.Close())
	s.Require().NoError(src.Close())

	_, statErr := os.Stat(filepath.Join(s.dir, Name(10)))
	s.Require().NoError(statErr, "the canonical segment is published")
	_, tempErr := os.Stat(filepath.Join(s.dir, Name(10)+TempSuffix))
	s.Require().True(os.IsNotExist(tempErr), "no temp file remains after a successful rewrite")
}

func (s *SegmentSuite) TestRewriteFromOverwritesStaleTemp() {
	src, err := Create(s.root, 1)
	s.Require().NoError(err)
	_, err = src.Append(1, segSetCommand)
	s.Require().NoError(err)
	s.Require().NoError(src.Sync())

	// A leftover temp from a previous interrupted rewrite must not block O_EXCL.
	s.Require().NoError(os.WriteFile(filepath.Join(s.dir, Name(1)+TempSuffix), []byte("stale"), 0o600))

	dst, err := RewriteFrom(s.root, src, 1, segMaxRecordBytes)
	s.Require().NoError(err)
	s.Require().NoError(dst.Close())
	s.Require().NoError(src.Close())
	s.Require().Equal(uint64(1), dst.LastLSN())
}

// indexPayload is large enough that a few hundred records span several
// IndexInterval steps.
var indexPayload = make([]byte, 1024)

// indexRecordCount records of indexPayload fill about 300 KiB, so an index over
// them holds several entries.
const indexRecordCount = 300

// TestIndexCoversWrittenRecords checks the index of a segment however its
// records got there: each entry names the record that starts at its offset, and
// entries are one IndexInterval apart, give or take one record.
func (s *SegmentSuite) TestIndexCoversWrittenRecords() {
	testCases := []struct {
		name  string
		build func(baseLSN uint64) *Segment
		// skipped is how many records at the start of the segment the build
		// leaves out.
		skipped uint64
	}{
		{name: "staged and flushed once", build: s.stagedSegment(indexRecordCount)},
		{name: "staged and flushed in parts", build: s.stagedSegment(10)},
		{name: "appended", build: s.appendedSegment},
		{name: "scanned after reopen", build: s.rescannedSegment},
		{name: "rewritten by a cut", build: s.rewrittenSegment(100), skipped: 100},
	}

	for i, tc := range testCases {
		s.Run(tc.name, func() {
			baseLSN := uint64(i*1000 + 1)
			seg := tc.build(baseLSN)
			defer func() { s.Require().NoError(seg.Close()) }()

			s.requireIndexMatchesFile(seg, baseLSN+tc.skipped)
		})
	}
}

// Staged records are not indexed until Flush writes them.
func (s *SegmentSuite) TestIndexWaitsForFlush() {
	seg, err := Create(s.root, 1)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(seg.Close()) }()

	for lsn := uint64(1); lsn <= indexRecordCount; lsn++ {
		s.Require().NoError(seg.Stage(lsn, indexPayload))
	}
	s.Require().Empty(seg.Index())

	s.Require().NoError(seg.Flush())
	s.Require().NotEmpty(seg.Index())
}

// Discarded or unwritten staged records leave no index entries behind, so
// records staged again under the same LSNs are indexed at their real offsets.
func (s *SegmentSuite) TestIndexDropsUnwrittenStaged() {
	testCases := []struct {
		name    string
		discard func(seg *Segment)
	}{
		{name: "discard", discard: func(seg *Segment) { seg.Discard(0, 0) }},
		{name: "failed flush", discard: func(seg *Segment) {
			// A closed file makes the write fail; a fresh handle on the same
			// file lets the segment carry on.
			s.Require().NoError(seg.file.Close())
			s.Require().Error(seg.Flush())

			file, err := s.root.OpenFile(seg.Name(), os.O_RDWR, 0)
			s.Require().NoError(err)
			seg.file = file
		}},
	}

	for i, tc := range testCases {
		s.Run(tc.name, func() {
			baseLSN := uint64(i*1000 + 1)
			seg, err := Create(s.root, baseLSN)
			s.Require().NoError(err)
			defer func() { s.Require().NoError(seg.Close()) }()

			s.Require().NoError(seg.Stage(baseLSN, segSetCommand))
			s.Require().NoError(seg.Flush())

			s.stageRange(seg, baseLSN+1, baseLSN+indexRecordCount)
			tc.discard(seg)
			s.Require().Equal(0, seg.Staged())

			s.Require().NoError(seg.Flush())
			s.Require().Empty(seg.Index())

			s.stageRange(seg, baseLSN+1, baseLSN+indexRecordCount)
			s.Require().NoError(seg.Flush())
			s.requireIndexMatchesFile(seg, baseLSN)
		})
	}
}

func (s *SegmentSuite) TestLookup() {
	index := []IndexEntry{{LSN: 100, Offset: 70_000}, {LSN: 200, Offset: 140_000}}

	testCases := []struct {
		name           string
		index          []IndexEntry
		lsn            uint64
		expectedOffset int64
		expectedOK     bool
	}{
		{name: "empty index", lsn: 150},
		{name: "below first entry", index: index, lsn: 99},
		{name: "on an entry", index: index, lsn: 100, expectedOffset: 70_000, expectedOK: true},
		{name: "between entries", index: index, lsn: 199, expectedOffset: 70_000, expectedOK: true},
		{name: "above last entry", index: index, lsn: 1_000, expectedOffset: 140_000, expectedOK: true},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			actualOffset, actualOK := Lookup(tc.index, tc.lsn)

			s.Require().Equal(tc.expectedOK, actualOK)
			s.Require().Equal(tc.expectedOffset, actualOffset)
		})
	}
}

// stageRange stages one indexPayload record for every LSN in [from, to].
func (s *SegmentSuite) stageRange(seg *Segment, from, to uint64) {
	s.T().Helper()

	for lsn := from; lsn <= to; lsn++ {
		s.Require().NoError(seg.Stage(lsn, indexPayload))
	}
}

// stagedSegment writes indexRecordCount records with Stage, flushing every
// flushEvery records.
func (s *SegmentSuite) stagedSegment(flushEvery uint64) func(baseLSN uint64) *Segment {
	return func(baseLSN uint64) *Segment {
		seg, err := Create(s.root, baseLSN)
		s.Require().NoError(err)

		for n := uint64(1); n <= indexRecordCount; n++ {
			s.Require().NoError(seg.Stage(baseLSN+n-1, indexPayload))
			if n%flushEvery == 0 {
				s.Require().NoError(seg.Flush())
			}
		}
		s.Require().NoError(seg.Flush())

		return seg
	}
}

func (s *SegmentSuite) appendedSegment(baseLSN uint64) *Segment {
	seg, err := Create(s.root, baseLSN)
	s.Require().NoError(err)

	for lsn := baseLSN; lsn < baseLSN+indexRecordCount; lsn++ {
		_, appendErr := seg.Append(lsn, indexPayload)
		s.Require().NoError(appendErr)
	}

	return seg
}

func (s *SegmentSuite) rescannedSegment(baseLSN uint64) *Segment {
	written := s.appendedSegment(baseLSN)
	s.Require().NoError(written.Close())

	seg, err := Open(s.root, baseLSN)
	s.Require().NoError(err)
	s.Require().NoError(seg.Scan(segMaxRecordBytes).Err)

	return seg
}

// rewrittenSegment rewrites an appended segment from the record skip places
// past its base.
func (s *SegmentSuite) rewrittenSegment(skip uint64) func(baseLSN uint64) *Segment {
	return func(baseLSN uint64) *Segment {
		src := s.appendedSegment(baseLSN)
		defer func() { s.Require().NoError(src.Close()) }()

		seg, err := RewriteFrom(s.root, src, baseLSN+skip, segMaxRecordBytes)
		s.Require().NoError(err)

		return seg
	}
}

// requireIndexMatchesFile checks that seg's index is non-empty, that every
// entry's offset in the file starts the record with the entry's LSN, and that
// entries are spaced IndexInterval apart, give or take one record. firstLSN is
// the segment's first record, which is never indexed: a reader reaches it from
// the header anyway.
func (s *SegmentSuite) requireIndexMatchesFile(seg *Segment, firstLSN uint64) {
	s.T().Helper()

	index := seg.Index()
	s.Require().NotEmpty(index)

	file, err := s.root.Open(seg.Name())
	s.Require().NoError(err)
	defer func() { s.Require().NoError(file.Close()) }()

	previous := int64(HeaderSize)
	for _, entry := range index {
		gap := entry.Offset - previous
		s.Require().GreaterOrEqual(gap, int64(IndexInterval), "entry for LSN %d", entry.LSN)
		s.Require().Less(gap, int64(IndexInterval+record.EncodedSize(len(indexPayload))), "entry for LSN %d", entry.LSN)
		s.Require().Greater(entry.LSN, firstLSN)
		previous = entry.Offset

		scanner := record.NewScanner(io.NewSectionReader(file, entry.Offset, seg.Size()-entry.Offset), segMaxRecordBytes)
		s.Require().True(scanner.Next(), "no record at offset %d", entry.Offset)
		s.Require().Equal(entry.LSN, scanner.Record().LSN)
	}
}
