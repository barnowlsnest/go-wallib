package wal

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/barnowlsnest/go-wallib/internal/segment"
)

// RecoverySuite reaches into segment files on disk to simulate crashes and
// verifies Open's recovery behavior (spec §9).
type RecoverySuite struct {
	suite.Suite
	dir string
}

func TestRecoverySuite(t *testing.T) {
	suite.Run(t, new(RecoverySuite))
}

func (s *RecoverySuite) SetupTest() {
	s.dir = s.T().TempDir()
}

// appendRecords opens a WAL, appends count records, and closes it.
func (s *RecoverySuite) appendRecords(count int, opts ...Option) {
	s.T().Helper()

	w, _, err := Open(s.dir, append([]Option{WithSyncPolicy(SyncImmediate)}, opts...)...)
	s.Require().NoError(err)

	for i := 1; i <= count; i++ {
		_, appendErr := w.Append(context.Background(), payloadForLSN(uint64(i)))
		s.Require().NoError(appendErr)
	}

	s.Require().NoError(w.Close())
}

// segmentPaths returns the segment file paths in base-LSN order.
func (s *RecoverySuite) segmentPaths() []string {
	s.T().Helper()

	paths, err := segment.List(s.dir)
	s.Require().NoError(err)

	return paths
}

func (s *RecoverySuite) TestTornTailIsTruncated() {
	s.appendRecords(3)

	// Append a partial record header to the last segment: an interrupted write.
	paths := s.segmentPaths()
	last, err := os.OpenFile(paths[len(paths)-1], os.O_WRONLY|os.O_APPEND, 0o600)
	s.Require().NoError(err)
	_, err = last.Write([]byte{0xDE, 0xAD, 0xBE})
	s.Require().NoError(err)
	s.Require().NoError(last.Close())

	w, report, err := Open(s.dir)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(w.Close()) }()

	s.Require().Equal(uint64(3), report.LastLSN)
	s.Require().Equal(int64(3), report.BytesTruncated)
}

func (s *RecoverySuite) TestMidLogCorruptionFails() {
	// One record per segment so the corruption lands in a non-final segment.
	s.appendRecords(4, WithMaxSegmentSize(40))

	paths := s.segmentPaths()
	s.Require().GreaterOrEqual(len(paths), 2, "need a non-final segment to corrupt")

	// Flip the last payload byte of the first segment, breaking its CRC.
	s.flipLastByte(paths[0])

	_, _, err := Open(s.dir)
	s.Require().ErrorIs(err, ErrCorrupt)
}

func (s *RecoverySuite) TestInterruptedRollDeletesEmptyTrailingSegment() {
	s.appendRecords(1)

	// Simulate a roll interrupted after creating the new segment but before
	// writing any record: a header-only trailing segment based at LSN 2.
	s.createEmptySegment(2)

	w, report, err := Open(s.dir)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(w.Close()) }()

	s.Require().Equal(1, report.SegmentsRemoved)
	s.Require().Equal(1, w.segmentCount())
	s.Require().Equal(uint64(1), w.LastLSN())
}

func (s *RecoverySuite) TestRestoresWriterStateContiguously() {
	s.appendRecords(2)

	w, _, err := Open(s.dir)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(w.Close()) }()

	// The next append must continue at LSN 3 with no gap.
	lsn, err := w.Append(context.Background(), payloadForLSN(3))
	s.Require().NoError(err)
	s.Require().Equal(uint64(3), lsn)
	s.Require().Equal(uint64(1), w.FirstLSN())
	s.Require().Equal(uint64(3), w.LastLSN())
}

// flipLastByte corrupts the final byte of a file, breaking the last record's CRC.
func (s *RecoverySuite) flipLastByte(path string) {
	s.T().Helper()

	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(file.Close()) }()

	info, err := file.Stat()
	s.Require().NoError(err)

	offset := info.Size() - 1
	one := make([]byte, 1)
	_, err = file.ReadAt(one, offset)
	s.Require().NoError(err)

	one[0] ^= 0xFF
	_, err = file.WriteAt(one, offset)
	s.Require().NoError(err)
}

// createEmptySegment writes a header-only segment based at baseLSN.
func (s *RecoverySuite) createEmptySegment(baseLSN uint64) {
	s.T().Helper()

	root, err := os.OpenRoot(s.dir)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(root.Close()) }()

	seg, err := segment.Create(root, baseLSN)
	s.Require().NoError(err)
	s.Require().NoError(seg.Close())
}
