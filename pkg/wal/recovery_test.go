package wal

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/barnowlsnest/go-wallib/internal/segment"
)

// fabricationMaxRecordSize is a per-record cap larger than any test payload,
// used only when fabricating on-disk segment layouts directly via the
// internal/segment package (bypassing the WAL's own maxRecordSize option).
const fabricationMaxRecordSize = 4 << 20 // 4 MiB

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

// fabricateInterruptedCut writes segments that look like a cut that created
// seg-<cutBase> but crashed before deleting the old below-cut segments. It
// leaves both the old contiguous segment (records 1..4) and the overlapping
// seg-<cutBase> on disk.
func (s *RecoverySuite) fabricateInterruptedCut(cutBase uint64) {
	s.T().Helper()

	root, err := os.OpenRoot(s.dir)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(root.Close()) }()

	old, err := segment.Create(root, 1)
	s.Require().NoError(err)
	for lsn := uint64(1); lsn <= 4; lsn++ {
		_, appendErr := old.Append(lsn, payloadForLSN(lsn))
		s.Require().NoError(appendErr)
	}
	s.Require().NoError(old.Sync())

	// The interrupted rewrite: seg-<cutBase> holding the surviving tail
	// (cutBase..4), created and fsynced but the old segment was never deleted.
	replacement, err := segment.RewriteFrom(root, old, cutBase, fabricationMaxRecordSize)
	s.Require().NoError(err)
	s.Require().NoError(replacement.Close())
	s.Require().NoError(old.Close())
}

func (s *RecoverySuite) TestInterruptedCutIsReconciled() {
	s.fabricateInterruptedCut(3) // keep 3, 4; discard 1, 2

	w, report, err := Open(s.dir)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(w.Close()) }()

	s.Require().Equal(1, report.SegmentsRemoved, "the superseded below-cut segment is deleted")
	s.Require().Equal(1, w.segmentCount())
	s.Require().Equal(uint64(3), w.FirstLSN())
	s.Require().Equal(uint64(4), w.LastLSN())

	var replayed []uint64
	err = w.Replay(0, func(entry Entry) error {
		replayed = append(replayed, entry.LSN)

		return nil
	})
	s.Require().NoError(err)
	s.Require().Equal([]uint64{3, 4}, replayed, "only the surviving tail remains")
}

func (s *RecoverySuite) TestInterruptedRewriteRevertsAndCleansTemp() {
	// A rewrite for CutOffset(3) that crashed before the atomic rename: the old
	// boundary segment (records 1..4) is fully intact and only a stale temp file
	// exists; no seg-3 was ever published. Recovery must keep all original data
	// (revert the cut), reuse no LSN, and delete the temp file.
	root, err := os.OpenRoot(s.dir)
	s.Require().NoError(err)
	old, err := segment.Create(root, 1)
	s.Require().NoError(err)
	for lsn := uint64(1); lsn <= 4; lsn++ {
		_, appendErr := old.Append(lsn, payloadForLSN(lsn))
		s.Require().NoError(appendErr)
	}
	s.Require().NoError(old.Sync())
	s.Require().NoError(old.Close())
	s.Require().NoError(root.Close())

	tempName := segment.Name(3) + segment.TempSuffix
	s.Require().NoError(os.WriteFile(filepath.Join(s.dir, tempName), []byte("partial rewrite"), 0o600))

	w, _, err := Open(s.dir)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(w.Close()) }()

	s.Require().Equal(uint64(1), w.FirstLSN())
	s.Require().Equal(uint64(4), w.LastLSN())

	var replayed []uint64
	err = w.Replay(0, func(entry Entry) error {
		replayed = append(replayed, entry.LSN)

		return nil
	})
	s.Require().NoError(err)
	s.Require().Equal([]uint64{1, 2, 3, 4}, replayed, "no >= K data lost; cut reverted")

	lsn, err := w.Append(context.Background(), payloadForLSN(5))
	s.Require().NoError(err)
	s.Require().Equal(uint64(5), lsn, "no LSN reuse after an interrupted rewrite")

	_, statErr := os.Stat(filepath.Join(s.dir, tempName))
	s.Require().True(os.IsNotExist(statErr), "the stale temp file is cleaned up on recovery")
}

func (s *RecoverySuite) TestGenuineGapStillFails() {
	// base 1 covers 1..2, next claims base 9 -> gap, not overlap.
	s.appendRecords(2, WithMaxSegmentSize(40))
	s.createEmptySegment(9)

	_, _, err := Open(s.dir)
	s.Require().ErrorIs(err, ErrCorrupt)
}
