package wal

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/barnowlsnest/go-wallib/internal/segment"
)

// CutSuite covers CutOffset: precise front cuts that may rewrite the boundary
// (including the active) segment.
type CutSuite struct {
	suite.Suite
	dir string
}

func TestCutSuite(t *testing.T) {
	suite.Run(t, new(CutSuite))
}

func (s *CutSuite) SetupTest() {
	s.dir = s.T().TempDir()
}

// openOneRecordPerSegment opens a WAL whose segment threshold forces every
// record into its own segment, giving exact boundary control.
func (s *CutSuite) openOneRecordPerSegment() *WAL {
	s.T().Helper()

	w, _, err := Open(s.dir, WithSyncPolicy(SyncImmediate), WithMaxSegmentSize(40))
	s.Require().NoError(err)

	return w
}

func (s *CutSuite) appendN(w *WAL, count int) {
	s.T().Helper()

	for i := 1; i <= count; i++ {
		_, err := w.Append(context.Background(), payloadForLSN(uint64(i)))
		s.Require().NoError(err)
	}
}

func (s *CutSuite) replayLSNs(w *WAL) []uint64 {
	s.T().Helper()

	var lsns []uint64
	err := w.Replay(0, func(entry Entry) error {
		lsns = append(lsns, entry.LSN)

		return nil
	})
	s.Require().NoError(err)

	return lsns
}

func (s *CutSuite) TestBelowFirstIsNoop() {
	w := s.openOneRecordPerSegment()
	defer func() { s.Require().NoError(w.Close()) }()

	s.appendN(w, 3)
	before := w.segmentCount()

	s.Require().NoError(w.CutOffset(1)) // nothing is below LSN 1

	s.Require().Equal(before, w.segmentCount())
	s.Require().Equal(uint64(1), w.FirstLSN())
}

func (s *CutSuite) TestCutOnBoundaryDeletesWholeSegments() {
	w := s.openOneRecordPerSegment() // one record per segment: bases 1,2,3,4,5
	defer func() { s.Require().NoError(w.Close()) }()

	s.appendN(w, 5)

	s.Require().NoError(w.CutOffset(3)) // K == segment base 3: pure whole-segment delete

	s.Require().Equal(uint64(3), w.FirstLSN())
	s.Require().Equal(uint64(5), w.LastLSN())
	s.Require().Equal([]uint64{3, 4, 5}, s.replayLSNs(w))
}

func (s *CutSuite) TestCutRewritesSealedBoundarySegment() {
	// A big segment size so several records share one sealed segment, then roll.
	w, _, err := Open(s.dir, WithSyncPolicy(SyncImmediate), WithMaxSegmentSize(120))
	s.Require().NoError(err)
	defer func() { s.Require().NoError(w.Close()) }()

	s.appendN(w, 8)
	s.Require().Greater(w.segmentCount(), 1, "need a sealed boundary segment to rewrite")

	// Choose a K that lands strictly inside the first (sealed) segment.
	firstBase := w.FirstLSN()
	cut := firstBase + 1

	s.Require().NoError(w.CutOffset(cut))

	s.Require().Equal(cut, w.FirstLSN())
	s.Require().Equal(uint64(8), w.LastLSN())
	got := s.replayLSNs(w)
	s.Require().Equal(cut, got[0], "first surviving LSN is exactly K")
	s.Require().Equal(uint64(8), got[len(got)-1])
}

func (s *CutSuite) TestCutRewritesActiveSegmentAndAppendsContinue() {
	// One large segment holds every record, so the boundary IS the active segment.
	w, _, err := Open(s.dir, WithSyncPolicy(SyncImmediate))
	s.Require().NoError(err)
	defer func() { s.Require().NoError(w.Close()) }()

	s.appendN(w, 5)
	s.Require().Equal(1, w.segmentCount(), "all records in the active segment")

	s.Require().NoError(w.CutOffset(3)) // rewrite the active segment -> seg-<3>

	s.Require().Equal(uint64(3), w.FirstLSN())
	s.Require().Equal(uint64(5), w.LastLSN())

	// Appends continue gaplessly at nextLSN into the new active segment.
	lsn, err := w.Append(context.Background(), payloadForLSN(6))
	s.Require().NoError(err)
	s.Require().Equal(uint64(6), lsn)
	s.Require().Equal([]uint64{3, 4, 5, 6}, s.replayLSNs(w))
}

func (s *CutSuite) TestCutEverythingThenAppendContinues() {
	w, _, err := Open(s.dir, WithSyncPolicy(SyncImmediate))
	s.Require().NoError(err)
	defer func() { s.Require().NoError(w.Close()) }()

	s.appendN(w, 4)

	s.Require().NoError(w.CutOffset(1000)) // beyond lastLSN: delete everything

	s.Require().Equal(uint64(0), w.FirstLSN(), "empty log")
	s.Require().Equal(uint64(4), w.LastLSN(), "lastLSN is never lowered")
	s.Require().Empty(s.replayLSNs(w))

	// Next append preserves monotonic, gapless LSNs.
	lsn, err := w.Append(context.Background(), payloadForLSN(5))
	s.Require().NoError(err)
	s.Require().Equal(uint64(5), lsn)
	s.Require().Equal([]uint64{5}, s.replayLSNs(w))
}

func (s *CutSuite) TestCutSurvivesReopen() {
	w := s.openOneRecordPerSegment()
	s.appendN(w, 5)
	s.Require().NoError(w.CutOffset(3))
	s.Require().NoError(w.Close())

	reopened, report, err := Open(s.dir)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(reopened.Close()) }()

	s.Require().Equal(uint64(3), report.FirstLSN)
	s.Require().Equal(uint64(5), report.LastLSN)
	s.Require().Equal([]uint64{3, 4, 5}, s.replayLSNs(reopened))
}

func (s *CutSuite) TestCutEverythingThenReopen() {
	w := s.openOneRecordPerSegment()
	s.appendN(w, 4)
	s.Require().NoError(w.CutOffset(1000)) // delete everything; active becomes seg-<5>
	s.Require().NoError(w.Close())

	reopened, _, err := Open(s.dir)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(reopened.Close()) }()

	s.Require().Empty(s.replayLSNs(reopened), "no records survive the cut across a reopen")

	// The next append must NOT reuse a deleted LSN; it continues at nextLSN (5).
	lsn, err := reopened.Append(context.Background(), payloadForLSN(5))
	s.Require().NoError(err)
	s.Require().Equal(uint64(5), lsn, "LSN must never be reused after cut-everything + reopen")
	s.Require().Equal(uint64(5), reopened.FirstLSN())
	s.Require().Equal(uint64(5), reopened.LastLSN())
	s.Require().Equal([]uint64{5}, s.replayLSNs(reopened))
}

func (s *CutSuite) TestCutAfterCloseFails() {
	w, _, err := Open(s.dir)
	s.Require().NoError(err)
	s.Require().NoError(w.Close())

	s.Require().ErrorIs(w.CutOffset(1), ErrClosed)
}

func (s *CutSuite) TestCutHonorsCanceledContext() {
	w, _, err := Open(s.dir, WithSyncPolicy(SyncImmediate))
	s.Require().NoError(err)
	defer func() { s.Require().NoError(w.Close()) }()

	s.appendN(w, 2)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s.Require().ErrorIs(w.CutOffsetContext(ctx, 2), context.Canceled)
}

func (s *CutSuite) TestCutToleratesAlreadyDeletedBelowSegment() {
	w := s.openOneRecordPerSegment() // one record per segment: bases 1..5 (5 = active)
	defer func() { s.Require().NoError(w.Close()) }()

	s.appendN(w, 5)

	// Simulate a cut/truncate that failed partway: segment 1's file is already
	// gone while the in-memory index still lists it.
	s.Require().NoError(os.Remove(filepath.Join(s.dir, segment.Name(1))))

	// A cut that must delete the already-gone segment 1 (and segment 2) still
	// succeeds instead of wedging on a not-exist error.
	s.Require().NoError(w.CutOffset(3))
	s.Require().Equal(uint64(3), w.FirstLSN())
	s.Require().Equal([]uint64{3, 4, 5}, s.replayLSNs(w))
}

func (s *CutSuite) TestCutEverythingToleratesLeftoverActiveSegment() {
	w := s.openOneRecordPerSegment() // bases 1..4 (4 = active), nextLSN = 5
	defer func() { s.Require().NoError(w.Close()) }()

	s.appendN(w, 4)

	// Simulate a previously interrupted cut-everything: an empty seg-5 (base =
	// nextLSN) already exists on disk, not yet folded into the index.
	root, err := os.OpenRoot(s.dir)
	s.Require().NoError(err)
	leftover, err := segment.Create(root, 5)
	s.Require().NoError(err)
	s.Require().NoError(leftover.Close())
	s.Require().NoError(root.Close())

	// cut-everything replaces the leftover rather than wedging on an exclusive create.
	s.Require().NoError(w.CutOffset(1000))
	s.Require().Equal(uint64(0), w.FirstLSN())

	lsn, err := w.Append(context.Background(), payloadForLSN(5))
	s.Require().NoError(err)
	s.Require().Equal(uint64(5), lsn, "appends continue gaplessly at nextLSN")
}

func (s *CutSuite) TestConcurrentReaderDuringCutIsSafe() {
	w := s.openOneRecordPerSegment()
	defer func() { s.Require().NoError(w.Close()) }()

	s.appendN(w, 20)

	done := make(chan struct{})
	go func() {
		defer close(done)

		for range 50 {
			reader, err := w.NewReader(0)
			if err != nil {
				return
			}

			for reader.Next() {
				_ = reader.Entry()
			}
			// A concurrent cut may raise firstLSN; the reader must never corrupt.
			s.Assert().NoError(reader.Err())
			_ = reader.Close()
		}
	}()

	for cut := uint64(2); cut <= 15; cut++ {
		s.Require().NoError(w.CutOffset(cut))
	}

	<-done

	s.Require().Equal(uint64(15), w.FirstLSN())
	s.Require().Equal(uint64(20), w.LastLSN())
}
