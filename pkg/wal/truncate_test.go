package wal

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
)

// TruncateSuite covers low-water-mark segment reclamation.
type TruncateSuite struct {
	suite.Suite
	dir string
}

func TestTruncateSuite(t *testing.T) {
	suite.Run(t, new(TruncateSuite))
}

func (s *TruncateSuite) SetupTest() {
	s.dir = s.T().TempDir()
}

// openWithOneRecordPerSegment opens a WAL whose segment threshold is small
// enough that every appended record rolls into its own segment, giving precise
// control over segment boundaries.
func (s *TruncateSuite) openWithOneRecordPerSegment() *WAL {
	s.T().Helper()

	w, _, err := Open(s.dir, WithSyncPolicy(SyncImmediate), WithMaxSegmentSize(40))
	s.Require().NoError(err)

	return w
}

func (s *TruncateSuite) appendN(w *WAL, count int) {
	s.T().Helper()

	for i := 1; i <= count; i++ {
		_, err := w.Append(context.Background(), payloadForLSN(uint64(i)))
		s.Require().NoError(err)
	}
}

func (s *TruncateSuite) TestDeletesWholeObsoleteSegments() {
	w := s.openWithOneRecordPerSegment()
	defer func() { s.Require().NoError(w.Close()) }()

	s.appendN(w, 6)
	before := w.segmentCount()
	s.Require().GreaterOrEqual(before, 3, "need several segments to exercise reclamation")

	s.Require().NoError(w.Truncate(4))

	s.Require().Less(w.segmentCount(), before, "obsolete segments must be reclaimed")
	s.Require().Positive(w.FirstLSN())
	s.Require().LessOrEqual(w.FirstLSN(), uint64(4))

	var replayed []uint64
	err := w.Replay(0, func(entry Entry) error {
		replayed = append(replayed, entry.LSN)

		return nil
	})
	s.Require().NoError(err)
	s.Require().Equal(uint64(6), replayed[len(replayed)-1], "surviving entries still replay to the end")
}

func (s *TruncateSuite) TestNeverDeletesActiveSegment() {
	w, _, err := Open(s.dir, WithSyncPolicy(SyncImmediate))
	s.Require().NoError(err)
	defer func() { s.Require().NoError(w.Close()) }()

	s.appendN(w, 3) // all in a single (active) segment

	s.Require().NoError(w.Truncate(1000)) // far beyond everything

	s.Require().Equal(1, w.segmentCount(), "the active segment must survive")
	s.Require().Equal(uint64(3), w.LastLSN())
}

func (s *TruncateSuite) TestBelowFirstSegmentIsNoop() {
	w := s.openWithOneRecordPerSegment()
	defer func() { s.Require().NoError(w.Close()) }()

	s.appendN(w, 4)
	before := w.segmentCount()
	firstBefore := w.FirstLSN()

	s.Require().NoError(w.Truncate(1)) // nothing is fully below LSN 1

	s.Require().Equal(before, w.segmentCount())
	s.Require().Equal(firstBefore, w.FirstLSN())
}

func (s *TruncateSuite) TestTruncateAfterCloseFails() {
	w, _, err := Open(s.dir)
	s.Require().NoError(err)
	s.Require().NoError(w.Close())

	s.Require().ErrorIs(w.Truncate(1), ErrClosed)
}
