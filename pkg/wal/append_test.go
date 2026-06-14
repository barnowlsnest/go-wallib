package wal

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
)

// realistic append payloads.
var (
	appendSetCommand    = []byte(`{"op":"set","key":"account:1001","value":{"balance":250000,"currency":"USD"}}`)
	appendDeleteCommand = []byte(`{"op":"delete","key":"session:9f3c1a7e-7b2d-4e51-bc44-6d2a0f1e88aa"}`)
	appendSnapshotChunk = bytes.Repeat([]byte("snapshot-chunk;"), 8) // ~120 bytes
)

// AppendSuite covers Append/AppendBatch, record-size limits, close behavior, and
// segment rolling.
type AppendSuite struct {
	suite.Suite
	dir string
}

func TestAppendSuite(t *testing.T) {
	suite.Run(t, new(AppendSuite))
}

func (s *AppendSuite) SetupTest() {
	s.dir = s.T().TempDir()
}

func (s *AppendSuite) open(opts ...Option) *WAL {
	w, _, err := Open(s.dir, opts...)
	s.Require().NoError(err)

	return w
}

func (s *AppendSuite) TestAppendAssignsSequentialLSNs() {
	w := s.open(WithSyncPolicy(SyncImmediate))
	defer func() { s.Require().NoError(w.Close()) }()

	payloads := [][]byte{appendSetCommand, appendDeleteCommand, appendSnapshotChunk, appendSetCommand, appendDeleteCommand}
	for i, payload := range payloads {
		lsn, err := w.Append(context.Background(), payload)
		s.Require().NoError(err)
		s.Require().Equal(uint64(i+1), lsn, "LSNs must be sequential and gapless")
	}

	s.Require().Equal(uint64(1), w.FirstLSN())
	s.Require().Equal(uint64(5), w.LastLSN())
}

func (s *AppendSuite) TestAppendBatchAssignsContiguousLSNs() {
	w := s.open(WithSyncPolicy(SyncImmediate))
	defer func() { s.Require().NoError(w.Close()) }()

	assignedLSNs, err := w.AppendBatch(context.Background(), [][]byte{appendSetCommand, appendDeleteCommand})
	s.Require().NoError(err)
	s.Require().Equal([]uint64{1, 2}, assignedLSNs)
	s.Require().Equal(uint64(2), w.LastLSN())
}

func (s *AppendSuite) TestAppendRejectsTooLarge() {
	w := s.open(WithMaxRecordSize(8))
	defer func() { s.Require().NoError(w.Close()) }()

	_, err := w.Append(context.Background(), make([]byte, 9))
	s.Require().ErrorIs(err, ErrRecordTooLarge)
}

func (s *AppendSuite) TestAppendAfterCloseFails() {
	w := s.open()
	s.Require().NoError(w.Close())

	_, err := w.Append(context.Background(), appendSetCommand)
	s.Require().ErrorIs(err, ErrClosed)
}

func (s *AppendSuite) TestAppendRollsSegment() {
	// Segment threshold small enough that each record forces a roll.
	w := s.open(WithSyncPolicy(SyncImmediate), WithMaxSegmentSize(128))
	defer func() { s.Require().NoError(w.Close()) }()

	for range 10 {
		_, err := w.Append(context.Background(), appendSetCommand)
		s.Require().NoError(err)
	}

	s.Require().GreaterOrEqual(w.segmentCount(), 2, "appends must roll into multiple segments")
	s.Require().Equal(uint64(10), w.LastLSN())
}

func (s *AppendSuite) TestOversizedRecordGetsOwnSegment() {
	w := s.open(WithSyncPolicy(SyncImmediate), WithMaxSegmentSize(64), WithMaxRecordSize(1<<20))
	defer func() { s.Require().NoError(w.Close()) }()

	_, err := w.Append(context.Background(), appendDeleteCommand)
	s.Require().NoError(err)

	lsn, err := w.Append(context.Background(), appendSnapshotChunk)
	s.Require().NoError(err)
	s.Require().Equal(uint64(2), lsn)
	s.Require().Equal(2, w.segmentCount(), "a record over the soft threshold rolls onto its own segment")
}

func (s *AppendSuite) TestAppendPersistsAcrossReopen() {
	w := s.open(WithSyncPolicy(SyncImmediate))
	_, err := w.AppendBatch(context.Background(), [][]byte{appendSetCommand, appendDeleteCommand, appendSnapshotChunk})
	s.Require().NoError(err)
	s.Require().NoError(w.Close())

	reopened, report, err := Open(s.dir)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(reopened.Close()) }()

	s.Require().Equal(uint64(3), report.EntriesRecovered)
	s.Require().Equal(uint64(1), report.FirstLSN)
	s.Require().Equal(uint64(3), report.LastLSN)
}
