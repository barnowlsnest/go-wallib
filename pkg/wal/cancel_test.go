package wal

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
)

// CancelSuite verifies the context-cancellation contract (spec §11): a canceled
// append is never acknowledged and never advances the log.
type CancelSuite struct {
	suite.Suite
	dir string
}

func TestCancelSuite(t *testing.T) {
	suite.Run(t, new(CancelSuite))
}

func (s *CancelSuite) SetupTest() {
	s.dir = s.T().TempDir()
}

func (s *CancelSuite) open() *WAL {
	s.T().Helper()

	w, _, err := Open(s.dir, WithSyncPolicy(SyncImmediate))
	s.Require().NoError(err)

	return w
}

func (s *CancelSuite) TestAppendWithCancelledContextIsNotWritten() {
	w := s.open()
	defer func() { s.Require().NoError(w.Close()) }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // canceled before the call

	_, err := w.Append(ctx, appendSetCommand)
	s.Require().ErrorIs(err, context.Canceled)
	s.Require().Equal(uint64(0), w.LastLSN(), "nothing may be committed for a canceled append")
}

func (s *CancelSuite) TestAppendBatchWithCancelledContextIsNotWritten() {
	w := s.open()
	defer func() { s.Require().NoError(w.Close()) }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := w.AppendBatch(ctx, [][]byte{appendSetCommand, appendDeleteCommand})
	s.Require().ErrorIs(err, context.Canceled)
	s.Require().Equal(uint64(0), w.LastLSN())
}

func (s *CancelSuite) TestAppendSucceedsWithLiveContext() {
	w := s.open()
	defer func() { s.Require().NoError(w.Close()) }()

	lsn, err := w.Append(context.Background(), appendSetCommand)
	s.Require().NoError(err)
	s.Require().Equal(uint64(1), lsn)
}

func (s *CancelSuite) TestCancelledAppendDoesNotConsumeAnLSN() {
	w := s.open()
	defer func() { s.Require().NoError(w.Close()) }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := w.Append(ctx, appendSetCommand)
	s.Require().ErrorIs(err, context.Canceled)

	// A subsequent live append must still receive LSN 1 — the canceled attempt
	// neither advanced nextLSN nor left a gap.
	lsn, err := w.Append(context.Background(), appendDeleteCommand)
	s.Require().NoError(err)
	s.Require().Equal(uint64(1), lsn)
	s.Require().Equal(uint64(1), w.LastLSN())
}
