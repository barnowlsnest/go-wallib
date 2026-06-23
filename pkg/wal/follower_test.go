package wal

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
)

// FollowerSuite covers the Follower stream (snapshot and follow modes).
type FollowerSuite struct {
	suite.Suite
	dir string
}

func TestFollowerSuite(t *testing.T) {
	suite.Run(t, new(FollowerSuite))
}

func (s *FollowerSuite) SetupTest() {
	s.dir = s.T().TempDir()
}

func (s *FollowerSuite) appendSequential(count int) *WAL {
	s.T().Helper()

	w, _, err := Open(s.dir, WithSyncPolicy(SyncImmediate))
	s.Require().NoError(err)

	for i := 1; i <= count; i++ {
		_, appendErr := w.Append(context.Background(), payloadForLSN(uint64(i)))
		s.Require().NoError(appendErr)
	}

	return w
}

// collectSnapshot drains a snapshot-mode follower into ordered (lsn, payload) pairs.
func (s *FollowerSuite) collectSnapshot(follower *Follower) (lsns []uint64, payloads [][]byte) {
	s.T().Helper()

	for lsn, payload := range follower.Records(context.Background()) {
		lsns = append(lsns, lsn)
		payloads = append(payloads, payload)
	}
	s.Require().NoError(follower.Err())

	return lsns, payloads
}

func (s *FollowerSuite) TestSnapshotFromBeginning() {
	w := s.appendSequential(5)
	defer func() { _ = w.Close() }()

	follower, err := w.Follower(0)
	s.Require().NoError(err)
	defer func() { _ = follower.Close() }()

	lsns, payloads := s.collectSnapshot(follower)
	s.Require().Equal([]uint64{1, 2, 3, 4, 5}, lsns)
	for i, lsn := range lsns {
		s.Assert().Equal(payloadForLSN(lsn), payloads[i])
	}
}

func (s *FollowerSuite) TestSnapshotFromMidLSN() {
	w := s.appendSequential(5)
	defer func() { _ = w.Close() }()

	follower, err := w.Follower(3)
	s.Require().NoError(err)
	defer func() { _ = follower.Close() }()

	lsns, _ := s.collectSnapshot(follower)
	s.Assert().Equal([]uint64{3, 4, 5}, lsns)
}

// Snapshot mode does not observe records appended after Follower() is called.
func (s *FollowerSuite) TestSnapshotIgnoresLaterAppends() {
	w := s.appendSequential(3)
	defer func() { _ = w.Close() }()

	follower, err := w.Follower(0)
	s.Require().NoError(err)
	defer func() { _ = follower.Close() }()

	_, err = w.Append(context.Background(), payloadForLSN(4))
	s.Require().NoError(err)

	lsns, _ := s.collectSnapshot(follower)
	s.Assert().Equal([]uint64{1, 2, 3}, lsns)
}

func (s *FollowerSuite) TestFollowerOnClosedWAL() {
	w := s.appendSequential(1)
	s.Require().NoError(w.Close())

	_, err := w.Follower(0)
	s.Require().ErrorIs(err, ErrClosed)
}

// The payload handed to the loop is a fresh copy, valid after the iteration.
func (s *FollowerSuite) TestSnapshotPayloadIsFreshCopy() {
	w := s.appendSequential(2)
	defer func() { _ = w.Close() }()

	follower, err := w.Follower(0)
	s.Require().NoError(err)
	defer func() { _ = follower.Close() }()

	var retained [][]byte
	for _, payload := range follower.Records(context.Background()) {
		retained = append(retained, payload)
	}
	s.Require().NoError(follower.Err())
	s.Require().Len(retained, 2)
	s.Assert().Equal(payloadForLSN(1), retained[0])
	s.Assert().Equal(payloadForLSN(2), retained[1])
}
