package wal

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

// NotifySuite covers the writer-goroutine commit-notification primitives.
type NotifySuite struct {
	suite.Suite
	dir string
}

func TestNotifySuite(t *testing.T) {
	suite.Run(t, new(NotifySuite))
}

func (s *NotifySuite) SetupTest() {
	s.dir = s.T().TempDir()
}

func (s *NotifySuite) open() *WAL {
	s.T().Helper()

	w, _, err := Open(s.dir, WithSyncPolicy(SyncImmediate))
	s.Require().NoError(err)

	return w
}

// A caught-up waiter is woken when a later append commits.
func (s *NotifySuite) TestAwaitCommitWokenByAppend() {
	w := s.open()
	defer func() { _ = w.Close() }()

	woken := make(chan commitWait, 1)
	go func() { woken <- w.awaitCommit(context.Background(), w.LastLSN()) }()

	time.Sleep(20 * time.Millisecond) // let the waiter subscribe
	_, err := w.Append(context.Background(), payloadForLSN(1))
	s.Require().NoError(err)

	select {
	case result := <-woken:
		s.Assert().Equal(commitWoken, result)
	case <-time.After(time.Second):
		s.Fail("awaitCommit was not woken by a commit")
	}
}

// A waiter already behind the committed LSN is woken immediately (no lost wakeup).
func (s *NotifySuite) TestAwaitCommitWokenImmediatelyWhenBehind() {
	w := s.open()
	defer func() { _ = w.Close() }()

	_, err := w.Append(context.Background(), payloadForLSN(1))
	s.Require().NoError(err)

	result := w.awaitCommit(context.Background(), 0) // from=0, committed=1
	s.Assert().Equal(commitWoken, result)
}

// A canceled context releases the waiter.
func (s *NotifySuite) TestAwaitCommitCanceled() {
	w := s.open()
	defer func() { _ = w.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result := w.awaitCommit(ctx, w.LastLSN())
	s.Assert().Equal(commitCanceled, result)
}

// A parked waiter is released (woken) when the WAL closes; the follower then
// re-subscribes and learns of closure via commitClosed.
func (s *NotifySuite) TestAwaitCommitReleasedOnShutdown() {
	w := s.open()

	woken := make(chan commitWait, 1)
	go func() { woken <- w.awaitCommit(context.Background(), w.LastLSN()) }()

	time.Sleep(20 * time.Millisecond)
	s.Require().NoError(w.Close())

	select {
	case result := <-woken:
		s.Assert().Equal(commitWoken, result)
	case <-time.After(time.Second):
		s.Fail("awaitCommit was not released by Close")
	}
}
