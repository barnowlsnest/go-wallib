package wal

import (
	"context"
	"iter"
	"testing"
	"time"

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

func (s *FollowerSuite) appendSequential(count int, opts ...Option) *WAL {
	s.T().Helper()

	w, _, err := Open(s.dir, append([]Option{WithSyncPolicy(SyncImmediate)}, opts...)...)
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

// drainRecords exhausts a follower's Records loop in a background goroutine,
// closing the returned channel when the loop ends. Used by the tests that assert
// how a follow loop terminates (ctx cancel, Close, WAL close).
func (s *FollowerSuite) drainRecords(ctx context.Context, follower *Follower) <-chan struct{} {
	s.T().Helper()

	done := make(chan struct{})
	go func() {
		defer close(done)

		for lsn := range follower.Records(ctx) {
			_ = lsn
		}
	}()

	return done
}

// Follow mode delivers records appended after the follower caught up.
func (s *FollowerSuite) TestFollowReceivesLiveAppends() {
	w := s.appendSequential(2)
	defer func() { _ = w.Close() }()

	follower, err := w.Follower(0, WithFollow())
	s.Require().NoError(err)
	defer func() { _ = follower.Close() }()

	received := make(chan uint64, 5)
	go func() {
		for lsn := range follower.Records(context.Background()) {
			received <- lsn
		}
	}()

	// First the two existing records, then live appends 3..5.
	for i := 3; i <= 5; i++ {
		_, appendErr := w.Append(context.Background(), payloadForLSN(uint64(i)))
		s.Require().NoError(appendErr)
	}

	got := make([]uint64, 0, 5)
	for len(got) < 5 {
		select {
		case lsn := <-received:
			got = append(got, lsn)
		case <-time.After(2 * time.Second):
			s.Fail("did not receive all live appends", "got %v", got)

			return
		}
	}
	s.Assert().Equal([]uint64{1, 2, 3, 4, 5}, got)
}

// Multiple independent followers from different start LSNs each see their suffix.
func (s *FollowerSuite) TestMultipleIndependentFollowers() {
	w := s.appendSequential(3)
	defer func() { _ = w.Close() }()

	collect := func(fromLSN uint64) <-chan uint64 {
		follower, err := w.Follower(fromLSN, WithFollow())
		s.Require().NoError(err)

		out := make(chan uint64, 10)
		go func() {
			defer func() { _ = follower.Close() }()
			for lsn := range follower.Records(context.Background()) {
				out <- lsn
				if lsn == 5 {
					return
				}
			}
		}()

		return out
	}

	fromOne := collect(1)
	fromThree := collect(3)

	for i := 4; i <= 5; i++ {
		_, err := w.Append(context.Background(), payloadForLSN(uint64(i)))
		s.Require().NoError(err)
	}

	drain := func(out <-chan uint64) []uint64 {
		var got []uint64
		for {
			select {
			case lsn := <-out:
				got = append(got, lsn)
				if lsn == 5 {
					return got
				}
			case <-time.After(2 * time.Second):
				s.Fail("follower stalled", "got %v", got)

				return got
			}
		}
	}

	s.Assert().Equal([]uint64{1, 2, 3, 4, 5}, drain(fromOne))
	s.Assert().Equal([]uint64{3, 4, 5}, drain(fromThree))
}

// Canceling the context cleanly ends a follow loop; Err reports the cause.
func (s *FollowerSuite) TestFollowContextCancelEndsCleanly() {
	w := s.appendSequential(1)
	defer func() { _ = w.Close() }()

	follower, err := w.Follower(0, WithFollow())
	s.Require().NoError(err)
	defer func() { _ = follower.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	done := s.drainRecords(ctx, follower)

	time.Sleep(50 * time.Millisecond) // let it catch up and block at the tail
	cancel()

	select {
	case <-done:
		s.Assert().ErrorIs(follower.Err(), context.Canceled)
	case <-time.After(2 * time.Second):
		s.Fail("follow loop did not end on context cancel")
	}
}

// Follower.Close ends a blocked follow loop with no error.
func (s *FollowerSuite) TestFollowCloseEndsLoop() {
	w := s.appendSequential(1)
	defer func() { _ = w.Close() }()

	follower, err := w.Follower(0, WithFollow())
	s.Require().NoError(err)

	done := s.drainRecords(context.Background(), follower)

	time.Sleep(50 * time.Millisecond)
	s.Require().NoError(follower.Close())

	select {
	case <-done:
		s.Assert().NoError(follower.Err())
	case <-time.After(2 * time.Second):
		s.Fail("follow loop did not end on Close")
	}
}

// Closing the WAL wakes a blocked follower with ErrClosed.
func (s *FollowerSuite) TestFollowWALCloseReturnsErrClosed() {
	w := s.appendSequential(1)

	follower, err := w.Follower(0, WithFollow())
	s.Require().NoError(err)
	defer func() { _ = follower.Close() }()

	done := s.drainRecords(context.Background(), follower)

	time.Sleep(50 * time.Millisecond)
	s.Require().NoError(w.Close())

	select {
	case <-done:
		s.Assert().ErrorIs(follower.Err(), ErrClosed)
	case <-time.After(2 * time.Second):
		s.Fail("follow loop did not end on WAL Close")
	}
}

// RecordsChan delivers a snapshot's entries by value and closes at the tail.
func (s *FollowerSuite) TestRecordsChanSnapshot() {
	w := s.appendSequential(3)
	defer func() { _ = w.Close() }()

	follower, err := w.Follower(0)
	s.Require().NoError(err)
	defer func() { _ = follower.Close() }()

	var got []uint64
	for entry := range follower.RecordsChan(context.Background()) {
		got = append(got, entry.LSN)
		s.Assert().Equal(payloadForLSN(entry.LSN), entry.Payload)
	}
	s.Require().NoError(follower.Err())
	s.Assert().Equal([]uint64{1, 2, 3}, got)
}

// RecordsChan composes with select against another channel in follow mode.
func (s *FollowerSuite) TestRecordsChanSelectInFollowMode() {
	w := s.appendSequential(1)
	defer func() { _ = w.Close() }()

	follower, err := w.Follower(0, WithFollow())
	s.Require().NoError(err)
	defer func() { _ = follower.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entries := follower.RecordsChan(ctx)

	_, err = w.Append(context.Background(), payloadForLSN(2))
	s.Require().NoError(err)

	other := make(chan struct{})
	got := make([]uint64, 0, 2)
	for len(got) < 2 {
		select {
		case entry := <-entries:
			got = append(got, entry.LSN)
		case <-other:
			s.Fail("unexpected other event")
		case <-time.After(2 * time.Second):
			s.Fail("RecordsChan stalled", "got %v", got)

			return
		}
	}
	s.Assert().Equal([]uint64{1, 2}, got)
}

// Closing the follower stops the bridge goroutine even when blocked on send.
func (s *FollowerSuite) TestRecordsChanCloseStopsBridge() {
	w := s.appendSequential(1)
	defer func() { _ = w.Close() }()

	follower, err := w.Follower(0, WithFollow())
	s.Require().NoError(err)

	// Never receive from the channel, forcing the bridge to block on send.
	entries := follower.RecordsChan(context.Background())
	_, err = w.Append(context.Background(), payloadForLSN(2))
	s.Require().NoError(err)

	time.Sleep(50 * time.Millisecond) // let the bridge block on a send
	s.Require().NoError(follower.Close())

	// The channel must eventually close (drain whatever is buffered/in flight).
	closed := make(chan struct{})
	go func() {
		defer close(closed)

		for entry := range entries {
			_ = entry
		}
	}()

	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		s.Fail("RecordsChan did not close after Follower.Close")
	}
}

// A follower lapped by Truncate stops with ErrTruncated.
func (s *FollowerSuite) TestFollowTruncatedPosition() {
	// Tiny segments so each record rolls into its own segment, making whole
	// segments reclaimable by Truncate.
	w := s.appendSequential(5, WithMaxSegmentSize(1))
	defer func() { _ = w.Close() }()

	// Consume the first record, then stop following.
	follower, err := w.Follower(1, WithFollow())
	s.Require().NoError(err)
	defer func() { _ = follower.Close() }()

	next, stop := iter.Pull2(follower.Records(context.Background()))
	lsn, _, ok := next()
	s.Require().True(ok)
	s.Require().Equal(uint64(1), lsn)

	// Reclaim everything below LSN 5 while the follower sits at LSN 1.
	s.Require().NoError(w.Truncate(5))

	// Appending advances the tail and wakes the follower, which finds its next
	// LSN reclaimed.
	_, err = w.Append(context.Background(), payloadForLSN(6))
	s.Require().NoError(err)

	for {
		_, _, ok = next()
		if !ok {
			break
		}
	}
	stop()
	s.Assert().ErrorIs(follower.Err(), ErrTruncated)
}
