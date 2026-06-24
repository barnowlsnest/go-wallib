package wal

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"
)

// ExampleSuite is a runnable, narrated walkthrough of the WAL's three core
// workflows — writing, reading, and crash recovery — written so the test log
// reads top-to-bottom as documentation. Run it verbosely to follow along:
//
//	go test ./pkg/wal -run TestExampleSuite -v
type ExampleSuite struct {
	suite.Suite
	dir string
}

func TestExampleSuite(t *testing.T) {
	suite.Run(t, new(ExampleSuite))
}

func (s *ExampleSuite) SetupTest() {
	// Each scenario gets its own temp directory; the test framework removes it
	// automatically when the test finishes.
	s.dir = s.T().TempDir()
	s.T().Logf("WAL directory: %s", s.dir)
}

// TestWriteThenRead demonstrates the happy path: open a log, durably append a
// handful of records, then stream them back in order with a Reader.
func (s *ExampleSuite) TestWriteThenRead() {
	s.T().Log("--- write ---")

	// SyncImmediate fsyncs before each Append returns, so every record below is
	// durable on disk the moment Append hands back its LSN.
	w, report, err := Open(s.dir, WithSyncPolicy(SyncImmediate))
	s.Require().NoError(err)
	s.T().Logf("opened fresh log: recovered %d entries", report.EntriesRecovered)

	commands := [][]byte{
		[]byte(`{"op":"set","key":"account:42","value":"100.00"}`),
		[]byte(`{"op":"set","key":"account:7","value":"55.50"}`),
		[]byte(`{"op":"delete","key":"account:42"}`),
	}

	for _, command := range commands {
		lsn, appendErr := w.Append(context.Background(), command)
		s.Require().NoError(appendErr)
		s.T().Logf("appended LSN %d: %s", lsn, command)
	}

	s.T().Logf("log now spans LSN %d..%d", w.FirstLSN(), w.LastLSN())
	s.Require().NoError(w.Close())

	s.T().Log("--- read ---")

	// Reopen and replay everything from the beginning (fromLSN 0). A real
	// consumer would rebuild its in-memory state by applying each command here.
	reopened, _, err := Open(s.dir)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(reopened.Close()) }()

	var replayed int
	err = reopened.Replay(0, func(entry Entry) error {
		replayed++
		s.T().Logf("replayed LSN %d: %s", entry.LSN, entry.Payload)

		return nil
	})
	s.Require().NoError(err)
	s.Require().Equal(len(commands), replayed)
}

// TestRecoveryAfterReopen demonstrates crash recovery: a log written in one
// "process lifetime" is reopened in the next, and appends continue gaplessly
// from where they left off.
func (s *ExampleSuite) TestRecoveryAfterReopen() {
	s.T().Log("--- first run: write and shut down ---")

	first, _, err := Open(s.dir, WithSyncPolicy(SyncImmediate))
	s.Require().NoError(err)

	for i := uint64(1); i <= 3; i++ {
		lsn, appendErr := first.Append(context.Background(), payloadForLSN(i))
		s.Require().NoError(appendErr)
		s.T().Logf("appended LSN %d", lsn)
	}

	// Close stands in for a clean shutdown. (An unclean crash leaves a torn tail
	// on disk, which Open detects and truncates during recovery.)
	s.Require().NoError(first.Close())
	s.T().Log("closed the log (process exits)")

	s.T().Log("--- second run: recover and resume ---")

	second, report, err := Open(s.dir)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(second.Close()) }()

	s.T().Logf(
		"recovery report: entries=%d firstLSN=%d lastLSN=%d bytesTruncated=%d segmentsRemoved=%d",
		report.EntriesRecovered,
		report.FirstLSN,
		report.LastLSN,
		report.BytesTruncated,
		report.SegmentsRemoved,
	)
	s.Require().Equal(uint64(3), report.EntriesRecovered)

	// Recovery restored the writer's LSN cursor, so the next append continues at
	// LSN 4 with no gap.
	lsn, err := second.Append(context.Background(), payloadForLSN(4))
	s.Require().NoError(err)
	s.T().Logf("resumed appending at LSN %d", lsn)
	s.Require().Equal(uint64(4), lsn)
	s.Require().Equal(uint64(1), second.FirstLSN())
	s.Require().Equal(uint64(4), second.LastLSN())
}

func ExampleFollower_follow() {
	dir, err := os.MkdirTemp("", "wal-follow-*")
	if err != nil {
		panic(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	w, _, err := Open(dir, WithSyncPolicy(SyncImmediate))
	if err != nil {
		panic(err)
	}
	defer func() { _ = w.Close() }()

	// Two records are already committed before we start following.
	for i := 1; i <= 2; i++ {
		if _, appendErr := w.Append(context.Background(), payloadForLSN(uint64(i))); appendErr != nil {
			panic(appendErr)
		}
	}

	follower, err := w.Follower(0, WithFollow())
	if err != nil {
		panic(err)
	}
	defer func() { _ = follower.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// In follow mode the follower also delivers records committed AFTER it was
	// created (a snapshot reader would stop at LSN 2). Append two more.
	for i := 3; i <= 4; i++ {
		if _, appendErr := w.Append(ctx, payloadForLSN(uint64(i))); appendErr != nil {
			panic(appendErr)
		}
	}

	// The loop blocks at the tail and resumes as new records commit; stop after
	// the fourth so the example terminates.
	for lsn, payload := range follower.Records(ctx) {
		fmt.Printf("LSN %d: %s\n", lsn, payload)

		if lsn == 4 {
			break
		}
	}
	// Output:
	// LSN 1: {"op":"set","key":"row:1","seq":1}
	// LSN 2: {"op":"set","key":"row:2","seq":2}
	// LSN 3: {"op":"set","key":"row:3","seq":3}
	// LSN 4: {"op":"set","key":"row:4","seq":4}
}
