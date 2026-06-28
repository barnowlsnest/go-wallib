package wal

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/barnowlsnest/go-wallib/internal/segment"
)

// LoggingSuite verifies that a custom Logger receives the Debug/Info events the
// WAL emits during recovery, segment rolling, and truncation.
type LoggingSuite struct {
	suite.Suite
	dir string
}

func TestLoggingSuite(t *testing.T) {
	suite.Run(t, new(LoggingSuite))
}

func (s *LoggingSuite) SetupTest() {
	s.dir = s.T().TempDir()
}

// spyLogger returns a mockLogger that accepts every level, so the WAL can log
// freely without an unexpected-call panic; individual tests assert the specific
// messages they care about via AssertCalled.
func (s *LoggingSuite) spyLogger() *mockLogger {
	s.T().Helper()

	spy := new(mockLogger)
	spy.On("Debug", mock.Anything, mock.Anything).Maybe()
	spy.On("Info", mock.Anything, mock.Anything).Maybe()
	spy.On("Warn", mock.Anything, mock.Anything).Maybe()
	spy.On("Error", mock.Anything, mock.Anything).Maybe()

	return spy
}

// seedSegment writes a sealed segment directly via the segment layer, simulating
// a log left behind by a previous process.
func (s *LoggingSuite) seedSegment(baseLSN uint64, payloads ...[]byte) {
	s.T().Helper()

	root, err := os.OpenRoot(s.dir)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(root.Close()) }()

	seg, err := segment.Create(root, baseLSN)
	s.Require().NoError(err)

	lsn := baseLSN
	for _, payload := range payloads {
		_, appendErr := seg.Append(lsn, payload)
		s.Require().NoError(appendErr)
		lsn++
	}

	s.Require().NoError(seg.Sync())
	s.Require().NoError(seg.Close())
}

// seedTornTail writes two records, then chops the last few bytes off the second
// so recovery must truncate a partial record.
func (s *LoggingSuite) seedTornTail() {
	s.T().Helper()

	root, err := os.OpenRoot(s.dir)
	s.Require().NoError(err)

	seg, err := segment.Create(root, 1)
	s.Require().NoError(err)

	_, err = seg.Append(1, payloadForLSN(1))
	s.Require().NoError(err)

	_, err = seg.Append(2, payloadForLSN(2))
	s.Require().NoError(err)
	fullSize := seg.Size()
	s.Require().NoError(seg.Sync())

	s.Require().NoError(seg.TruncateTo(fullSize - 3))
	s.Require().NoError(seg.Close())
	s.Require().NoError(root.Close())
}

// fieldEquals reports whether fields carry key with the given value.
func fieldEquals(fields []Field, key string, value any) bool {
	for _, field := range fields {
		if field.Key == key && field.Value == value {
			return true
		}
	}

	return false
}

func (s *LoggingSuite) TestRecoveryLogsInfo() {
	cases := []struct {
		setup   func()
		message string
	}{
		{func() {}, "wal: created new empty log"},
		{func() { s.seedSegment(1, walSetCommand, walDeleteCommand) }, "wal: recovery complete"},
		{s.seedTornTail, "wal: truncated torn tail on recovery"},
	}

	for _, tc := range cases {
		s.Run(tc.message, func() {
			s.dir = s.T().TempDir()
			tc.setup()

			spy := s.spyLogger()
			w, _, err := Open(s.dir, WithLogger(spy))
			s.Require().NoError(err)
			s.Require().NoError(w.Close())

			spy.AssertCalled(s.T(), "Info", tc.message, mock.Anything)
		})
	}
}

func (s *LoggingSuite) TestRollLogsDebug() {
	spy := s.spyLogger()

	// A 40-byte soft threshold forces every record into its own segment.
	w, _, err := Open(s.dir, WithSyncPolicy(SyncImmediate), WithMaxSegmentSize(40), WithLogger(spy))
	s.Require().NoError(err)
	defer func() { s.Require().NoError(w.Close()) }()

	for lsn := uint64(1); lsn <= 3; lsn++ {
		_, appendErr := w.Append(context.Background(), payloadForLSN(lsn))
		s.Require().NoError(appendErr)
	}

	s.Require().Greater(w.segmentCount(), 1, "appends must have rolled new segments")

	carriesNewBase := mock.MatchedBy(func(fields []Field) bool {
		return fieldEquals(fields, "newBaseLSN", uint64(2))
	})
	spy.AssertCalled(s.T(), "Debug", "wal: rolled to new segment", carriesNewBase)
}

func (s *LoggingSuite) TestTruncateLogsInfoAndDebug() {
	spy := s.spyLogger()

	w, _, err := Open(s.dir, WithSyncPolicy(SyncImmediate), WithMaxSegmentSize(40), WithLogger(spy))
	s.Require().NoError(err)
	defer func() { s.Require().NoError(w.Close()) }()

	for lsn := uint64(1); lsn <= 6; lsn++ {
		_, appendErr := w.Append(context.Background(), payloadForLSN(lsn))
		s.Require().NoError(appendErr)
	}

	s.Require().NoError(w.Truncate(4))

	carriesUpTo := mock.MatchedBy(func(fields []Field) bool {
		return fieldEquals(fields, "upTo", uint64(4))
	})
	spy.AssertCalled(s.T(), "Info", "wal: truncated log", carriesUpTo)
	spy.AssertCalled(s.T(), "Debug", "wal: deleted truncated segment", mock.Anything)
}
