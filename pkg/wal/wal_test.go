package wal

import (
	"os"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/barnowlsnest/go-wal/internal/segment"
)

// realistic WAL payloads resembling serialized state-change commands.
var (
	walSetCommand    = []byte(`{"op":"set","key":"account:1001","value":{"balance":250000,"currency":"USD"}}`)
	walDeleteCommand = []byte(`{"op":"delete","key":"session:9f3c1a7e-7b2d-4e51-bc44-6d2a0f1e88aa"}`)
	walCheckpoint    = []byte("checkpoint: replica=eu-west-1 offset=4096 term=7 leader=node-3")
)

// WALSuite covers Open/recovery and Close on a fresh temp directory.
type WALSuite struct {
	suite.Suite
	dir string
}

func TestWALSuite(t *testing.T) {
	suite.Run(t, new(WALSuite))
}

func (s *WALSuite) SetupTest() {
	s.dir = s.T().TempDir()
}

// seedSegment writes a segment with the given base LSN and payloads directly via
// the segment layer, simulating a log left behind by a previous process.
func (s *WALSuite) seedSegment(baseLSN uint64, payloads ...[]byte) {
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

func (s *WALSuite) TestOpenFreshLog() {
	w, report, err := Open(s.dir)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(w.Close()) }()

	s.Require().Equal(&RecoveryReport{}, report)
	s.Require().Equal(uint64(0), w.FirstLSN())
	s.Require().Equal(uint64(0), w.LastLSN())
	s.Require().Equal(1, w.segmentCount(), "a fresh log starts with exactly one segment")
}

func (s *WALSuite) TestCloseIsIdempotent() {
	w, _, err := Open(s.dir)
	s.Require().NoError(err)

	s.Require().NoError(w.Close())
	s.Require().NoError(w.Close(), "second Close must be a no-op")
}

func (s *WALSuite) TestReopenEmptyLog() {
	w, _, err := Open(s.dir)
	s.Require().NoError(err)
	s.Require().NoError(w.Close())

	reopened, report, err := Open(s.dir)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(reopened.Close()) }()

	s.Require().Equal(uint64(0), report.LastLSN)
	s.Require().Equal(1, reopened.segmentCount())
}

func (s *WALSuite) TestRecoverExistingRecords() {
	s.seedSegment(1, walSetCommand, walDeleteCommand, walCheckpoint)

	w, report, err := Open(s.dir)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(w.Close()) }()

	s.Require().Equal(uint64(3), report.EntriesRecovered)
	s.Require().Equal(uint64(1), report.FirstLSN)
	s.Require().Equal(uint64(3), report.LastLSN)
	s.Require().Equal(uint64(1), w.FirstLSN())
	s.Require().Equal(uint64(3), w.LastLSN())
}

func (s *WALSuite) TestRecoverTornTailTruncates() {
	root, err := os.OpenRoot(s.dir)
	s.Require().NoError(err)

	seg, err := segment.Create(root, 1)
	s.Require().NoError(err)

	_, err = seg.Append(1, walSetCommand)
	s.Require().NoError(err)
	goodSize := seg.Size()

	_, err = seg.Append(2, walDeleteCommand)
	s.Require().NoError(err)
	fullSize := seg.Size()
	s.Require().NoError(seg.Sync())

	// Chop the last 3 bytes to simulate an interrupted write of record 2; the
	// partial record left on disk is what recovery must truncate.
	s.Require().NoError(seg.TruncateTo(fullSize - 3))
	s.Require().NoError(seg.Close())
	s.Require().NoError(root.Close())

	w, report, err := Open(s.dir)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(w.Close()) }()

	s.Require().Equal(uint64(1), report.EntriesRecovered)
	s.Require().Equal((fullSize-3)-goodSize, report.BytesTruncated, "leftover torn bytes removed")
	s.Require().Equal(uint64(1), report.FirstLSN)
	s.Require().Equal(uint64(1), w.LastLSN())
}
