package segment

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/barnowlsnest/go-wal/internal/record"
)

// segMaxRecordBytes is a per-record cap larger than any test payload.
const segMaxRecordBytes = 4 << 20 // 4 MiB

// realistic WAL payloads resembling serialized state-change commands.
var (
	segSetCommand    = []byte(`{"op":"set","key":"account:1001","value":{"balance":250000,"currency":"USD"}}`)
	segDeleteCommand = []byte(`{"op":"delete","key":"session:9f3c1a7e-7b2d-4e51-bc44-6d2a0f1e88aa"}`)
	segCheckpoint    = []byte("checkpoint: replica=eu-west-1 offset=4096 term=7 leader=node-3")
)

// SegmentSuite exercises the segment file lifecycle against a real temp
// directory opened as an os.Root.
type SegmentSuite struct {
	suite.Suite
	root *os.Root
	dir  string
}

func TestSegmentSuite(t *testing.T) {
	suite.Run(t, new(SegmentSuite))
}

func (s *SegmentSuite) SetupTest() {
	s.dir = s.T().TempDir()
	root, err := os.OpenRoot(s.dir)
	s.Require().NoError(err)
	s.root = root
}

func (s *SegmentSuite) TearDownTest() {
	s.Require().NoError(s.root.Close())
}

func (s *SegmentSuite) TestCreateThenReopen() {
	seg, err := Create(s.root, 1)
	s.Require().NoError(err)
	s.Require().Equal(uint64(1), seg.BaseLSN())
	s.Require().Equal(int64(HeaderSize), seg.Size())
	s.Require().Equal(uint64(0), seg.LastLSN(), "empty segment reports baseLSN-1")
	s.Require().Equal(Name(1), seg.Name())
	s.Require().NoError(seg.Close())

	reopened, err := Open(s.root, 1)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(reopened.Close()) }()
	s.Require().Equal(uint64(1), reopened.BaseLSN())
	s.Require().Equal(int64(HeaderSize), reopened.Size())
}

func (s *SegmentSuite) TestCreateRejectsDuplicate() {
	seg, err := Create(s.root, 1)
	s.Require().NoError(err)
	s.Require().NoError(seg.Close())

	_, err = Create(s.root, 1)
	s.Require().Error(err, "creating an existing segment must fail (O_EXCL)")
}

func (s *SegmentSuite) TestOpenRejectsFilenameBaseMismatch() {
	seg, err := Create(s.root, 5)
	s.Require().NoError(err)
	s.Require().NoError(seg.Close())

	// Rename so the filename claims base 7 while the header still says 5.
	s.Require().NoError(os.Rename(
		filepath.Join(s.dir, Name(5)),
		filepath.Join(s.dir, Name(7)),
	))

	_, err = Open(s.root, 7)
	s.Require().ErrorIs(err, ErrCorrupt)
}

func (s *SegmentSuite) TestOpenMissingSegmentErrors() {
	_, err := Open(s.root, 42)
	s.Require().Error(err)
}

func (s *SegmentSuite) TestAppendThenScan() {
	seg, err := Create(s.root, 1)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(seg.Close()) }()

	payloads := [][]byte{segSetCommand, segDeleteCommand, segCheckpoint}
	for i, payload := range payloads {
		written, appendErr := seg.Append(uint64(i+1), payload)
		s.Require().NoError(appendErr)
		s.Require().Equal(record.EncodedSize(len(payload)), written)
	}
	s.Require().Equal(uint64(3), seg.LastLSN())
	s.Require().NoError(seg.Sync())

	result := seg.Scan(segMaxRecordBytes)
	s.Require().NoError(result.Err)
	s.Require().Equal(uint64(3), result.Records)
	s.Require().Equal(uint64(3), result.LastLSN)
	s.Require().Equal(seg.Size(), result.ValidEnd)
}

func (s *SegmentSuite) TestScanTornTailThenTruncate() {
	seg, err := Create(s.root, 1)
	s.Require().NoError(err)

	_, err = seg.Append(1, segSetCommand)
	s.Require().NoError(err)
	goodSize := seg.Size()

	_, err = seg.Append(2, segDeleteCommand)
	s.Require().NoError(err)
	s.Require().NoError(seg.Sync())

	// Simulate a torn tail by chopping the last few bytes of the second record.
	s.Require().NoError(seg.TruncateTo(seg.Size() - 3))
	s.Require().NoError(seg.Close())

	reopened, err := Open(s.root, 1)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(reopened.Close()) }()

	result := reopened.Scan(segMaxRecordBytes)
	s.Require().ErrorIs(result.Err, record.ErrTorn)
	s.Require().Equal(uint64(1), result.Records, "only the intact first record survives")
	s.Require().Equal(uint64(1), result.LastLSN)
	s.Require().Equal(goodSize, result.ValidEnd, "truncation point is the end of the last good record")
}
