package segment

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"
)

// DirSuite groups segment naming, listing, and directory-fsync tests.
type DirSuite struct {
	suite.Suite
}

func TestDirSuite(t *testing.T) {
	suite.Run(t, new(DirSuite))
}

func (s *DirSuite) TestNameIsZeroPaddedAndRoundTrips() {
	cases := []struct {
		name     string
		baseLSN  uint64
		wantName string
	}{
		{"first segment", 1, "00000000000000000001.wal"},
		{"mid-range base", 4_200_000, "00000000000004200000.wal"},
		{"max uint64", ^uint64(0), "18446744073709551615.wal"},
	}
	for _, tc := range cases {
		s.Run(tc.name, func() {
			got := Name(tc.baseLSN)
			s.Require().Equal(tc.wantName, got)

			parsed, ok := ParseBaseLSN(got)
			s.Require().True(ok)
			s.Require().Equal(tc.baseLSN, parsed)
		})
	}
}

func (s *DirSuite) TestParseBaseLSNRejectsForeignNames() {
	cases := []struct {
		name     string
		filename string
	}{
		{"wrong extension", "00000000000000000042.log"},
		{"no extension", "00000000000000000042"},
		{"too short stem", "42.wal"},
		{"too long stem", "000000000000000000042.wal"},
		{"non-numeric stem", "0000000000000000004x.wal"},
		{"unrelated file", "README.md"},
		{"checkpoint sidecar", "checkpoint.json"},
	}
	for _, tc := range cases {
		s.Run(tc.name, func() {
			_, ok := ParseBaseLSN(tc.filename)
			s.Require().False(ok, "%q must not parse as a segment", tc.filename)
		})
	}
}

func (s *DirSuite) TestListReturnsSegmentsSortedByBaseLSN() {
	dir := s.T().TempDir()

	// Create segment files out of order, interleaved with noise files that must
	// be ignored.
	for _, baseLSN := range []uint64{30, 1, 4_200_000, 20} {
		path := filepath.Join(dir, Name(baseLSN))
		s.Require().NoError(os.WriteFile(path, []byte("segment-placeholder"), 0o600))
	}
	s.Require().NoError(os.WriteFile(filepath.Join(dir, "README.md"), nil, 0o600))
	s.Require().NoError(os.WriteFile(filepath.Join(dir, "checkpoint.json"), nil, 0o600))
	s.Require().NoError(os.Mkdir(filepath.Join(dir, "00000000000000000099.wal"), 0o750)) // a dir, not a file

	paths, err := List(dir)
	s.Require().NoError(err)

	want := []string{
		filepath.Join(dir, Name(1)),
		filepath.Join(dir, Name(20)),
		filepath.Join(dir, Name(30)),
		filepath.Join(dir, Name(4_200_000)),
	}
	s.Require().Equal(want, paths)
}

func (s *DirSuite) TestListEmptyDir() {
	paths, err := List(s.T().TempDir())
	s.Require().NoError(err)
	s.Require().Empty(paths)
}

func (s *DirSuite) TestListMissingDirErrors() {
	_, err := List(filepath.Join(s.T().TempDir(), "does-not-exist"))
	s.Require().Error(err)
}

func (s *DirSuite) TestSyncDir() {
	s.Require().NoError(SyncDir(s.T().TempDir()))
}

func (s *DirSuite) TestSyncDirMissingErrors() {
	err := SyncDir(filepath.Join(s.T().TempDir(), "does-not-exist"))
	s.Require().Error(err)
}
