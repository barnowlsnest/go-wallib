package wal

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"
)

// SyncSuite covers group commit, the sync policies, and the public Sync.
type SyncSuite struct {
	suite.Suite
	dir string
}

func TestSyncSuite(t *testing.T) {
	suite.Run(t, new(SyncSuite))
}

func (s *SyncSuite) SetupTest() {
	s.dir = s.T().TempDir()
}

func (s *SyncSuite) TestConcurrentAppendsAreUniqueAndGapless() {
	w, _, err := Open(s.dir, WithSyncPolicy(SyncBatched))
	s.Require().NoError(err)
	defer func() { s.Require().NoError(w.Close()) }()

	const writers = 500
	assigned := make([]uint64, writers)

	var group sync.WaitGroup
	for i := range writers {
		group.Go(func() {
			payload := fmt.Appendf(nil, `{"op":"set","key":"row:%d","value":%d}`, i, i*7)
			lsn, appendErr := w.Append(context.Background(), payload)
			s.Require().NoError(appendErr)
			assigned[i] = lsn
		})
	}
	group.Wait()

	seen := make(map[uint64]int, writers)
	for _, lsn := range assigned {
		seen[lsn]++
	}
	for lsn := uint64(1); lsn <= writers; lsn++ {
		s.Require().Equal(1, seen[lsn], "LSN %d must be assigned exactly once", lsn)
	}
	s.Require().Equal(uint64(writers), w.LastLSN())
}

func (s *SyncSuite) TestSyncPoliciesPersistAfterSync() {
	policies := map[string]SyncPolicy{
		"immediate": SyncImmediate,
		"batched":   SyncBatched,
		"interval":  SyncInterval,
	}

	for name, policy := range policies {
		s.Run(name, func() {
			dir := s.T().TempDir()

			w, _, err := Open(dir, WithSyncPolicy(policy))
			s.Require().NoError(err)

			for i := range 20 {
				payload := fmt.Appendf(nil, `{"op":"set","key":"k%d"}`, i)
				_, appendErr := w.Append(context.Background(), payload)
				s.Require().NoError(appendErr)
			}

			s.Require().NoError(w.Sync())
			s.Require().NoError(w.Close())

			reopened, report, err := Open(dir)
			s.Require().NoError(err)
			defer func() { s.Require().NoError(reopened.Close()) }()
			s.Require().Equal(uint64(20), report.LastLSN, "policy %s must persist all records", name)
		})
	}
}

func (s *SyncSuite) TestSyncAfterCloseFails() {
	w, _, err := Open(s.dir)
	s.Require().NoError(err)
	s.Require().NoError(w.Close())

	s.Require().ErrorIs(w.Sync(), ErrClosed)
}

func (s *SyncSuite) TestSyncOnEmptyLogSucceeds() {
	w, _, err := Open(s.dir)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(w.Close()) }()

	s.Require().NoError(w.Sync())
}
