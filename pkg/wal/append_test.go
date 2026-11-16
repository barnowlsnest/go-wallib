package wal

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/barnowlsnest/go-wallib/internal/record"
	"github.com/barnowlsnest/go-wallib/internal/segment"
)

// realistic append payloads.
var (
	appendSetCommand    = []byte(`{"op":"set","key":"account:1001","value":{"balance":250000,"currency":"USD"}}`)
	appendDeleteCommand = []byte(`{"op":"delete","key":"session:9f3c1a7e-7b2d-4e51-bc44-6d2a0f1e88aa"}`)
	appendSnapshotChunk = bytes.Repeat([]byte("snapshot-chunk;"), 8) // ~120 bytes
)

// AppendSuite covers Append/AppendBatch, record-size limits, close behavior, and
// segment rolling.
type AppendSuite struct {
	suite.Suite
	dir string
}

func TestAppendSuite(t *testing.T) {
	suite.Run(t, new(AppendSuite))
}

func (s *AppendSuite) SetupTest() {
	s.dir = s.T().TempDir()
}

func (s *AppendSuite) open(opts ...Option) *WAL {
	s.T().Helper()

	w, _, err := Open(s.dir, opts...)
	s.Require().NoError(err)

	return w
}

func (s *AppendSuite) TestAppendAssignsSequentialLSNs() {
	w := s.open(WithSyncPolicy(SyncImmediate))
	defer func() { s.Require().NoError(w.Close()) }()

	payloads := [][]byte{appendSetCommand, appendDeleteCommand, appendSnapshotChunk, appendSetCommand, appendDeleteCommand}
	for i, payload := range payloads {
		lsn, err := w.Append(context.Background(), payload)
		s.Require().NoError(err)
		s.Require().Equal(uint64(i+1), lsn, "LSNs must be sequential and gapless")
	}

	s.Require().Equal(uint64(1), w.FirstLSN())
	s.Require().Equal(uint64(5), w.LastLSN())
}

func (s *AppendSuite) TestAppendBatchAssignsContiguousLSNs() {
	w := s.open(WithSyncPolicy(SyncImmediate))
	defer func() { s.Require().NoError(w.Close()) }()

	assignedLSNs, err := w.AppendBatch(context.Background(), [][]byte{appendSetCommand, appendDeleteCommand})
	s.Require().NoError(err)
	s.Require().Equal([]uint64{1, 2}, assignedLSNs)
	s.Require().Equal(uint64(2), w.LastLSN())
}

func (s *AppendSuite) TestAppendRejectsTooLarge() {
	w := s.open(WithMaxRecordSize(8))
	defer func() { s.Require().NoError(w.Close()) }()

	_, err := w.Append(context.Background(), make([]byte, 9))
	s.Require().ErrorIs(err, ErrRecordTooLarge)
}

func (s *AppendSuite) TestAppendAfterCloseFails() {
	w := s.open()
	s.Require().NoError(w.Close())

	_, err := w.Append(context.Background(), appendSetCommand)
	s.Require().ErrorIs(err, ErrClosed)
}

func (s *AppendSuite) TestAppendRollsSegment() {
	// Segment threshold small enough that each record forces a roll.
	w := s.open(WithSyncPolicy(SyncImmediate), WithMaxSegmentSize(128))
	defer func() { s.Require().NoError(w.Close()) }()

	for range 10 {
		_, err := w.Append(context.Background(), appendSetCommand)
		s.Require().NoError(err)
	}

	s.Require().GreaterOrEqual(w.segmentCount(), 2, "appends must roll into multiple segments")
	s.Require().Equal(uint64(10), w.LastLSN())
}

func (s *AppendSuite) TestOversizedRecordGetsOwnSegment() {
	w := s.open(WithSyncPolicy(SyncImmediate), WithMaxSegmentSize(64), WithMaxRecordSize(1<<20))
	defer func() { s.Require().NoError(w.Close()) }()

	_, err := w.Append(context.Background(), appendDeleteCommand)
	s.Require().NoError(err)

	lsn, err := w.Append(context.Background(), appendSnapshotChunk)
	s.Require().NoError(err)
	s.Require().Equal(uint64(2), lsn)
	s.Require().Equal(2, w.segmentCount(), "a record over the soft threshold rolls onto its own segment")
}

func (s *AppendSuite) TestAppendPersistsAcrossReopen() {
	w := s.open(WithSyncPolicy(SyncImmediate))
	_, err := w.AppendBatch(context.Background(), [][]byte{appendSetCommand, appendDeleteCommand, appendSnapshotChunk})
	s.Require().NoError(err)
	s.Require().NoError(w.Close())

	reopened, report, err := Open(s.dir)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(reopened.Close()) }()

	s.Require().Equal(uint64(3), report.EntriesRecovered)
	s.Require().Equal(uint64(1), report.FirstLSN)
	s.Require().Equal(uint64(3), report.LastLSN)
}

// newTestRequest builds an append request as AppendBatch would, for tests that
// hand several requests to commit as one group.
func newTestRequest(payloads ...[]byte) *appendRequest {
	return &appendRequest{
		ctx:      context.Background(),
		resultCh: make(chan appendResult, 1),
		payloads: payloads,
	}
}

// readAll returns every entry from a reopened log in dir.
func (s *AppendSuite) readAll() (lsns []uint64, payloads [][]byte) {
	s.T().Helper()

	w := s.open()
	defer func() { s.Require().NoError(w.Close()) }()

	reader, err := w.NewReader(w.FirstLSN())
	s.Require().NoError(err)
	defer func() { s.Require().NoError(reader.Close()) }()

	for reader.Next() {
		entry := reader.Entry()
		lsns = append(lsns, entry.LSN)
		payloads = append(payloads, bytes.Clone(entry.Payload))
	}
	s.Require().NoError(reader.Err())

	return lsns, payloads
}

// A batch larger than writeBufferLimit is written out early, and a roll inside
// the batch writes the staged records to the old segment first; either way every
// record lands once, in order.
func (s *AppendSuite) TestAppendBatchFlushesEarly() {
	const recordSize = 1 << 20
	payloads := make([][]byte, 5)
	for i := range payloads {
		payloads[i] = bytes.Repeat(fmt.Appendf(nil, "snapshot-chunk-%d;", i), recordSize/17)
	}

	tests := []struct {
		name             string
		maxSegmentSize   int64
		expectedSegments int
	}{
		{name: "one segment", maxSegmentSize: 64 << 20, expectedSegments: 1},
		{name: "rolls every two records", maxSegmentSize: 5 << 19, expectedSegments: 3},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			s.dir = s.T().TempDir()
			w := s.open(WithSyncPolicy(SyncBatched), WithMaxSegmentSize(tc.maxSegmentSize))

			actualLSNs, err := w.AppendBatch(context.Background(), payloads)
			s.Require().NoError(err)
			s.Require().Equal([]uint64{1, 2, 3, 4, 5}, actualLSNs)
			s.Require().Equal(tc.expectedSegments, w.segmentCount())
			s.Require().NoError(w.Close())

			readLSNs, readPayloads := s.readAll()
			s.Require().Equal([]uint64{1, 2, 3, 4, 5}, readLSNs)
			s.Require().Equal(payloads, readPayloads)
		})
	}
}

// When writing a group's staged records fails, every request in the group fails
// and their LSNs are handed out again by the next append.
func (s *AppendSuite) TestWriteFailureFailsWholeBatch() {
	w := s.open(WithSyncPolicy(SyncBatched))

	_, err := w.Append(context.Background(), appendSetCommand)
	s.Require().NoError(err)

	// A closed file makes the group's single write fail.
	s.Require().NoError(w.active.Close())
	group := []*appendRequest{
		newTestRequest(appendDeleteCommand),
		newTestRequest(appendSetCommand, appendSnapshotChunk),
	}
	w.commit(group)

	for _, request := range group {
		actual := <-request.resultCh
		s.Require().Error(actual.err)
		s.Require().Nil(actual.assignedLSNs)
	}
	s.Require().Equal(uint64(1), w.LastLSN())
	s.Require().Equal(uint64(2), w.nextLSN)

	// With a working handle on the same file, the next append reuses LSN 2.
	reopened, err := segment.Open(w.root, w.active.BaseLSN())
	s.Require().NoError(err)
	w.mu.Lock()
	w.active = reopened
	w.mu.Unlock()

	actualLSNs, err := w.AppendBatch(context.Background(), [][]byte{appendDeleteCommand, appendSetCommand})
	s.Require().NoError(err)
	s.Require().Equal([]uint64{2, 3}, actualLSNs)
	s.Require().NoError(w.Close())

	readLSNs, readPayloads := s.readAll()
	s.Require().Equal([]uint64{1, 2, 3}, readLSNs)
	s.Require().Equal([][]byte{appendSetCommand, appendDeleteCommand, appendSetCommand}, readPayloads)
}

// A request that fails on its own (here a roll whose new segment cannot be
// created) fails alone: the earlier request in the group, written out by that
// roll, is kept exactly once, and the failed request's LSN is reused.
func (s *AppendSuite) TestFailedRollFailsOnlyThatRequest() {
	// One record fills a segment, so the second request's first record rolls.
	w := s.open(
		WithSyncPolicy(SyncBatched),
		WithMaxSegmentSize(int64(segment.HeaderSize+record.EncodedSize(len(appendSetCommand)))),
	)

	// A file already named for the next segment makes the roll's create fail.
	blocker := filepath.Join(s.dir, segment.Name(2))
	s.Require().NoError(os.WriteFile(blocker, nil, 0o600))

	first, second := newTestRequest(appendSetCommand), newTestRequest(appendDeleteCommand)
	w.commit([]*appendRequest{first, second})

	actualFirst, actualSecond := <-first.resultCh, <-second.resultCh
	s.Require().NoError(actualFirst.err)
	s.Require().Equal([]uint64{1}, actualFirst.assignedLSNs)
	s.Require().Error(actualSecond.err)
	s.Require().Equal(uint64(1), w.LastLSN())

	s.Require().NoError(os.Remove(blocker))
	lsn, err := w.Append(context.Background(), appendDeleteCommand)
	s.Require().NoError(err)
	s.Require().Equal(uint64(2), lsn)
	s.Require().NoError(w.Close())

	readLSNs, readPayloads := s.readAll()
	s.Require().Equal([]uint64{1, 2}, readLSNs)
	s.Require().Equal([][]byte{appendSetCommand, appendDeleteCommand}, readPayloads)
}
