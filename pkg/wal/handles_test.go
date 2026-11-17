package wal

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/barnowlsnest/go-wallib/internal/segment"
)

// HandlesSuite covers the shared segment read descriptors.
type HandlesSuite struct {
	suite.Suite
	root *os.Root
	dir  string
}

func TestHandlesSuite(t *testing.T) {
	suite.Run(t, new(HandlesSuite))
}

// Base LSNs of the two files SetupTest writes. They only need a segment name;
// the handles never parse their content.
const (
	handleFirst  uint64 = 1
	handleSecond uint64 = 2
)

func (s *HandlesSuite) SetupTest() {
	s.dir = s.T().TempDir()
	s.writeFile(handleFirst, "first")
	s.writeFile(handleSecond, "second")

	root, err := os.OpenRoot(s.dir)
	s.Require().NoError(err)
	s.root = root
}

func (s *HandlesSuite) TearDownTest() {
	s.Require().NoError(s.root.Close())
}

func (s *HandlesSuite) writeFile(baseLSN uint64, content string) {
	s.T().Helper()
	s.Require().NoError(os.WriteFile(filepath.Join(s.dir, segment.Name(baseLSN)), []byte(content), 0o600))
}

// contentOf reads the whole file behind handle through ReadAt, as a Reader does.
func (s *HandlesSuite) contentOf(handle *readHandle) string {
	s.T().Helper()

	info, err := handle.file.Stat()
	s.Require().NoError(err)

	buf := make([]byte, info.Size())
	_, err = handle.file.ReadAt(buf, 0)
	s.Require().NoError(err)

	return string(buf)
}

func (s *HandlesSuite) requireClosed(handle *readHandle) {
	s.T().Helper()

	_, err := handle.file.Stat()
	s.Require().ErrorIs(err, os.ErrClosed)
}

func (s *HandlesSuite) TestHeldHandleIsShared() {
	handles := newReadHandles(s.root, s.dir, 4)
	defer handles.close()

	first, err := handles.acquire(handleFirst)
	s.Require().NoError(err)
	second, err := handles.acquire(handleFirst)
	s.Require().NoError(err)

	s.Require().Same(first, second)
	s.Require().Equal(2, first.refs)

	handles.release(first)
	handles.release(second)
}

func (s *HandlesSuite) TestReleasedHandleStaysOpenForNextReader() {
	handles := newReadHandles(s.root, s.dir, 4)
	defer handles.close()

	first, err := handles.acquire(handleFirst)
	s.Require().NoError(err)
	handles.release(first)

	again, err := handles.acquire(handleFirst)
	s.Require().NoError(err)
	defer handles.release(again)

	s.Require().Same(first, again)
	s.Require().Equal("first", s.contentOf(again))
}

func (s *HandlesSuite) TestIdleCapClosesLeastRecentlyUsed() {
	handles := newReadHandles(s.root, s.dir, 1)
	defer handles.close()

	first, err := handles.acquire(handleFirst)
	s.Require().NoError(err)
	second, err := handles.acquire(handleSecond)
	s.Require().NoError(err)

	handles.release(first)
	handles.release(second)

	s.requireClosed(first)
	s.Require().NotContains(handles.byBase, handleFirst)
	s.Require().Contains(handles.byBase, handleSecond)
	s.Require().Equal(1, handles.idle.Len())
}

func (s *HandlesSuite) TestHeldHandlesDoNotCountTowardCap() {
	handles := newReadHandles(s.root, s.dir, 1)
	defer handles.close()

	first, err := handles.acquire(handleFirst)
	s.Require().NoError(err)
	second, err := handles.acquire(handleSecond)
	s.Require().NoError(err)

	s.Require().Equal("first", s.contentOf(first))
	s.Require().Equal("second", s.contentOf(second))

	handles.release(first)
	handles.release(second)
}

// replace runs a delete or rename of the segment based at baseLSN the way the
// writer does, between beginReplace and endReplace.
func (s *HandlesSuite) replace(handles *readHandles, baseLSN uint64, change func()) {
	s.T().Helper()

	handles.beginReplace(baseLSN)
	change()
	handles.endReplace(baseLSN)
}

func (s *HandlesSuite) TestReplaceClosesIdleHandle() {
	handles := newReadHandles(s.root, s.dir, 4)
	defer handles.close()

	handle, err := handles.acquire(handleFirst)
	s.Require().NoError(err)
	handles.release(handle)

	s.replace(handles, handleFirst, func() {})

	s.requireClosed(handle)
	s.Require().Empty(handles.byBase)
	s.Require().Zero(handles.idle.Len())
}

func (s *HandlesSuite) TestReplaceKeepsHeldHandleUntilRelease() {
	handles := newReadHandles(s.root, s.dir, 4)
	defer handles.close()

	handle, err := handles.acquire(handleFirst)
	s.Require().NoError(err)

	s.replace(handles, handleFirst, func() {})

	s.Require().Equal("first", s.contentOf(handle), "a held handle must stay readable")

	handles.release(handle)
	s.requireClosed(handle)
	s.Require().Zero(handles.idle.Len(), "a retired handle must not turn idle")
}

func (s *HandlesSuite) TestAcquireAfterReplaceSeesReplacedFile() {
	handles := newReadHandles(s.root, s.dir, 4)
	defer handles.close()

	stale, err := handles.acquire(handleFirst)
	s.Require().NoError(err)
	handles.release(stale)

	s.replace(handles, handleFirst, func() {
		s.Require().NoError(os.Remove(filepath.Join(s.dir, segment.Name(handleFirst))))
		s.writeFile(handleFirst, "replacement")
	})

	fresh, err := handles.acquire(handleFirst)
	s.Require().NoError(err)
	defer handles.release(fresh)

	s.Require().NotSame(stale, fresh)
	s.Require().Equal("replacement", s.contentOf(fresh))
}

// TestAcquireDuringReplaceIsNotCached checks that a file opened while it is
// being deleted is not handed to Readers that arrive after the delete.
func (s *HandlesSuite) TestAcquireDuringReplaceIsNotCached() {
	handles := newReadHandles(s.root, s.dir, 4)
	defer handles.close()

	handles.beginReplace(handleFirst)

	during, err := handles.acquire(handleFirst)
	s.Require().NoError(err)
	s.Require().Empty(handles.byBase, "a file being replaced must not be cached")

	s.Require().NoError(os.Remove(filepath.Join(s.dir, segment.Name(handleFirst))))
	handles.endReplace(handleFirst)

	s.Require().Equal("first", s.contentOf(during), "a Reader that opened it keeps reading")

	_, err = handles.acquire(handleFirst)
	s.Require().ErrorIs(err, os.ErrNotExist)

	handles.release(during)
	s.requireClosed(during)
	s.Require().Empty(handles.replacing)
}

func (s *HandlesSuite) TestMissingFileReportsNotExist() {
	handles := newReadHandles(s.root, s.dir, 4)
	defer handles.close()

	_, err := handles.acquire(99)
	s.Require().ErrorIs(err, os.ErrNotExist)
	s.Require().Empty(handles.byBase)
}

func (s *HandlesSuite) TestCloseRetiresEverything() {
	handles := newReadHandles(s.root, s.dir, 4)

	idle, err := handles.acquire(handleFirst)
	s.Require().NoError(err)
	handles.release(idle)

	held, err := handles.acquire(handleSecond)
	s.Require().NoError(err)

	handles.close()

	s.requireClosed(idle)
	s.Require().Equal("second", s.contentOf(held), "a held handle outlives close")

	handles.release(held)
	s.requireClosed(held)
}

func (s *HandlesSuite) TestAcquireAfterCloseOpensByPath() {
	handles := newReadHandles(s.root, s.dir, 4)
	handles.close()
	// The WAL closes its root right after the handles; opening must not need it.
	s.Require().NoError(s.root.Close())
	root, err := os.OpenRoot(s.dir)
	s.Require().NoError(err)
	s.root = root

	handle, err := handles.acquire(handleFirst)
	s.Require().NoError(err)

	s.Require().Equal("first", s.contentOf(handle))
	s.Require().Empty(handles.byBase, "handles opened after close are not cached")

	handles.release(handle)
	s.requireClosed(handle)
}

// TestTruncateRetiresDeletedSegments checks that segments a Truncate deletes do
// not stay open as idle handles.
func (s *HandlesSuite) TestTruncateRetiresDeletedSegments() {
	w, _, err := Open(s.T().TempDir(), WithSyncPolicy(SyncImmediate), WithMaxSegmentSize(40))
	s.Require().NoError(err)
	defer func() { s.Require().NoError(w.Close()) }()

	for i := 1; i <= 6; i++ {
		_, appendErr := w.Append(context.Background(), payloadForLSN(uint64(i)))
		s.Require().NoError(appendErr)
	}

	s.Require().NoError(w.Replay(0, func(Entry) error { return nil }))

	before := w.segmentBases()
	s.Require().NoError(w.Truncate(4))
	after := w.segmentBases()
	s.Require().Less(len(after), len(before))

	w.handles.mu.Lock()
	defer w.handles.mu.Unlock()

	for _, base := range before[:len(before)-len(after)] {
		s.Assert().NotContains(w.handles.byBase, base)
	}
}

// TestReaderKeepsReadingAfterWALClose checks that a Reader created before Close
// still reads the segments after the one it was in, as it did before read
// descriptors were shared.
func (s *HandlesSuite) TestReaderKeepsReadingAfterWALClose() {
	w, _, err := Open(s.T().TempDir(), WithSyncPolicy(SyncImmediate), WithMaxSegmentSize(40))
	s.Require().NoError(err)

	const count = 5
	for i := 1; i <= count; i++ {
		_, appendErr := w.Append(context.Background(), payloadForLSN(uint64(i)))
		s.Require().NoError(appendErr)
	}

	reader, err := w.NewReader(0)
	s.Require().NoError(err)
	defer func() { _ = reader.Close() }()

	s.Require().True(reader.Next())
	s.Require().NoError(w.Close())

	read := 1
	for reader.Next() {
		read++
		s.Require().Equal(payloadForLSN(uint64(read)), reader.Entry().Payload)
	}
	s.Require().NoError(reader.Err())
	s.Require().Equal(count, read)
}
