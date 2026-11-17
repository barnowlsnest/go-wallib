package wal

import (
	"container/list"
	"os"
	"sync"

	"github.com/barnowlsnest/go-wallib/internal/segment"
)

// readHandle is one read-only descriptor for a segment file, shared by every
// Reader in that segment. Readers read it with ReadAt, so they never share a file
// offset and need no lock to read.
//
// Sharing the descriptor saves the directory and file opens a Reader otherwise
// pays on every segment it enters; on scattered reads of older data those opens
// were about 15% of CPU.
type readHandle struct {
	file *os.File
	// idle is this handle's place in readHandles.idle while no Reader holds it.
	idle    *list.Element
	baseLSN uint64
	refs    int
	// retired handles are no longer in readHandles.byBase: their file was
	// deleted or replaced, or the WAL closed. The descriptor closes when the
	// last Reader releases it.
	retired bool
}

// readHandles caches readHandles by segment base LSN, so a Reader entering a
// segment that is already open needs no file name at all. A handle no Reader
// holds stays open (idle) for the next Reader, up to maxIdle of them; beyond
// that the least recently used idle handle is closed. Handles in use are never
// closed.
type readHandles struct {
	root   *os.Root
	byBase map[uint64]*readHandle
	// replacing counts, per base LSN, the deletes or renames of that segment
	// file in progress; see beginReplace.
	replacing map[uint64]int
	// idle lists handles no Reader holds, most recently released first.
	idle    *list.List
	dir     string
	maxIdle int
	mu      sync.Mutex
	closed  bool
}

func newReadHandles(root *os.Root, dir string, maxIdle int) *readHandles {
	return &readHandles{
		root:      root,
		byBase:    make(map[uint64]*readHandle),
		replacing: make(map[uint64]int),
		idle:      list.New(),
		dir:       dir,
		maxIdle:   maxIdle,
	}
}

// acquire returns a handle for the segment based at baseLSN, opening it if none
// is cached. While that file is being deleted or replaced (see beginReplace),
// the file is opened but not cached, so a Reader sees exactly what an open by
// path would show and no later Reader is handed a file that is gone.
//
// After close, the file is opened by path and the handle is not cached, so a
// Reader that outlives the WAL keeps reading as it did before handles were
// shared.
func (h *readHandles) acquire(baseLSN uint64) (*readHandle, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		file, err := os.OpenInRoot(h.dir, segment.Name(baseLSN))
		if err != nil {
			return nil, err
		}

		return &readHandle{file: file, baseLSN: baseLSN, refs: 1, retired: true}, nil
	}

	if handle, ok := h.byBase[baseLSN]; ok {
		if handle.idle != nil {
			h.idle.Remove(handle.idle)
			handle.idle = nil
		}
		handle.refs++

		return handle, nil
	}

	file, err := h.root.Open(segment.Name(baseLSN))
	if err != nil {
		return nil, err
	}

	if h.replacing[baseLSN] > 0 {
		return &readHandle{file: file, baseLSN: baseLSN, refs: 1, retired: true}, nil
	}

	handle := &readHandle{file: file, baseLSN: baseLSN, refs: 1}
	h.byBase[baseLSN] = handle

	return handle, nil
}

// release gives back a handle from acquire. The last release of a retired handle
// closes it; otherwise the handle turns idle and the oldest idle handle beyond
// maxIdle is closed.
func (h *readHandles) release(handle *readHandle) {
	h.mu.Lock()
	defer h.mu.Unlock()

	handle.refs--
	if handle.refs > 0 {
		return
	}

	if handle.retired {
		_ = handle.file.Close()

		return
	}

	handle.idle = h.idle.PushFront(handle)
	if h.idle.Len() > h.maxIdle {
		oldest, _ := h.idle.Remove(h.idle.Back()).(*readHandle)
		oldest.idle = nil
		delete(h.byBase, oldest.baseLSN)
		_ = oldest.file.Close()
	}
}

// beginReplace is called by the writer before it deletes or renames over the
// segment file based at baseLSN, and endReplace once that step is done. In
// between, no handle for that segment is cached: the cached one is dropped now
// and acquire opens the file without caching it. Readers already holding the
// dropped handle keep reading the file they opened.
func (h *readHandles) beginReplace(baseLSN uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.replacing[baseLSN]++
	if handle, ok := h.byBase[baseLSN]; ok {
		h.retireLocked(handle)
	}
}

// endReplace ends a beginReplace for baseLSN; later acquires cache again.
func (h *readHandles) endReplace(baseLSN uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.replacing[baseLSN]--
	if h.replacing[baseLSN] <= 0 {
		delete(h.replacing, baseLSN)
	}
}

// close retires every cached handle. Later acquires open files by path.
func (h *readHandles) close() {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.closed = true
	for _, handle := range h.byBase {
		h.retireLocked(handle)
	}
}

// retireLocked is retire for a caller holding mu.
func (h *readHandles) retireLocked(handle *readHandle) {
	delete(h.byBase, handle.baseLSN)
	handle.retired = true

	if handle.idle != nil {
		h.idle.Remove(handle.idle)
		handle.idle = nil
		_ = handle.file.Close()
	}
}
