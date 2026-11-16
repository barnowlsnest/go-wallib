package segment

import "sort"

// IndexInterval is how many bytes of records lie between two index entries. A
// reader that starts from the nearest entry at or below its LSN reads at most
// about this much before reaching it, instead of the whole segment before it.
// At 64 KiB a 64 MiB segment needs 1024 entries, 16 KiB of memory.
const IndexInterval = 64 << 10

// IndexEntry names the record with LSN that starts at byte Offset of its
// segment file.
type IndexEntry struct {
	LSN    uint64
	Offset int64
}

// Lookup returns the offset of the last entry in index whose LSN is at or below
// lsn. ok is false when there is none, and reading must start at the header.
func Lookup(index []IndexEntry, lsn uint64) (offset int64, ok bool) {
	above := sort.Search(len(index), func(i int) bool { return index[i].LSN > lsn })
	if above == 0 {
		return 0, false
	}

	return index[above-1].Offset, true
}

// indexDue reports whether a record starting at offset is far enough past the
// last indexed position to get an entry. The first record never does, because
// a reader starts there anyway.
func indexDue(index []IndexEntry, offset int64) bool {
	last := int64(HeaderSize)
	if len(index) > 0 {
		last = index[len(index)-1].Offset
	}

	return offset-last >= IndexInterval
}
