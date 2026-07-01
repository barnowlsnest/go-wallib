package segment

import (
	"cmp"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// ext is the filename extension shared by all segment files.
const ext = ".wal"

// nameWidth is the zero-padded width of the base-LSN portion of a segment
// filename. 20 digits is the maximum decimal width of a uint64, so filenames
// sort lexicographically in the same order as their numeric base LSN.
const nameWidth = 20

// TempSuffix is appended to a segment's canonical filename while RewriteFrom
// builds it, before the atomic rename publishes it. A file ending in
// ext+TempSuffix is a partial segment from an interrupted rewrite and holds no
// committed data.
const TempSuffix = ".tmp"

// Name returns the canonical segment filename for a given base LSN, e.g.
// Name(42) == "00000000000000000042.wal".
func Name(baseLSN uint64) string {
	return fmt.Sprintf("%0*d%s", nameWidth, baseLSN, ext)
}

// ParseBaseLSN extracts the base LSN from a segment filename. ok is false when
// name is not a canonical segment filename (wrong extension, wrong width, or
// non-numeric stem), so foreign files in the directory are safely ignored.
func ParseBaseLSN(name string) (baseLSN uint64, ok bool) {
	stem, found := strings.CutSuffix(name, ext)
	if !found || len(stem) != nameWidth {
		return 0, false
	}

	parsed, err := strconv.ParseUint(stem, 10, 64)
	if err != nil {
		return 0, false
	}

	return parsed, true
}

// segmentFile pairs a segment's full path with its parsed base LSN for sorting.
type segmentFile struct {
	path    string
	baseLSN uint64
}

// List returns the full paths of all segment files in dir, sorted ascending by
// base LSN. Non-segment files and subdirectories are ignored.
func List(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var found []segmentFile
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		baseLSN, ok := ParseBaseLSN(entry.Name())
		if !ok {
			continue
		}
		found = append(found, segmentFile{
			path:    filepath.Join(dir, entry.Name()),
			baseLSN: baseLSN,
		})
	}

	slices.SortFunc(found, func(a, b segmentFile) int {
		return cmp.Compare(a.baseLSN, b.baseLSN)
	})

	paths := make([]string, len(found))
	for i, sf := range found {
		paths[i] = sf.path
	}
	return paths, nil
}

// RemoveStaleTempFiles deletes leftover "*.wal.tmp" files from a RewriteFrom that
// crashed before its atomic rename. They were never published and hold no
// committed data, so removing them on recovery reclaims their space. It fsyncs
// the directory when it removed anything, and reports how many it removed.
func RemoveStaleTempFiles(root *os.Root, dir string) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}

	removed := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ext+TempSuffix) {
			continue
		}

		if removeErr := root.Remove(entry.Name()); removeErr != nil {
			return removed, removeErr
		}

		removed++
	}

	if removed > 0 {
		return removed, SyncDir(dir)
	}

	return removed, nil
}

// SyncDir fsyncs a directory so that prior file creations, renames, and removals
// within it are durable across a crash. It opens the directory through
// os.OpenInRoot so the access is confined to dir and cannot be diverted outside
// it by a symlink.
func SyncDir(dir string) error {
	handle, err := os.OpenInRoot(dir, ".")
	if err != nil {
		return err
	}
	defer func() { _ = handle.Close() }()

	return handle.Sync()
}
