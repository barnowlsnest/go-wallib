// Package wal implements a production-ready Write-Ahead Log.
//
// A WAL durably records state changes as an append-only sequence of records
// before they are applied, so that state can be reconstructed by replaying the
// log after a restart or crash.
//
// Each appended record is assigned a unique, monotonic, gapless Log Sequence
// Number (LSN) starting at 1. LSN 0 is reserved as a sentinel meaning "no
// entry" (an empty log's LastLSN, or "from the beginning" for Replay/NewReader).
//
// Records are framed with a CRC32C checksum and stored in size-rolled segment
// files. A single writer goroutine (the Singular Update Queue) serializes and
// batches writes. Durability is configurable via SyncPolicy.
//
// # Retention
//
// There is no automatic retention policy: no TTL, no size-based expiry, and no
// background purge. All committed records are kept until the application calls
// Truncate. The intended pattern is to persist application state (for example a
// snapshot or checkpoint) and then call Truncate with that LSN to reclaim disk.
//
// Truncate advances a low-water mark (FirstLSN) by deleting whole closed segment
// files whose entries are entirely below the given LSN. The active segment is
// never deleted, so truncation is segment-granular best-effort reclamation, not
// a precise per-entry delete. WithMaxSegmentSize controls when new segment
// files are created during rolling; it does not delete old data.
//
// On Open, recovery may truncate a torn tail or remove empty trailing segments
// from an interrupted roll; those steps repair crash damage and are not
// retention policy.
package wal
