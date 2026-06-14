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
package wal
