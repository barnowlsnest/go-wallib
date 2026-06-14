# go-wal

A production-ready Write-Ahead Log library for Go: append-only, durable, and
crash-safe, with monotonic Log Sequence Numbers, CRC32C-checksummed records,
size-rolled segment files, low-water-mark cleanup, and a single-writer Singular
Update Queue.

## Features

- **Durable, ordered appends.** Every record is assigned a unique, monotonic,
  gapless Log Sequence Number (LSN) starting at 1.
- **Crash recovery.** On `Open` a torn tail (interrupted final write) is
  truncated; mid-log corruption is reported as a hard error; an interrupted
  segment roll (empty trailing segment) is reclaimed. Writer state is fully
  restored, so the next append continues with no gap.
- **Configurable durability** via `SyncPolicy`: `SyncImmediate`, `SyncBatched`
  (group commit), or `SyncInterval` (periodic background fsync).
- **Segmentation & cleanup.** The log rolls into size-bounded segments; a record
  is never split across files. `Truncate` reclaims whole obsolete segments at a
  low-water mark.
- **Single writer (Singular Update Queue).** One goroutine serializes and
  batches all writes; the API is safe for concurrent use.
- **Readers & replay.** A forward `Reader` cursor and `Replay` callback iterate
  committed entries from any LSN.
- Standard library only (plus an optional structured logger); no `unsafe`; all
  file access is confined to the log directory via `os.Root`.

## Install

```bash
go get github.com/barnowlsnest/go-wal/pkg/wal
```

Requires Go 1.26 or newer.

## Usage

```go
package main

import (
	"context"
	"fmt"

	"github.com/barnowlsnest/go-wal/pkg/wal"
)

func main() {
	w, report, err := wal.Open("data/wal", wal.WithSyncPolicy(wal.SyncBatched))
	if err != nil {
		panic(err)
	}
	defer func() { _ = w.Close() }()

	fmt.Printf("recovered %d entries up to LSN %d\n",
		report.EntriesRecovered, report.LastLSN)

	lsn, err := w.Append(context.Background(), []byte(`{"op":"set","key":"k","value":1}`))
	if err != nil {
		panic(err)
	}
	fmt.Println("appended at LSN", lsn)

	// Replay everything from the beginning.
	err = w.Replay(0, func(entry wal.Entry) error {
		fmt.Printf("LSN %d: %s\n", entry.LSN, entry.Payload)
		return nil
	})
	if err != nil {
		panic(err)
	}

	// After persisting a snapshot at lsn, reclaim older segments.
	if err := w.Truncate(lsn); err != nil {
		panic(err)
	}
}
```

## Sync policies

| Policy          | When the fsync happens                                   | Trade-off                                  |
|-----------------|----------------------------------------------------------|--------------------------------------------|
| `SyncImmediate` | before acknowledging each append                         | strongest durability, slowest              |
| `SyncBatched`   | once per group commit, before acknowledging the batch    | high throughput under concurrency          |
| `SyncInterval`  | periodically, by a background goroutine                  | fastest, bounded data-loss window on crash |

Call `Sync()` at any time to force a flush regardless of policy.

## Durability & idempotency

`Append` is **at-least-once**. If it returns `(lsn, nil)`, the record is durable
per the configured `SyncPolicy`. If it returns an error, or the process dies
before it returns, the record may or may not be durable, and a retry may create a
duplicate with a **new** LSN. Deduplicate using a key embedded in the payload —
**not** the LSN.

A canceled `context` is honored: an append whose context is done is never
committed and never consumes an LSN.

## Options

```go
wal.WithSyncPolicy(wal.SyncBatched)      // durability policy (default SyncBatched)
wal.WithMaxSegmentSize(64 << 20)         // soft roll threshold, bytes (default 64 MiB)
wal.WithMaxRecordSize(64 << 20)          // hard per-record limit, bytes (default 64 MiB)
wal.WithBatchSize(256)                    // max appends coalesced per commit
wal.WithBatchTimeout(2 * time.Millisecond)   // group-commit linger (SyncBatched)
wal.WithFlushInterval(100 * time.Millisecond) // background fsync period (SyncInterval)
wal.WithLogger(logger)                    // structured logger (default: no-op)
```

## Logging

`wal.Logger` is a structured, leveled interface
(`Debug`/`Info`/`Warn`/`Error(msg string, fields ...wal.Field)`) and `wal.Field`
is an alias for [`go-logslib`](https://github.com/barnowlsnest/go-logslib)'s
`logger.Field`, so a `*logger.Logger` satisfies it directly:

```go
w, _, err := wal.Open("data/wal", wal.WithLogger(myLogslibLogger))
```

If no logger is supplied, a no-op logger is used.

## On-disk format

Each segment file begins with a 28-byte header
(`Magic | Version | Flags | BaseLSN | CreatedAt | HeaderCRC`) followed by framed
records. Each record is `CRC32C(4) | Length(4) | LSN(8) | Payload`, with the
CRC32C (Castagnoli) computed over `Length || LSN || Payload`. All integers are
little-endian. Segment filenames are the zero-padded base LSN (e.g.
`00000000000000000001.wal`).

## License

MIT
