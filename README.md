# go-wallib

[![Build](https://github.com/barnowlsnest/go-wal/actions/workflows/build.yml/badge.svg)](https://github.com/barnowlsnest/go-wal/actions/workflows/build.yml)
[![Lint](https://github.com/barnowlsnest/go-wal/actions/workflows/lint.yml/badge.svg)](https://github.com/barnowlsnest/go-wal/actions/workflows/lint.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/barnowlsnest/go-wallib/pkg/wal.svg)](https://pkg.go.dev/github.com/barnowlsnest/go-wallib/pkg/wal)
[![Go Version](https://img.shields.io/badge/Go-%3E%3D%201.27-00ADD8?logo=go)](https://go.dev/dl/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A production-ready Write-Ahead Log library for Go: append-only, durable, and
crash-safe, with monotonic Log Sequence Numbers, CRC32C-checksummed records,
size-rolled segment files, low-water-mark cleanup, and a single-writer Singular
Update Queue.

> **Module path:** `github.com/barnowlsnest/go-wallib`  
> **Repository:** [github.com/barnowlsnest/go-wal](https://github.com/barnowlsnest/go-wal)

## Status

The library is used as a durable append log with crash recovery, segmentation,
and retention under caller control. The public API in `pkg/wal` is stable for
normal use; please open an issue before relying on undocumented internals under
`internal/`.

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
  is never split across files. `Truncate` reclaims whole obsolete segments;
  `CutOffset` precisely drops every record below an LSN (rewriting the boundary
  segment when needed).
- **Single writer (Singular Update Queue).** One goroutine serializes and
  batches all writes; the API is safe for concurrent use.
- **Readers, replay & followers.** A forward `Reader` cursor, `Replay`
  callback, and `Follower` (`iter.Seq2` / channel) iterate committed entries
  from any LSN — including live follow mode.
- Standard library only (plus an optional structured logger); no `unsafe`; all
  file access is confined to the log directory via `os.Root`.

## Install

```bash
go get github.com/barnowlsnest/go-wallib/pkg/wal
```

Requires **Go 1.27** or newer.

## Usage

```go
package main

import (
	"context"
	"fmt"

	"github.com/barnowlsnest/go-wallib/pkg/wal"
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

Full API docs: [pkg.go.dev/github.com/barnowlsnest/go-wallib/pkg/wal](https://pkg.go.dev/github.com/barnowlsnest/go-wallib/pkg/wal).

## Following the log

`Follower` is a forward cursor that exposes committed records as a range-over-func
iterator (`iter.Seq2[uint64, []byte]`). It works in two modes:

- **Snapshot mode** (default) — ends at the tail captured at creation time.
- **Follow mode** (`wal.WithFollow()`) — blocks at the tail and resumes as new
  records commit, like `tail -f` for the WAL.

```go
follower, err := w.Follower(0, wal.WithFollow()) // 0 = from the beginning
if err != nil {
    panic(err)
}
defer func() { _ = follower.Close() }()

ctx, cancel := context.WithCancel(context.Background())
defer cancel()

for lsn, payload := range follower.Records(ctx) {
    fmt.Printf("LSN %d: %s\n", lsn, payload)
}
if err := follower.Err(); err != nil {
    // handle read error
}
```

Each `payload` is a **fresh copy** — callers may retain it after the loop body
returns without risk of aliasing.

**Multiple independent followers** can tail the log concurrently; each maintains
its own cursor and position independently of the writer and of other followers.

**`select` integration.** Use `RecordsChan` to receive entries over a channel,
which composes naturally with `select`:

```go
ch := follower.RecordsChan(ctx)
for entry := range ch {
    fmt.Printf("LSN %d: %s\n", entry.LSN, entry.Payload)
}
```

**Truncation.** When `Truncate(upTo)` reclaims segments that a lagging follower
has not yet consumed, the follower's iterator ends and `Err()` returns
`wal.ErrTruncated`. Check `Err()` after every loop.

## Retention

There is **no automatic retention policy**. The log grows until the application
explicitly reclaims space — there is no TTL, no size-based expiry, and no
background purge goroutine.

Retention is **caller-driven**:

| API | Behavior |
|-----|----------|
| `Truncate(upToLSN)` | Deletes **whole closed segment files** entirely below the LSN. Advances `FirstLSN`. The **active segment is never deleted**, so some entries below `upToLSN` may remain readable. |
| `CutOffset(upToLSN)` | Drops **every** record below the LSN, rewriting the boundary segment (including the active one) when needed. Precise front-cut; LSN numbering stays monotonic and gapless. |

Typical pattern after a snapshot or checkpoint:

```go
if err := w.Truncate(snapshotLSN); err != nil {
    panic(err)
}

// Or, when you need a precise cut into the active segment:
if err := w.CutOffset(snapshotLSN); err != nil {
    panic(err)
}
```

`WithMaxSegmentSize` only controls when new segment files are created during
rolling; it does **not** delete old data. On `Open`, recovery may truncate a
torn tail or remove empty trailing segments from an interrupted roll; those steps
repair crash damage and are not retention policy.

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
wal.WithSyncPolicy(wal.SyncBatched)           // durability policy (default SyncBatched)
wal.WithMaxSegmentSize(64 << 20)              // soft roll threshold, bytes (default 64 MiB)
wal.WithMaxRecordSize(64 << 20)               // hard per-record limit, bytes (default 64 MiB)
wal.WithBatchSize(256)                        // max appends coalesced per commit
wal.WithBatchTimeout(2 * time.Millisecond)    // group-commit linger (SyncBatched)
wal.WithFlushInterval(100 * time.Millisecond) // background fsync period (SyncInterval)
wal.WithLogger(logger)                        // structured logger (default: no-op)
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

## Development

Prerequisites: Go 1.27+, [Task](https://taskfile.dev), and
[golangci-lint](https://golangci-lint.run/) (for lint).

```bash
git clone https://github.com/barnowlsnest/go-wal.git
cd go-wal

task go-test    # race + coverage
task go-lint    # golangci-lint
task sanity     # tidy, fmt, vet, lint, test
task build      # sanity + build
```

CI runs build/test and lint on every push and pull request to `main`.

## Contributing

Bug reports, fixes, and improvements are welcome.

1. Open an issue for larger changes so we can agree on the approach.
2. Keep PRs focused; match existing style and package boundaries (`pkg/wal` is
   the public surface; `internal/` is not).
3. Run `task sanity` before opening a pull request.
4. Prefer tests that cover crash/recovery, retention, and concurrency when
   touching those paths.

## License

Released under the [MIT License](LICENSE). Copyright (c) 2026 Barn Owls Nest.
