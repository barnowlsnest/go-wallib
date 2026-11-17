package wal

import (
	"context"
	"fmt"
	"testing"
)

// makeBenchPayload builds a payload of the requested size from a repeating
// WAL-command pattern, so benchmarks exercise realistic bytes rather than zeros.
func makeBenchPayload(size int) []byte {
	pattern := []byte(`{"op":"set","key":"account:1001","value":{"balance":250000}};`)
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = pattern[i%len(pattern)]
	}

	return payload
}

// benchmarkAppend measures single-record Append throughput for one policy and
// payload size.
func benchmarkAppend(b *testing.B, policy SyncPolicy, payloadSize int) {
	b.Helper()

	w, _, err := Open(b.TempDir(), WithSyncPolicy(policy))
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	payload := makeBenchPayload(payloadSize)
	ctx := context.Background()

	b.ReportAllocs()
	b.SetBytes(int64(payloadSize))
	for b.Loop() {
		if _, appendErr := w.Append(ctx, payload); appendErr != nil {
			b.Fatal(appendErr)
		}
	}
}

func BenchmarkAppend(b *testing.B) {
	policies := []struct {
		name   string
		policy SyncPolicy
	}{
		{"Immediate", SyncImmediate},
		{"Batched", SyncBatched},
		{"Interval", SyncInterval},
	}

	for _, policyCase := range policies {
		for _, payloadSize := range []int{64, 4096} {
			b.Run(fmt.Sprintf("%s/%dB", policyCase.name, payloadSize), func(b *testing.B) {
				benchmarkAppend(b, policyCase.policy, payloadSize)
			})
		}
	}
}

func BenchmarkAppendParallel(b *testing.B) {
	w, _, err := Open(b.TempDir(), WithSyncPolicy(SyncBatched))
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	payload := makeBenchPayload(256)

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		for pb.Next() {
			if _, appendErr := w.Append(ctx, payload); appendErr != nil {
				b.Fatal(appendErr)
			}
		}
	})
}

func BenchmarkReplay(b *testing.B) {
	const records = 10_000

	w, _, err := Open(b.TempDir(), WithSyncPolicy(SyncBatched))
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	payload := makeBenchPayload(128)
	ctx := context.Background()
	for range records {
		if _, appendErr := w.Append(ctx, payload); appendErr != nil {
			b.Fatal(appendErr)
		}
	}
	if syncErr := w.Sync(); syncErr != nil {
		b.Fatal(syncErr)
	}

	b.ReportAllocs()
	for b.Loop() {
		visited := 0
		replayErr := w.Replay(0, func(Entry) error {
			visited++

			return nil
		})
		if replayErr != nil {
			b.Fatal(replayErr)
		}
	}
}

// benchLayouts spreads the same ~8 MiB of 1 KiB records over one segment or
// over 64 segments, so a benchmark shows both the cost that grows with the whole
// log and the cost that stays inside one segment.
var benchLayouts = []struct {
	name        string
	segmentSize int64
}{
	{"OneSegment", 16 << 20},
	{"64Segments", 128 << 10},
}

const (
	benchFillRecords = 8 << 10
	benchFillBatch   = 256
	benchRecordSize  = 1 << 10
)

// openFilledWAL opens a WAL in a fresh directory and appends benchFillRecords
// records, so reads near the tail have a large log behind them.
func openFilledWAL(b *testing.B, segmentSize int64) *WAL {
	b.Helper()

	w, _, err := Open(b.TempDir(), WithSyncPolicy(SyncInterval), WithMaxSegmentSize(segmentSize))
	if err != nil {
		b.Fatal(err)
	}

	payload := makeBenchPayload(benchRecordSize)
	batch := make([][]byte, benchFillBatch)
	for i := range batch {
		batch[i] = payload
	}

	for range benchFillRecords / benchFillBatch {
		if _, appendErr := w.AppendBatch(context.Background(), batch); appendErr != nil {
			b.Fatal(appendErr)
		}
	}

	return w
}

// BenchmarkNewReaderNearTail measures opening a reader at the last record and
// reading it, which is what a cache miss on a recent record costs.
func BenchmarkNewReaderNearTail(b *testing.B) {
	for _, layout := range benchLayouts {
		b.Run(layout.name, func(b *testing.B) {
			w := openFilledWAL(b, layout.segmentSize)
			defer func() { _ = w.Close() }()

			lastLSN := w.LastLSN()

			b.ReportAllocs()
			for b.Loop() {
				reader, err := w.NewReader(lastLSN)
				if err != nil {
					b.Fatal(err)
				}
				if !reader.Next() || reader.Entry().LSN != lastLSN {
					b.Fatalf("expected LSN %d, err %v", lastLSN, reader.Err())
				}
				_ = reader.Close()
			}
		})
	}
}

// BenchmarkFollowerTail measures one append reaching a follow-mode follower
// parked at the tail of a large log: the per-record cost of live tailing.
func BenchmarkFollowerTail(b *testing.B) {
	for _, layout := range benchLayouts {
		b.Run(layout.name, func(b *testing.B) {
			w := openFilledWAL(b, layout.segmentSize)
			defer func() { _ = w.Close() }()

			follower, err := w.Follower(w.LastLSN()+1, WithFollow())
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = follower.Close() }()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			entries := follower.RecordsChan(ctx)
			payload := makeBenchPayload(benchRecordSize)

			b.ReportAllocs()
			for b.Loop() {
				lsn, appendErr := w.Append(ctx, payload)
				if appendErr != nil {
					b.Fatal(appendErr)
				}
				if entry := <-entries; entry.LSN != lsn {
					b.Fatalf("expected LSN %d, got %d (err %v)", lsn, entry.LSN, follower.Err())
				}
			}
		})
	}
}

// benchOlderReadRecords is how many records each older read takes, like a
// cache miss that fetches a short run of entries.
const benchOlderReadRecords = 4

// BenchmarkNewReaderOlder measures a cache miss on older data: a reader opened
// at an LSN spread across the whole log (not the tail), reading a few records
// and closing. The LSNs step by a prime so they land at different distances
// from the index entry below them.
func BenchmarkNewReaderOlder(b *testing.B) {
	const stride = 997

	for _, layout := range benchLayouts {
		b.Run(layout.name, func(b *testing.B) {
			w := openFilledWAL(b, layout.segmentSize)
			defer func() { _ = w.Close() }()

			span := w.LastLSN() - benchOlderReadRecords
			next := uint64(0)

			b.ReportAllocs()
			for b.Loop() {
				fromLSN := next%span + 1
				next += stride

				reader, err := w.NewReader(fromLSN)
				if err != nil {
					b.Fatal(err)
				}
				for want := fromLSN; want < fromLSN+benchOlderReadRecords; want++ {
					if !reader.Next() || reader.Entry().LSN != want {
						b.Fatalf("expected LSN %d, err %v", want, reader.Err())
					}
				}
				_ = reader.Close()
			}
		})
	}
}
