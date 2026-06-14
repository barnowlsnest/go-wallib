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
