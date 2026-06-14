package record

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// fuzzMaxRecordBytes caps payload sizes during fuzzing so a corrupt length field
// cannot drive an enormous allocation. It mirrors a realistic per-record limit.
const fuzzMaxRecordBytes = 1 << 16 // 64 KiB

// FuzzScannerNoPanic feeds arbitrary byte streams to the scanner and asserts it
// always terminates without panicking, regardless of how malformed the input is.
// The scanner must classify every stream as clean EOF, torn, corrupt, or too
// large — never crash.
func FuzzScannerNoPanic(f *testing.F) {
	cleanRecord, err := Encode(nil, 1_000_000, setCommand)
	require.NoError(f, err, "seed Encode")
	twoRecords, err := Encode(cleanRecord, 1_000_001, deleteCommand)
	require.NoError(f, err, "seed Encode")

	f.Add([]byte(nil))                                // empty stream (clean EOF)
	f.Add(cleanRecord)                                // one valid record
	f.Add(twoRecords)                                 // two valid records
	f.Add(cleanRecord[:HeaderSize/2])                 // torn header
	f.Add(cleanRecord[:len(cleanRecord)-1])           // torn payload
	f.Add([]byte{0, 0, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF}) // huge declared length
	f.Add(bytes.Repeat([]byte{0xAB}, HeaderSize+7))   // garbage bytes

	f.Fuzz(func(_ *testing.T, stream []byte) {
		scanner := NewScanner(bytes.NewReader(stream), fuzzMaxRecordBytes)
		for scanner.Next() {
			_ = scanner.Record()
		}
		// None of these may panic on any input.
		_ = scanner.Err()
		_ = scanner.Offset()
	})
}

// FuzzEncodeDecodeRoundTrip asserts that any payload that fits within the record
// limit survives an Encode then Scan unchanged, with its LSN preserved and no
// trailing records produced.
func FuzzEncodeDecodeRoundTrip(f *testing.F) {
	f.Add(uint64(1), setCommand)
	f.Add(uint64(1<<40), unicodeNote)
	f.Add(uint64(0), []byte(nil))

	f.Fuzz(func(t *testing.T, lsn uint64, payload []byte) {
		if len(payload) > fuzzMaxRecordBytes {
			t.Skip("payload exceeds the record limit; not a round-trip case")
		}

		encoded, err := Encode(nil, lsn, payload)
		require.NoErrorf(t, err, "Encode(lsn=%d, %d bytes)", lsn, len(payload))

		scanner := NewScanner(bytes.NewReader(encoded), fuzzMaxRecordBytes)
		require.Truef(t, scanner.Next(), "expected one record, got none (err=%v)", scanner.Err())

		decoded := scanner.Record()
		require.Equal(t, lsn, decoded.LSN, "LSN must survive the round trip")
		// bytes.Equal (not require.Equal) because an empty payload round-trips to a
		// nil slice, and testify's Equal treats []byte(nil) and []byte{} as unequal.
		require.Truef(t, bytes.Equal(payload, decoded.Payload),
			"payload must survive the round trip: got %q, want %q", decoded.Payload, payload)

		require.False(t, scanner.Next(), "unexpected extra record after a single encoded record")
		require.NoError(t, scanner.Err(), "unexpected scan error after round trip")
	})
}
