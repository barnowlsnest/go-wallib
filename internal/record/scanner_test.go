package record

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/suite"
)

// ScannerSuite groups all Scanner tests.
type ScannerSuite struct {
	suite.Suite
}

func TestScannerSuite(t *testing.T) {
	suite.Run(t, new(ScannerSuite))
}

// encodeRecords encodes a slice of Records into one buffer, asserting NoError.
func (s *ScannerSuite) encodeRecords(records []Record) []byte {
	var buf []byte
	for _, rec := range records {
		var err error
		buf, err = Encode(buf, rec.LSN, rec.Payload)
		s.Require().NoError(err, "Encode must not fail for test record lsn=%d", rec.LSN)
	}
	return buf
}

// scanAll drains a scanner, returning every valid record (with copied payloads,
// since the scanner reuses its buffer) plus the terminal error and offset.
func scanAll(scanner *Scanner) (records []Record, offset int64, err error) {
	for scanner.Next() {
		current := scanner.Record()
		records = append(records, Record{LSN: current.LSN, Payload: clonePayload(current.Payload)})
	}
	return records, scanner.Offset(), scanner.Err()
}

func clonePayload(payload []byte) []byte {
	if payload == nil {
		return nil
	}
	return bytes.Clone(payload)
}

// concat joins byte slices into a fresh allocation (no aliasing of inputs).
func concat(parts ...[]byte) []byte {
	return bytes.Join(parts, nil)
}

// Realistic WAL record fixtures — representative of real workloads.
var (
	scannerSetCmd    = []byte(`{"op":"set","key":"user:88421","value":{"name":"Олена Ткаченко","role":"admin","mfa":true}}`)
	scannerDeleteCmd = []byte(`{"op":"delete","key":"session:c3d2e1f0-a1b2-4c3d-8e9f-0a1b2c3d4e5f","reason":"logout"}`)
	scannerUnicode   = []byte("payload: ₿itcoin трансфер → résumé τέλος 完了 🔒 NUL\x00 byte inside")
	scannerBinBlob   = fullByteRange()
	scannerLargeBlob = bytes.Repeat([]byte("wal-segment-data-block;"), 4096) // ~90 KiB
)

// defaultMaxRecordBytes is a realistic per-record cap larger than any fixture.
const defaultMaxRecordBytes = 4 * 1024 * 1024 // 4 MiB

// testRecords is the canonical set of diverse records used across tests.
var testRecords = []Record{
	{LSN: 1_000_000, Payload: scannerSetCmd},
	{LSN: 1_000_001, Payload: scannerDeleteCmd},
	{LSN: 1_000_002, Payload: scannerUnicode},
	{LSN: 1_000_003, Payload: scannerBinBlob},
	{LSN: 1_000_004, Payload: scannerLargeBlob},
	{LSN: 1_000_005, Payload: nil}, // empty payload is valid
}

// TestScanOutcomes is the table-driven core: each case feeds a byte stream to a
// scanner and asserts the records recovered, the terminal error, and the
// truncation offset. Clean reads, torn tails, CRC corruption, and oversized
// records all share this shape.
func (s *ScannerSuite) TestScanOutcomes() {
	full := s.encodeRecords(testRecords)
	twoGood := s.encodeRecords(testRecords[:2])
	threeGood := s.encodeRecords(testRecords[:3])
	thirdRecord := s.encodeRecords(testRecords[2:3])

	// CRC corruption: flip the first payload byte of record index 1.
	corrupted := bytes.Clone(s.encodeRecords(testRecords[:4]))
	rec0Bytes := EncodedSize(len(testRecords[0].Payload))
	corrupted[rec0Bytes+HeaderSize] ^= 0xFF

	// Oversized records, scanned with a small max that fits the leaders but not
	// the offending record.
	smallA := Record{LSN: 2_000_001, Payload: []byte(`{"op":"set","key":"cfg:timeout","value":30}`)}
	smallB := Record{LSN: 2_000_002, Payload: []byte(`{"op":"set","key":"cfg:retries","value":3}`)}
	oversized := Record{LSN: 2_000_003, Payload: bytes.Repeat([]byte("X"), 512)}
	leadersThenBig := s.encodeRecords([]Record{smallA, smallB, oversized})

	cases := []struct {
		name        string
		stream      []byte
		maxRecBytes int
		wantRecords []Record
		wantOffset  int64
		wantErr     error
	}{
		{
			name:        "clean round trip of all records",
			stream:      full,
			maxRecBytes: defaultMaxRecordBytes,
			wantRecords: testRecords,
			wantOffset:  int64(len(full)),
			wantErr:     nil,
		},
		{
			name:        "torn tail partial header",
			stream:      concat(twoGood, thirdRecord[:8]), // 8 of 16 header bytes
			maxRecBytes: defaultMaxRecordBytes,
			wantRecords: testRecords[:2],
			wantOffset:  int64(len(twoGood)),
			wantErr:     ErrTorn,
		},
		{
			name:        "torn tail partial payload",
			stream:      concat(twoGood, thirdRecord[:HeaderSize+len(testRecords[2].Payload)/2]),
			maxRecBytes: defaultMaxRecordBytes,
			wantRecords: testRecords[:2],
			wantOffset:  int64(len(twoGood)),
			wantErr:     ErrTorn,
		},
		{
			name:        "torn tail single stray byte",
			stream:      concat(threeGood, []byte{0xAB}),
			maxRecBytes: defaultMaxRecordBytes,
			wantRecords: testRecords[:3],
			wantOffset:  int64(len(threeGood)),
			wantErr:     ErrTorn,
		},
		{
			name:        "corrupt crc mid stream",
			stream:      corrupted,
			maxRecBytes: defaultMaxRecordBytes,
			wantRecords: testRecords[:1],
			wantOffset:  int64(rec0Bytes),
			wantErr:     ErrCorrupt,
		},
		{
			name:        "oversized first record",
			stream:      s.encodeRecords([]Record{oversized}),
			maxRecBytes: len(oversized.Payload) - 1,
			wantRecords: nil,
			wantOffset:  0,
			wantErr:     ErrTooLarge,
		},
		{
			name:        "oversized record after good records",
			stream:      leadersThenBig,
			maxRecBytes: 128,
			wantRecords: []Record{smallA, smallB},
			wantOffset:  int64(EncodedSize(len(smallA.Payload)) + EncodedSize(len(smallB.Payload))),
			wantErr:     ErrTooLarge,
		},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			records, offset, err := scanAll(NewScanner(bytes.NewReader(tc.stream), tc.maxRecBytes))

			if tc.wantErr == nil {
				s.Require().NoError(err)
			} else {
				s.Require().ErrorIs(err, tc.wantErr)
			}
			s.Require().Equal(tc.wantRecords, records)
			s.Require().Equal(tc.wantOffset, offset)
		})
	}
}

// TestOffsetAdvancesPerRecord verifies the running offset matches the cumulative
// encoded size after each individual record (not just at the end).
func (s *ScannerSuite) TestOffsetAdvancesPerRecord() {
	encoded := s.encodeRecords(testRecords)
	scanner := NewScanner(bytes.NewReader(encoded), defaultMaxRecordBytes)

	var cumulative int64
	for i := 0; scanner.Next(); i++ {
		cumulative += int64(EncodedSize(len(scanner.Record().Payload)))
		s.Require().Equal(cumulative, scanner.Offset(), "offset after record %d", i)
	}
	s.Require().NoError(scanner.Err())
	s.Require().Equal(int64(len(encoded)), scanner.Offset())
}
