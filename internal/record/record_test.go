package record

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/suite"
)

// fullByteRange returns a 256-byte payload covering every possible byte value,
// exercising encoders that might mishandle NUL bytes or high bits.
func fullByteRange() []byte {
	b := make([]byte, 256)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}

// realistic WAL payloads resembling serialized state-change commands.
var (
	setCommand    = []byte(`{"op":"set","key":"account:1001","value":{"balance":250000,"currency":"USD"}}`)
	deleteCommand = []byte(`{"op":"delete","key":"session:9f3c1a7e-7b2d-4e51-bc44-6d2a0f1e88aa"}`)
	unicodeNote   = []byte("note: transfer 250 000 ₴ → résumé τέλος 完了 ✅")
	largeBlob     = bytes.Repeat([]byte("snapshot-chunk;"), 8192) // ~120 KiB
)

type RecordSuite struct {
	suite.Suite
}

func TestRecordSuite(t *testing.T) {
	suite.Run(t, new(RecordSuite))
}

func (s *RecordSuite) TestEncodedSize() {
	cases := []struct {
		name       string
		payloadLen int
	}{
		{"empty", 0},
		{"set command", len(setCommand)},
		{"full byte range", 256},
		{"large snapshot blob", len(largeBlob)},
	}
	for _, tc := range cases {
		s.Run(tc.name, func() {
			s.Require().Equal(HeaderSize+tc.payloadLen, EncodedSize(tc.payloadLen))
		})
	}
}

func (s *RecordSuite) TestEncodeLayout() {
	cases := []struct {
		name    string
		lsn     uint64
		payload []byte
	}{
		{"set command", 4096, setCommand},
		{"delete command", 4097, deleteCommand},
		{"unicode note", 1, unicodeNote},
		{"binary full range", 1 << 40, fullByteRange()},
		{"empty payload", 7, nil},
		{"large blob", 123456789, largeBlob},
	}
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	for _, tc := range cases {
		s.Run(tc.name, func() {
			buf, err := Encode(nil, tc.lsn, tc.payload)
			s.Require().NoError(err)

			s.Require().Len(buf, HeaderSize+len(tc.payload))
			gotLen := binary.LittleEndian.Uint32(buf[4:8])
			s.Require().Equal(uint32(len(tc.payload)), gotLen, "length field")
			gotLSN := binary.LittleEndian.Uint64(buf[8:16])
			s.Require().Equal(tc.lsn, gotLSN, "lsn field")
			s.Require().True(bytes.Equal(buf[HeaderSize:], tc.payload), "payload bytes")

			wantCRC := crc32.Checksum(buf[4:], crcTab) // crc covers length||lsn||payload
			s.Require().Equal(wantCRC, binary.LittleEndian.Uint32(buf[0:4]), "crc")
		})
	}
}

func (s *RecordSuite) TestEncodePreservesExistingBuffer() {
	// Simulate batching: two records written into one growing buffer.
	first, err := Encode(nil, 100, setCommand)
	s.Require().NoError(err)

	combined, err := Encode(first, 101, deleteCommand)
	s.Require().NoError(err)

	s.Require().True(bytes.HasPrefix(combined, first), "earlier record must be left intact")
	s.Require().Len(combined, EncodedSize(len(setCommand))+EncodedSize(len(deleteCommand)))

	secondLSN := binary.LittleEndian.Uint64(combined[len(first)+8 : len(first)+16])
	s.Require().Equal(uint64(101), secondLSN, "second record LSN")
}
