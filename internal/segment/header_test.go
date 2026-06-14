package segment

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/suite"
)

// HeaderSuite groups the segment header encode/decode tests.
type HeaderSuite struct {
	suite.Suite
}

func TestHeaderSuite(t *testing.T) {
	suite.Run(t, new(HeaderSuite))
}

// realistic header field values resembling a live segment.
const (
	sampleBaseLSN   uint64 = 4_200_000
	sampleCreatedAt int64  = 1_718_000_000_000_000_000 // ~2024-06-10 in Unix ns
)

func (s *HeaderSuite) TestRoundTrip() {
	cases := []struct {
		name   string
		header Header
	}{
		{
			name:   "typical live segment",
			header: Header{Version: Version, Flags: 0, BaseLSN: sampleBaseLSN, CreatedAt: sampleCreatedAt},
		},
		{
			name:   "first segment of a fresh log",
			header: Header{Version: Version, Flags: 0, BaseLSN: 1, CreatedAt: sampleCreatedAt},
		},
		{
			name:   "max base lsn and reserved flags set",
			header: Header{Version: Version, Flags: 0xBEEF, BaseLSN: 1<<63 + 7, CreatedAt: sampleCreatedAt},
		},
	}
	for _, tc := range cases {
		s.Run(tc.name, func() {
			encoded := EncodeHeader(tc.header)
			s.Require().Len(encoded, HeaderSize)
			s.Require().True(bytes.HasPrefix(encoded, magic[:]), "header must start with magic")

			decoded, err := DecodeHeader(encoded)
			s.Require().NoError(err)
			s.Require().Equal(tc.header, decoded)
		})
	}
}

func (s *HeaderSuite) TestDecodeErrors() {
	good := EncodeHeader(Header{Version: Version, BaseLSN: sampleBaseLSN, CreatedAt: sampleCreatedAt})

	cases := []struct {
		name    string
		corrupt func([]byte) []byte
		wantErr error
	}{
		{
			name:    "truncated header",
			corrupt: func(b []byte) []byte { return b[:HeaderSize-8] },
			wantErr: ErrCorrupt,
		},
		{
			name: "bad magic",
			corrupt: func(b []byte) []byte {
				mangled := bytes.Clone(b)
				mangled[0] ^= 0xFF
				return mangled
			},
			wantErr: ErrBadMagic,
		},
		{
			name: "stale header crc after field tamper",
			corrupt: func(b []byte) []byte {
				mangled := bytes.Clone(b)
				mangled[8] ^= 0xFF // flip a BaseLSN byte, leave the CRC untouched
				return mangled
			},
			wantErr: ErrCorrupt,
		},
		{
			name: "unsupported version",
			corrupt: func(_ []byte) []byte {
				return EncodeHeader(Header{Version: 999, BaseLSN: sampleBaseLSN, CreatedAt: sampleCreatedAt})
			},
			wantErr: ErrUnknownVersion,
		},
	}
	for _, tc := range cases {
		s.Run(tc.name, func() {
			_, err := DecodeHeader(tc.corrupt(good))
			s.Require().ErrorIs(err, tc.wantErr)
		})
	}
}

// TestHeaderByteLayout pins the exact on-disk offsets so an accidental field
// reorder or width change is caught.
func (s *HeaderSuite) TestHeaderByteLayout() {
	encoded := EncodeHeader(Header{Version: Version, Flags: 0, BaseLSN: sampleBaseLSN, CreatedAt: sampleCreatedAt})

	s.Require().Equal(Version, binary.LittleEndian.Uint16(encoded[4:6]), "version offset")
	s.Require().Equal(uint16(0), binary.LittleEndian.Uint16(encoded[6:8]), "flags offset")
	s.Require().Equal(sampleBaseLSN, binary.LittleEndian.Uint64(encoded[8:16]), "base lsn offset")
	s.Require().Equal(uint64(sampleCreatedAt), binary.LittleEndian.Uint64(encoded[16:24]), "created-at offset")
}
