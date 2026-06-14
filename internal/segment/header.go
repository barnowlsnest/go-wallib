// Package segment implements the on-disk WAL segment file: a fixed,
// self-describing header followed by framed records.
package segment

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// HeaderSize is the fixed segment header size in bytes.
const HeaderSize = 28

// Version is the current segment format version written by this package.
const Version uint16 = 1

// magic identifies a go-wal segment file. It is the first four bytes of every
// segment so a stray or foreign file cannot be mistaken for one.
var magic = [4]byte{'G', 'W', 'A', 'L'}

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Sentinel errors returned when decoding a segment header.
var (
	// ErrBadMagic indicates the file does not begin with the go-wal magic and is
	// not a segment file at all.
	ErrBadMagic = errors.New("wal/segment: bad magic")
	// ErrUnknownVersion indicates a well-formed header whose format version this
	// package does not understand.
	ErrUnknownVersion = errors.New("wal/segment: unknown format version")
	// ErrCorrupt indicates a truncated header or a header CRC mismatch.
	ErrCorrupt = errors.New("wal/segment: corrupt segment")
)

// Header is the decoded segment header.
//
// On-disk layout (all integers little-endian):
//
//	Magic(4) | Version(2) | Flags(2) | BaseLSN(8) | CreatedAt(8) | HeaderCRC(4)
//
// HeaderCRC is the CRC32C of the preceding 24 bytes. The struct fields are
// ordered for memory alignment, not to match the on-disk order — serialization
// uses explicit byte offsets below, so the two are independent.
type Header struct {
	// BaseLSN is the LSN of the first record this segment may contain.
	BaseLSN uint64
	// CreatedAt is the segment creation time in Unix nanoseconds. It is stored
	// unsigned so encode/decode need no signed-conversion; callers convert from
	// time.Now().UnixNano() at construction.
	CreatedAt uint64
	// Version is the segment format version.
	Version uint16
	// Flags is a reserved bitfield, currently always zero.
	Flags uint16
}

// EncodeHeader serializes header into a freshly allocated HeaderSize-byte slice,
// appending the header CRC.
func EncodeHeader(header Header) []byte {
	encoded := make([]byte, HeaderSize)
	copy(encoded[0:4], magic[:])
	binary.LittleEndian.PutUint16(encoded[4:6], header.Version)
	binary.LittleEndian.PutUint16(encoded[6:8], header.Flags)
	binary.LittleEndian.PutUint64(encoded[8:16], header.BaseLSN)
	binary.LittleEndian.PutUint64(encoded[16:24], header.CreatedAt)
	binary.LittleEndian.PutUint32(encoded[24:28], crc32.Checksum(encoded[0:24], crcTable))

	return encoded
}

// DecodeHeader parses and validates a segment header. It distinguishes a foreign
// file (ErrBadMagic), an unreadable/corrupt header (ErrCorrupt), and a header
// from an unsupported format version (ErrUnknownVersion).
func DecodeHeader(encoded []byte) (Header, error) {
	switch {
	case len(encoded) < HeaderSize:
		return Header{}, ErrCorrupt
	case [4]byte(encoded[0:4]) != magic:
		return Header{}, ErrBadMagic
	case crc32.Checksum(encoded[0:24], crcTable) != binary.LittleEndian.Uint32(encoded[24:28]):
		return Header{}, ErrCorrupt
	}

	header := Header{
		Version:   binary.LittleEndian.Uint16(encoded[4:6]),
		Flags:     binary.LittleEndian.Uint16(encoded[6:8]),
		BaseLSN:   binary.LittleEndian.Uint64(encoded[8:16]),
		CreatedAt: binary.LittleEndian.Uint64(encoded[16:24]),
	}
	if header.Version != Version {
		return Header{}, ErrUnknownVersion
	}

	return header, nil
}
