// Package record implements CRC32C-checksummed framing for a single WAL record.
//
// On-disk record layout (all integers little-endian):
//
//	+---------+---------+---------+------------------+
//	| CRC32C  | Length  |   LSN   |     Payload      |
//	| 4 bytes | 4 bytes | 8 bytes |  Length bytes    |
//	+---------+---------+---------+------------------+
//
// CRC32C is computed over Length || LSN || Payload.
package record

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
)

// HeaderSize is the fixed per-record framing overhead: crc(4)+length(4)+lsn(8).
const HeaderSize = 16

// MaxPayloadSize is the largest payload the on-disk Length field (uint32) can
// represent. Callers enforce a smaller MaxRecordSize.
const MaxPayloadSize = math.MaxUint32

// ErrPayloadTooLarge is returned by Encode when a payload cannot be represented
// by the uint32 Length field.
var ErrPayloadTooLarge = errors.New("wal/record: payload exceeds MaxPayloadSize")

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Record is a decoded log record.
type Record struct {
	LSN     uint64
	Payload []byte
}

// EncodedSize returns the on-disk byte size of a record carrying a payload of
// payloadLen bytes.
func EncodedSize(payloadLen int) int { return HeaderSize + payloadLen }

// Encode appends the framed record for (lsn, payload) to dst and returns the
// extended slice. dst may be nil. It returns ErrPayloadTooLarge if the payload
// cannot be represented by the uint32 Length field.
func Encode(dst []byte, lsn uint64, payload []byte) ([]byte, error) {
	n := len(payload)
	if n < 0 || n > MaxPayloadSize {
		return nil, ErrPayloadTooLarge
	}
	var hdr [HeaderSize]byte
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(n))
	binary.LittleEndian.PutUint64(hdr[8:16], lsn)

	// crc32.Update returns only a checksum (no error to handle/suppress).
	crc := crc32.Update(0, crcTable, hdr[4:16])
	crc = crc32.Update(crc, crcTable, payload)
	binary.LittleEndian.PutUint32(hdr[0:4], crc)

	dst = append(dst, hdr[:]...)
	dst = append(dst, payload...)
	return dst, nil
}
