package wal

import "errors"

// Sentinel errors returned by the WAL. All are checkable with errors.Is.
var (
	// ErrClosed is returned by any operation on a WAL that has been closed.
	ErrClosed = errors.New("wal: closed")

	// ErrCorrupt indicates unrecoverable corruption detected during recovery,
	// such as a checksum mismatch in a non-final segment or non-contiguous
	// segment base LSNs.
	ErrCorrupt = errors.New("wal: corrupt log")

	// ErrRecordTooLarge is returned by Append when a payload exceeds the
	// configured maximum record size.
	ErrRecordTooLarge = errors.New("wal: record exceeds max size")

	// ErrInvalidLSN is returned when an LSN argument is outside the valid range,
	// for example a Truncate or Replay bound beyond the current LastLSN.
	ErrInvalidLSN = errors.New("wal: invalid lsn")
)
