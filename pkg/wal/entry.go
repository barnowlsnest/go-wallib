package wal

// Entry is a single log record returned by readers and Replay. Fields are
// ordered to minimize struct padding (slice before the scalar LSN).
type Entry struct {
	// Payload is the opaque caller bytes. For a Reader it is only valid until the
	// next call to Next.
	Payload []byte
	// LSN is the unique, monotonic Log Sequence Number of this entry.
	LSN uint64
}

// RecoveryReport summarizes what Open found and repaired while recovering a log.
type RecoveryReport struct {
	// EntriesRecovered is the number of valid records found across all segments.
	EntriesRecovered uint64
	// FirstLSN is the lowest surviving LSN, or 0 when the log is empty.
	FirstLSN uint64
	// LastLSN is the highest surviving LSN, or 0 when the log is empty.
	LastLSN uint64
	// BytesTruncated is the number of bytes removed from a torn tail.
	BytesTruncated int64
	// SegmentsRemoved is the number of empty trailing segments deleted.
	SegmentsRemoved int
}
