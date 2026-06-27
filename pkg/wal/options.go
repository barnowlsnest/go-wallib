package wal

import (
	"time"

	"github.com/barnowlsnest/go-logslib/v2/pkg/logger"
)

// SyncPolicy controls when data is fsynced relative to acknowledging an append.
type SyncPolicy int

const (
	// SyncImmediate fsyncs before acknowledging each append (strongest, slowest).
	SyncImmediate SyncPolicy = iota
	// SyncBatched group-commits: one fsync per batch before acknowledging it.
	SyncBatched
	// SyncInterval acknowledges after the OS write; a background goroutine fsyncs
	// periodically (fastest, with a bounded data-loss window).
	SyncInterval
)

// Field is a structured log field. It is a type alias for go-logslib's
// logger.Field so that a *logger.Logger (or *logger.ContextLogger) satisfies
// Logger directly, without any adapter.
type Field = logger.Field

// Logger is the structured logging hook used by the WAL. Its method set is a
// subset of go-logslib's *logger.Logger, so an instance of that logger can be
// passed to WithLogger as-is.
type Logger interface {
	Debug(msg string, fields ...Field)
	Info(msg string, fields ...Field)
	Warn(msg string, fields ...Field)
	Error(msg string, fields ...Field)
}

// noopLogger is the default Logger; it discards everything.
type noopLogger struct{}

func (noopLogger) Debug(string, ...Field) {}
func (noopLogger) Info(string, ...Field)  {}
func (noopLogger) Warn(string, ...Field)  {}
func (noopLogger) Error(string, ...Field) {}

// Default option values.
const (
	defaultMaxSegmentSize      = 64 << 20 // 64 MiB soft roll threshold
	defaultMaxRecordSize       = 64 << 20 // 64 MiB hard per-record limit
	defaultBatchSize           = 256
	defaultBatchTimeout        = 2 * time.Millisecond
	defaultFlushInterval       = 100 * time.Millisecond
	defaultMaxOperationTimeout = 30 * time.Second
)

// options holds the resolved configuration for a WAL. Fields are ordered to
// minimize struct padding (the interface, which carries a pointer, comes first).
type options struct {
	logger              Logger
	maxSegmentSize      int64
	maxOperationTimeout time.Duration
	maxRecordSize       int
	batchSize           int
	batchTimeout        time.Duration
	flushInterval       time.Duration
	syncPolicy          SyncPolicy
}

// defaultOptions returns the configuration used when no Option overrides it.
func defaultOptions() options {
	return options{
		logger:              noopLogger{},
		maxSegmentSize:      defaultMaxSegmentSize,
		maxRecordSize:       defaultMaxRecordSize,
		batchSize:           defaultBatchSize,
		batchTimeout:        defaultBatchTimeout,
		flushInterval:       defaultFlushInterval,
		syncPolicy:          SyncBatched,
		maxOperationTimeout: defaultMaxOperationTimeout,
	}
}

// Option configures a WAL at Open time.
type Option func(*options)

// WithSyncPolicy sets the durability policy. Default: SyncBatched.
func WithSyncPolicy(policy SyncPolicy) Option {
	return func(o *options) { o.syncPolicy = policy }
}

// WithMaxSegmentSize sets the soft segment-roll threshold in bytes. Default 64 MiB.
func WithMaxSegmentSize(bytes int64) Option {
	return func(o *options) { o.maxSegmentSize = bytes }
}

// WithMaxRecordSize sets the hard per-record limit in bytes. Default 64 MiB.
func WithMaxRecordSize(bytes int) Option {
	return func(o *options) { o.maxRecordSize = bytes }
}

// WithBatchSize caps the number of appends coalesced into one group commit.
func WithBatchSize(count int) Option {
	return func(o *options) { o.batchSize = count }
}

// WithBatchTimeout sets the group-commit linger window for SyncBatched.
func WithBatchTimeout(window time.Duration) Option {
	return func(o *options) { o.batchTimeout = window }
}

// WithFlushInterval sets the background fsync period for SyncInterval.
func WithFlushInterval(period time.Duration) Option {
	return func(o *options) { o.flushInterval = period }
}

func WithMaxOperationTimeout(timeout time.Duration) Option {
	return func(o *options) { o.maxOperationTimeout = timeout }
}

// WithLogger sets an optional logger. A nil logger is ignored, leaving the
// default no-op logger in place.
func WithLogger(customLogger Logger) Option {
	return func(o *options) {
		if customLogger != nil {
			o.logger = customLogger
		}
	}
}
