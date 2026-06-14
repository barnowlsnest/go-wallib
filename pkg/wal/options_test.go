package wal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/barnowlsnest/go-logslib/v2/pkg/logger"
)

// Compile-time proof that go-logslib's loggers satisfy our Logger interface, so
// callers can pass them to WithLogger directly.
var (
	_ Logger = (*logger.Logger)(nil)
	_ Logger = (*logger.ContextLogger)(nil)
)

// mockLogger is a testify mock of the Logger interface.
type mockLogger struct {
	mock.Mock
}

func (m *mockLogger) Debug(msg string, fields ...Field) { m.Called(msg, fields) }
func (m *mockLogger) Info(msg string, fields ...Field)  { m.Called(msg, fields) }
func (m *mockLogger) Warn(msg string, fields ...Field)  { m.Called(msg, fields) }
func (m *mockLogger) Error(msg string, fields ...Field) { m.Called(msg, fields) }

// OptionsSuite covers default configuration and the functional options.
type OptionsSuite struct {
	suite.Suite
}

func TestOptionsSuite(t *testing.T) {
	suite.Run(t, new(OptionsSuite))
}

func (s *OptionsSuite) TestDefaults() {
	o := defaultOptions()

	s.Require().Equal(SyncBatched, o.syncPolicy)
	s.Require().Equal(int64(defaultMaxSegmentSize), o.maxSegmentSize)
	s.Require().Equal(defaultMaxRecordSize, o.maxRecordSize)
	s.Require().Positive(o.batchSize)
	s.Require().Positive(o.batchTimeout)
	s.Require().Positive(o.flushInterval)
	s.Require().NotNil(o.logger, "logger must default to a non-nil no-op")
}

func (s *OptionsSuite) TestOptionsApply() {
	o := defaultOptions()

	for _, apply := range []Option{
		WithSyncPolicy(SyncImmediate),
		WithMaxSegmentSize(1 << 20),
		WithMaxRecordSize(2 << 20),
		WithBatchSize(8),
		WithBatchTimeout(5 * time.Millisecond),
		WithFlushInterval(50 * time.Millisecond),
	} {
		apply(&o)
	}

	s.Require().Equal(SyncImmediate, o.syncPolicy)
	s.Require().Equal(int64(1<<20), o.maxSegmentSize)
	s.Require().Equal(2<<20, o.maxRecordSize)
	s.Require().Equal(8, o.batchSize)
	s.Require().Equal(5*time.Millisecond, o.batchTimeout)
	s.Require().Equal(50*time.Millisecond, o.flushInterval)
}

func (s *OptionsSuite) TestWithLoggerStoresAndInvokes() {
	spy := new(mockLogger)
	spy.On("Info", "recovered log", mock.Anything).Once()

	o := defaultOptions()
	WithLogger(spy)(&o)

	o.logger.Info("recovered log", Field{Key: "entries", Value: 42})

	spy.AssertExpectations(s.T())
}

func (s *OptionsSuite) TestWithLoggerIgnoresNil() {
	o := defaultOptions()
	original := o.logger

	WithLogger(nil)(&o)

	s.Require().Equal(original, o.logger, "nil logger must not replace the default")
}
