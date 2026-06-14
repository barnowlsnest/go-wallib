package wal

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/suite"
)

// TypesSuite covers the public sentinel errors and zero-value types.
type TypesSuite struct {
	suite.Suite
}

func TestTypesSuite(t *testing.T) {
	suite.Run(t, new(TypesSuite))
}

func (s *TypesSuite) TestSentinelErrorsAreDistinct() {
	sentinels := map[string]error{
		"ErrClosed":         ErrClosed,
		"ErrCorrupt":        ErrCorrupt,
		"ErrRecordTooLarge": ErrRecordTooLarge,
		"ErrInvalidLSN":     ErrInvalidLSN,
	}

	for nameA, errA := range sentinels {
		for nameB, errB := range sentinels {
			if nameA == nameB {
				continue
			}

			s.Require().Falsef(errors.Is(errA, errB), "%s must not alias %s", nameA, nameB)
		}
	}
}

func (s *TypesSuite) TestSentinelErrorsHaveMessages() {
	for _, err := range []error{ErrClosed, ErrCorrupt, ErrRecordTooLarge, ErrInvalidLSN} {
		s.Require().Error(err)
		s.Require().NotEmpty(err.Error())
	}
}

func (s *TypesSuite) TestEntryZeroValue() {
	var entry Entry
	s.Require().Equal(uint64(0), entry.LSN)
	s.Require().Nil(entry.Payload)
}

func (s *TypesSuite) TestRecoveryReportZeroValue() {
	var report RecoveryReport
	s.Require().Equal(RecoveryReport{
		EntriesRecovered: 0,
		FirstLSN:         0,
		LastLSN:          0,
		BytesTruncated:   0,
		SegmentsRemoved:  0,
	}, report)
}
