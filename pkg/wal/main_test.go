package wal

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain runs every test in the package under goleak so a leaked WAL
// goroutine (e.g. the writer that fails to stop on Close) fails the suite.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
