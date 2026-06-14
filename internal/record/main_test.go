package record

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain runs every test in the package under goleak so that any goroutine
// leaked by the record layer fails the suite.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
