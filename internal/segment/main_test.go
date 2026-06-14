package segment

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain runs every test in the package under goleak so a leaked goroutine
// fails the suite.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
