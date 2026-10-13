package main

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the package when any test leaks a goroutine.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
