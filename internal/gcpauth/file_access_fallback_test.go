package gcpauth

import (
	"strings"
	"testing"
)

func TestUnsupportedCredentialFileAccessFailsClosed(t *testing.T) {
	err := unsupportedCredentialFileAccess(nil, nil)
	if err == nil || !strings.Contains(err.Error(), "unsupported on this operating system") {
		t.Fatalf("unsupported platform validation must fail closed, got %v", err)
	}
}
