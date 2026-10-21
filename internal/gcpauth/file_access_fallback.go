package gcpauth

import (
	"fmt"
	"os"
)

// unsupportedCredentialFileAccess is deliberately untagged so its fail-closed
// behavior is testable on every supported test runner. Unsupported OS builds
// bind validateCredentialFileAccess to this function in their declaration-only
// selector file.
func unsupportedCredentialFileAccess(_ *os.File, _ os.FileInfo) error {
	return fmt.Errorf("credential file access validation is unsupported on this operating system")
}
