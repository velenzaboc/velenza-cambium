//go:build windows

package gcpauth

import (
	"fmt"
	"os"
	"os/exec"
	"testing"
)

// ensureCredentialFixtureOwnedByCurrentUser makes a just-written test
// credential file deterministically pass validateCredentialFileAccess,
// regardless of what the host's inherited owner and DACL defaulted to.
// On Windows workstations, two steps are required:
//
//  1. A file created by an administrator account can default to owner
//     BUILTIN\Administrators (a group) instead of that account's own SID,
//     which fails the "must be owned by the effective user" check.
//     /setowner alone fixes this.
//  2. Fixing only the owner still leaves the file's inherited DACL (from its
//     parent temp directory) carrying other principals, which independently
//     fails the "DACL grants access to an unapproved principal" check.
//     /inheritance:r /grant:r is needed too, exactly as the sibling
//     configureCredentialACL helper in file_access_windows_test.go already
//     does for its own fixtures.
//
// This is the same underlying shape that made a real gcloud-written
// credential file fail validation before OWNER RIGHTS was recognized
// (velenzaboc/velenza-cambium#13, velenzaboc/velenza-canon#82). Every test in this
// package that constructs a credential file for validateCredentialFileAccess
// to read needs this, not just the Windows-specific access tests, so it
// lives as its own build-tagged helper rather than being folded into just
// one of them.
func ensureCredentialFixtureOwnedByCurrentUser(t *testing.T, path string) {
	t.Helper()
	username := os.Getenv("USERNAME")
	if username == "" {
		t.Fatal("USERNAME is empty; cannot construct a user-owned credential fixture")
	}
	if output, err := exec.Command("icacls", path, "/setowner", username).CombinedOutput(); err != nil {
		t.Fatalf("icacls set fixture owner: %v: %s", err, output)
	}
	command := exec.Command("icacls", path,
		"/inheritance:r",
		"/grant:r",
		fmt.Sprintf("%s:(F)", username),
		"SYSTEM:(F)",
		"Administrators:(F)",
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("icacls reset fixture DACL: %v: %s", err, output)
	}
}
