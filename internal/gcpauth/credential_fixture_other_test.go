//go:build !windows

package gcpauth

import "testing"

// ensureCredentialFixtureOwnedByCurrentUser is a no-op outside Windows: the
// creating process's own user is always the new file's owner elsewhere, so
// there is nothing to correct. See the windows-tagged sibling for why this
// exists at all.
func ensureCredentialFixtureOwnedByCurrentUser(t *testing.T, path string) {
	t.Helper()
}
