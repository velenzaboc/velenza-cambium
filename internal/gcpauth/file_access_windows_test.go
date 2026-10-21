//go:build windows

package gcpauth

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// configureCredentialACL constructs the exact minimal DACL the production
// check admits. icacls is Windows' native ACL tool and is present on the
// Windows runner that executes this build-constrained test.
//
// Explicitly sets the owner rather than trusting whatever os.WriteFile left
// behind: on Windows workstations, a file created by an
// administrator account can default to owner BUILTIN\Administrators (a
// group) instead of that account's own SID — the same shape that made a
// real gcloud-written credential file fail validation. /grant:r alone does
// not change the owner, so a test relying on the ambient default is only as
// reliable as whatever quirk the current host happens to have.
func configureCredentialACL(t *testing.T, path string) {
	t.Helper()
	username := os.Getenv("USERNAME")
	if username == "" {
		t.Fatal("USERNAME is empty; cannot construct a user-owned credential ACL")
	}
	if output, err := exec.Command("icacls", path, "/setowner", username).CombinedOutput(); err != nil {
		t.Fatalf("icacls set owner: %v: %s", err, output)
	}
	command := exec.Command("icacls", path,
		"/inheritance:r",
		"/grant:r",
		fmt.Sprintf("%s:(F)", username),
		"SYSTEM:(F)",
		"Administrators:(F)",
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("icacls minimal credential ACL: %v: %s", err, output)
	}
}

func openCredentialForAccessTest(t *testing.T) (*os.File, os.FileInfo) {
	t.Helper()
	path := t.TempDir() + "\\credential.json"
	if err := os.WriteFile(path, []byte(`{"type":"authorized_user"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	configureCredentialACL(t, path)
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	return file, info
}

func TestValidateCredentialFileAccessWindowsAllowsOwnerSystemAndAdministrators(t *testing.T) {
	file, info := openCredentialForAccessTest(t)
	if err := validateCredentialFileAccess(file, info); err != nil {
		t.Fatalf("validateCredentialFileAccess(minimal DACL) = %v, want nil", err)
	}
}

func TestValidateCredentialFileAccessWindowsAllowsInheritedOwnerRightsACE(t *testing.T) {
	// Reproduces the shape Windows attaches by default via inheritance: an
	// explicit grant to the current user/SYSTEM/Administrators, plus the
	// well-known OWNER RIGHTS (S-1-3-4) placeholder principal. On Windows
	// workstations, a gcloud-written legacy_credentials adc.json for a
	// workstation service account carried NT AUTHORITY\SYSTEM:(I)(F),
	// BUILTIN\Administrators:(I)(F), OWNER RIGHTS:(I)(F) and was refused
	// with "unapproved principal" until OWNER RIGHTS was recognized.
	file, info := openCredentialForAccessTest(t)
	username := os.Getenv("USERNAME")
	if output, err := exec.Command("icacls", file.Name(),
		"/inheritance:r",
		"/grant:r",
		fmt.Sprintf("%s:(F)", username),
		"SYSTEM:(F)",
		"Administrators:(F)",
		"*S-1-3-4:(F)",
	).CombinedOutput(); err != nil {
		t.Fatalf("icacls add OWNER RIGHTS ACE: %v: %s", err, output)
	}
	if err := validateCredentialFileAccess(file, info); err != nil {
		t.Fatalf("validateCredentialFileAccess(OWNER RIGHTS ACE present) = %v, want nil", err)
	}
}

func TestValidateCredentialFileAccessWindowsRejectsUnapprovedDACLPrincipal(t *testing.T) {
	file, info := openCredentialForAccessTest(t)
	if output, err := exec.Command("icacls", file.Name(), "/grant", "Everyone:(R)").CombinedOutput(); err != nil {
		t.Fatalf("icacls add unapproved principal: %v: %s", err, output)
	}
	err := validateCredentialFileAccess(file, info)
	if err == nil || !strings.Contains(err.Error(), "unapproved principal") {
		t.Fatalf("validateCredentialFileAccess(unapproved DACL) = %v, want unapproved-principal refusal", err)
	}
}

func TestValidateCredentialFileAccessWindowsRejectsNonUserOwner(t *testing.T) {
	file, info := openCredentialForAccessTest(t)
	if output, err := exec.Command("icacls", file.Name(), "/setowner", "Administrators").CombinedOutput(); err != nil {
		t.Fatalf("icacls set non-user owner: %v: %s", err, output)
	}
	err := validateCredentialFileAccess(file, info)
	if err == nil || !strings.Contains(err.Error(), "owned by the effective user") {
		t.Fatalf("validateCredentialFileAccess(non-user owner) = %v, want owner refusal", err)
	}
}

func TestValidateCredentialFileAccessWindowsRejectsNullHandle(t *testing.T) {
	file := os.NewFile(0, "invalid-security-handle")
	if file == nil {
		t.Fatal("os.NewFile(NULL) = nil, want a file wrapper carrying the invalid handle")
	}
	t.Cleanup(func() { _ = file.Close() })
	err := validateCredentialFileAccess(file, nil)
	if err == nil || !strings.Contains(err.Error(), "read credential file security descriptor") {
		t.Fatalf("validateCredentialFileAccess(NULL handle) = %v, want descriptor refusal", err)
	}
}

func TestValidateCredentialFileAccessWindowsAllowsRestrictiveDenyACE(t *testing.T) {
	file, info := openCredentialForAccessTest(t)
	if output, err := exec.Command("icacls", file.Name(), "/deny", "Everyone:(R)").CombinedOutput(); err != nil {
		t.Fatalf("icacls add restrictive deny ACE: %v: %s", err, output)
	}
	if err := validateCredentialFileAccess(file, info); err != nil {
		t.Fatalf("validateCredentialFileAccess(restrictive deny ACE) = %v, want nil", err)
	}
}
