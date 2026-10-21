//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package gcpauth

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

type credentialAccessFileInfo struct {
	system any
}

func (credentialAccessFileInfo) Name() string       { return "credential.json" }
func (credentialAccessFileInfo) Size() int64        { return 0 }
func (credentialAccessFileInfo) Mode() os.FileMode  { return 0o600 }
func (credentialAccessFileInfo) ModTime() time.Time { return time.Time{} }
func (credentialAccessFileInfo) IsDir() bool        { return false }
func (i credentialAccessFileInfo) Sys() any         { return i.system }

func TestReadOwnerOnlyRegularFileRejectsFinalPathSymlink(t *testing.T) {
	directory := t.TempDir()
	target := filepath.Join(directory, "carrier.json")
	if err := os.WriteFile(target, []byte(`{"type":"fixture"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(directory, "application_default_credentials.json")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := readOwnerOnlyRegularFile(link); err == nil || !strings.Contains(err.Error(), "must not be a symlink") {
		t.Fatalf("symlink error = %v", err)
	}
}

func TestValidateCredentialFileAccessUnixRejectsUntrustedOwnershipMetadata(t *testing.T) {
	for _, tt := range []struct {
		name string
		info os.FileInfo
	}{
		{name: "wrong metadata type", info: credentialAccessFileInfo{system: struct{}{}}},
		{name: "different owner", info: credentialAccessFileInfo{system: &syscall.Stat_t{Uid: uint32(os.Geteuid() + 1)}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateCredentialFileAccess(nil, tt.info); err == nil || !strings.Contains(err.Error(), "owned by the effective user") {
				t.Fatalf("ownership metadata error = %v", err)
			}
		})
	}
}
