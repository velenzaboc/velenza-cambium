//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package gcpauth

import (
	"fmt"
	"os"
	"syscall"
)

func validateCredentialFileAccess(_ *os.File, info os.FileInfo) error {
	permissions := info.Mode().Perm()
	if permissions != 0o400 && permissions != 0o600 {
		return fmt.Errorf("credential file must be owner-only and owner-readable (mode 0400 or 0600)")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() {
		return fmt.Errorf("credential file must be owned by the effective user")
	}
	return nil
}
