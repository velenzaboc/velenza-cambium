//go:build !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris && !windows

package gcpauth

var validateCredentialFileAccess = unsupportedCredentialFileAccess
