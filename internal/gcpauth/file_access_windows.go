//go:build windows

package gcpauth

import (
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

func validateCredentialFileAccess(file *os.File, _ os.FileInfo) error {
	descriptor, err := windows.GetSecurityInfo(windows.Handle(file.Fd()), windows.SE_FILE_OBJECT, windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		return fmt.Errorf("read credential file security descriptor: %w", err)
	}
	owner, _, err := descriptor.Owner()
	if err != nil || owner == nil {
		return fmt.Errorf("resolve credential file owner: %w", err)
	}
	currentUser, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return fmt.Errorf("resolve effective Windows user: %w", err)
	}
	if !owner.Equals(currentUser.User.Sid) {
		return fmt.Errorf("credential file must be owned by the effective user")
	}
	dacl, _, err := descriptor.DACL()
	if err != nil {
		return fmt.Errorf("resolve credential file DACL: %w", err)
	}
	if dacl == nil {
		return fmt.Errorf("credential file must have a restrictive DACL")
	}
	localSystem, err := windows.CreateWellKnownSid(windows.WinLocalSystemSid)
	if err != nil {
		return fmt.Errorf("resolve LocalSystem SID: %w", err)
	}
	administrators, err := windows.CreateWellKnownSid(windows.WinBuiltinAdministratorsSid)
	if err != nil {
		return fmt.Errorf("resolve Administrators SID: %w", err)
	}
	// OWNER RIGHTS (S-1-3-4) is a well-known placeholder principal, not a
	// grant to any other account: it lets the current owner's own rights be
	// edited independently of the Administrators/Everyone entries, and
	// Windows attaches it to most files by default via inheritance. It
	// carries no access beyond what the real owner (already checked above)
	// already has, so it belongs on the approved list alongside the owner
	// itself.
	ownerRights, err := windows.CreateWellKnownSid(windows.WinCreatorOwnerRightsSid)
	if err != nil {
		return fmt.Errorf("resolve OWNER RIGHTS SID: %w", err)
	}
	for index := uint32(0); index < uint32(dacl.AceCount); index++ {
		var ace *windows.ACCESS_ALLOWED_ACE
		if err := windows.GetAce(dacl, index, &ace); err != nil {
			return fmt.Errorf("read credential file DACL entry %d: %w", index, err)
		}
		if ace == nil {
			return fmt.Errorf("credential file DACL entry %d is empty", index)
		}
		switch ace.Header.AceType {
		case windows.ACCESS_DENIED_ACE_TYPE:
			continue
		case windows.ACCESS_ALLOWED_ACE_TYPE:
			principal := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
			if principal.Equals(currentUser.User.Sid) || principal.Equals(localSystem) || principal.Equals(administrators) || principal.Equals(ownerRights) {
				continue
			}
			return fmt.Errorf("credential file DACL grants access to an unapproved principal")
		default:
			return fmt.Errorf("credential file DACL entry %d uses unsupported ACE type %d", index, ace.Header.AceType)
		}
	}
	return nil
}
