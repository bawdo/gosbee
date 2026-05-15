package keychain

// Service is the macOS Keychain service identifier used by jellyfish.
const Service = "au.com.flipgroup.jellyfish"

// ErrNotFound is returned when no item exists for the given account.
type notFoundError struct{}

func (notFoundError) Error() string { return "keychain item not found" }

// ErrNotFound is the sentinel callers can compare against with errors.Is.
var ErrNotFound = notFoundError{}
