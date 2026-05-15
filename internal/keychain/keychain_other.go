//go:build !darwin

package keychain

import "errors"

var errUnsupported = errors.New("keychain is only supported on macOS")

func Get(account string) (string, error)      { return "", errUnsupported }
func Set(account, token string) error          { return errUnsupported }
func Delete(account string) error              { return errUnsupported }
