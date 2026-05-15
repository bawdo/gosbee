//go:build darwin

package keychain

import (
	"errors"

	kc "github.com/keybase/go-keychain"
)

// Get returns the stored token for account, or ErrNotFound.
func Get(account string) (string, error) {
	q := kc.NewItem()
	q.SetSecClass(kc.SecClassGenericPassword)
	q.SetService(Service)
	q.SetAccount(account)
	q.SetMatchLimit(kc.MatchLimitOne)
	q.SetReturnData(true)

	results, err := kc.QueryItem(q)
	if err != nil {
		return "", err
	}
	if len(results) == 0 {
		return "", ErrNotFound
	}
	return string(results[0].Data), nil
}

// Set writes (or replaces) the token for account.
func Set(account, token string) error {
	item := kc.NewItem()
	item.SetSecClass(kc.SecClassGenericPassword)
	item.SetService(Service)
	item.SetAccount(account)
	item.SetData([]byte(token))
	item.SetSynchronizable(kc.SynchronizableNo)
	item.SetAccessible(kc.AccessibleWhenUnlocked)

	err := kc.AddItem(item)
	if errors.Is(err, kc.ErrorDuplicateItem) {
		// Replace the existing item.
		query := kc.NewItem()
		query.SetSecClass(kc.SecClassGenericPassword)
		query.SetService(Service)
		query.SetAccount(account)
		if delErr := kc.DeleteItem(query); delErr != nil {
			return delErr
		}
		return kc.AddItem(item)
	}
	return err
}

// Delete removes the token for account. Returns nil if it did not exist.
func Delete(account string) error {
	q := kc.NewItem()
	q.SetSecClass(kc.SecClassGenericPassword)
	q.SetService(Service)
	q.SetAccount(account)
	err := kc.DeleteItem(q)
	if errors.Is(err, kc.ErrorItemNotFound) {
		return nil
	}
	return err
}
