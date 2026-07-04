package memory

import (
	"testing"
)

// TestRelationalStore_ProvisionNewWorkspace checks that the method is defined on the RelationalStore.
func TestRelationalStore_ProvisionNewWorkspace(t *testing.T) {
	var store *RelationalStore
	// We verify that the method can be referenced compile-time.
	_ = func(id string, email string) error {
		if store != nil {
			return store.ProvisionNewWorkspace(id, email)
		}
		return nil
	}
}
