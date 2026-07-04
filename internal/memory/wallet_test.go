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
	_ = func(id string, tier string, tokens int) error {
		if store != nil {
			return store.UpgradeWorkspaceTier(id, tier, tokens)
		}
		return nil
	}
	_ = func(jobID string, workspaceID string) error {
		if store != nil {
			return store.CreateBackgroundJob(jobID, workspaceID)
		}
		return nil
	}
	_ = func(jobID string, status string, result string) error {
		if store != nil {
			return store.UpdateJobStatus(jobID, status, result)
		}
		return nil
	}
	_ = func(jobID string) (string, string, error) {
		if store != nil {
			return store.GetJobStatus(jobID)
		}
		return "", "", nil
	}
}
