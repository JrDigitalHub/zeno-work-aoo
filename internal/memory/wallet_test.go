package memory

import (
	"context"
	"testing"
)

// TestRelationalStore_ProvisionNewWorkspace checks that the method is defined on the RelationalStore.
func TestRelationalStore_ProvisionNewWorkspace(t *testing.T) {
	var store *RelationalStore
	// We verify that the method can be referenced compile-time.
	_ = func(ctx context.Context, id string, email string) error {
		if store != nil {
			return store.ProvisionNewWorkspace(ctx, id, email)
		}
		return nil
	}
	_ = func(ctx context.Context, id string, tier string, tokens int, ref string) error {
		if store != nil {
			return store.UpgradeWorkspaceTier(ctx, id, tier, tokens, ref)
		}
		return nil
	}
	_ = func(ctx context.Context, id string, idempotencyKey string, workspaceID string) error {
		if store != nil {
			return store.CreateBackgroundJob(ctx, id, idempotencyKey, workspaceID)
		}
		return nil
	}
	_ = func(ctx context.Context, workspaceID string, jobID string, status string, result string) error {
		if store != nil {
			return store.UpdateJobStatus(ctx, workspaceID, jobID, status, result)
		}
		return nil
	}
	_ = func(ctx context.Context, jobID string) (string, string, error) {
		if store != nil {
			return store.GetJobStatus(ctx, jobID)
		}
		return "", "", nil
	}
}
