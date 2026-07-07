package memory

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/lib/pq"
)

// GetTokenWallet returns the token balance and subscription tier for a given workspace.
// If the workspace does not exist, it inserts a default row and returns default values.
func (r *RelationalStore) GetTokenWallet(ctx context.Context, workspaceID string) (int, string, error) {
	var balance int
	var tier string
	query := `SELECT token_balance, subscription_tier FROM workspaces WHERE id = $1`
	err := r.DB.QueryRowContext(ctx, query, workspaceID).Scan(&balance, &tier)
	if err == sql.ErrNoRows {
		// Auto-insert default workspace wallet
		balance = 50000
		tier = "Trial"
		insertQuery := `
			INSERT INTO workspaces (id, name, is_paused, token_balance, subscription_tier)
			VALUES ($1, $2, FALSE, $3, $4)
			ON CONFLICT (id) DO NOTHING
		`
		_, err = r.DB.ExecContext(ctx, insertQuery, workspaceID, "Workspace "+workspaceID, balance, tier)
		if err != nil {
			return 0, "", fmt.Errorf("failed to auto-create workspace wallet: %v", err)
		}
		// Query again to verify insert and retrieve correct state (handles concurrent insert)
		err = r.DB.QueryRowContext(ctx, query, workspaceID).Scan(&balance, &tier)
		if err != nil {
			return 0, "", err
		}
	} else if err != nil {
		return 0, "", err
	}
	return balance, tier, nil
}

// DeductTokens atomicly subtracts amount from the workspace's token balance.
// It returns an error if the balance is insufficient or if the database query fails.
func (r *RelationalStore) DeductTokens(ctx context.Context, workspaceID string, amount int) (int, error) {
	var newBalance int
	err := r.ExecuteTransaction(ctx, func(tx *sql.Tx) error {
		var balance int
		query := `SELECT token_balance FROM workspaces WHERE id = $1 FOR UPDATE`
		err := tx.QueryRowContext(ctx, query, workspaceID).Scan(&balance)
		if err == sql.ErrNoRows {
			// Auto-insert default if not found
			balance = 50000
			tier := "Trial"
			insertQuery := `
				INSERT INTO workspaces (id, name, is_paused, token_balance, subscription_tier)
				VALUES ($1, $2, FALSE, $3, $4)
				ON CONFLICT (id) DO NOTHING
			`
			_, err = tx.ExecContext(ctx, insertQuery, workspaceID, "Workspace "+workspaceID, balance, tier)
			if err != nil {
				return fmt.Errorf("failed to auto-create wallet in transaction: %v", err)
			}
		} else if err != nil {
			return err
		}

		if balance < amount {
			return fmt.Errorf("insufficient tokens: current balance %d, required %d", balance, amount)
		}

		newBalance = balance - amount
		updateQuery := `UPDATE workspaces SET token_balance = $1 WHERE id = $2`
		_, err = tx.ExecContext(ctx, updateQuery, newBalance, workspaceID)
		if err != nil {
			return fmt.Errorf("failed to update token balance: %v", err)
		}
		return nil
	})

	if err != nil {
		return 0, err
	}
	return newBalance, nil
}

// ProvisionNewWorkspace checks if a workspace exists in the workspaces table.
// If it does not exist, it inserts a clean production-grade row initialized with exactly 50000 tokens,
// the "Trial" subscription_tier, and stores the user's email safely.
func (r *RelationalStore) ProvisionNewWorkspace(ctx context.Context, workspaceID string, email string) error {
	var exists bool
	query := `SELECT EXISTS(SELECT 1 FROM workspaces WHERE id = $1)`
	err := r.DB.QueryRowContext(ctx, query, workspaceID).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check workspace existence: %v", err)
	}

	if exists {
		return nil
	}

	// Insert clean production-grade row initialized with exactly 50000 tokens, "Trial" tier, and email.
	insertQuery := `
		INSERT INTO workspaces (id, name, is_paused, token_balance, subscription_tier, email)
		VALUES ($1, $2, FALSE, $3, $4, $5)
		ON CONFLICT (id) DO NOTHING
	`
	name := "Workspace " + workspaceID
	_, err = r.DB.ExecContext(ctx, insertQuery, workspaceID, name, 50000, "Trial", email)
	if err != nil {
		return fmt.Errorf("failed to provision workspace: %v", err)
	}

	return nil
}

// UpgradeWorkspaceTier updates the subscription tier and replenishes the token balance under a FOR UPDATE lock transaction.
func (r *RelationalStore) UpgradeWorkspaceTier(ctx context.Context, workspaceID string, newTier string, tokensToAdd int, referenceID string) error {
	return r.ExecuteTransaction(ctx, func(tx *sql.Tx) error {
		// Enforce unique reference_id by inserting into journal_entries first.
		// If referenceID has already been processed for this workspace, the insert will fail
		// due to the unique_workspace_reference constraint.
		if referenceID != "" {
			insertJournal := `
				INSERT INTO journal_entries (workspace_id, account_id, entry_type, amount, description, reference_id)
				VALUES ($1, $2, $3, $4, $5, $6)
			`
			_, err := tx.ExecContext(ctx, insertJournal, workspaceID, "REVENUE", "CREDIT", float64(tokensToAdd), "Workspace Subscription Upgrade to "+newTier, referenceID)
			if err != nil {
				if pgErr, ok := err.(*pq.Error); ok && pgErr.Code == "23505" && pgErr.Constraint == "unique_workspace_reference" {
					slog.Info("duplicate webhook replay, no-op", slog.String("workspace_id", workspaceID), slog.String("reference_id", referenceID))
					return nil
				}
				return fmt.Errorf("failed to log upgrade transaction: %w", err)
			}
		}

		var balance int
		var currentTier string

		// Lock row for update
		selectQuery := `SELECT token_balance, subscription_tier FROM workspaces WHERE id = $1 FOR UPDATE`
		err := tx.QueryRowContext(ctx, selectQuery, workspaceID).Scan(&balance, &currentTier)
		if err != nil {
			if err == sql.ErrNoRows {
				// Workspace doesn't exist, create it with default initial tokens + upgrade tokens
				insertQuery := `
					INSERT INTO workspaces (id, name, is_paused, token_balance, subscription_tier)
					VALUES ($1, $2, FALSE, $3, $4)
					ON CONFLICT (id) DO NOTHING
				`
				name := "Workspace " + workspaceID
				_, err = tx.ExecContext(ctx, insertQuery, workspaceID, name, 50000+tokensToAdd, newTier)
				if err != nil {
					return fmt.Errorf("failed to create wallet during upgrade: %v", err)
				}
				return nil
			}
			return err
		}

		newBalance := balance + tokensToAdd
		updateQuery := `UPDATE workspaces SET token_balance = $1, subscription_tier = $2 WHERE id = $3`
		_, err = tx.ExecContext(ctx, updateQuery, newBalance, newTier, workspaceID)
		if err != nil {
			return fmt.Errorf("failed to upgrade workspace: %v", err)
		}
		return nil
	})
}


