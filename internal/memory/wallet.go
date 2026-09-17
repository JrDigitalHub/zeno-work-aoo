package memory

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"github.com/google/uuid"
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

// ProcessBillingUpgrade atomically claims the billing idempotency lock in processed_billing_transactions
// and upgrades the workspace tier, token balances, and writes balanced double-entry ledger rows to journal_entries.
// If the transaction was already processed (rowsAffected == 0), it rolls back cleanly and returns (true, nil).
// If any step fails, tx.Rollback() is called and the error is returned so webhook retries can re-process cleanly.
// Strictly calls tx.Commit() when all updates succeed.
func (r *RelationalStore) ProcessBillingUpgrade(ctx context.Context, gateway string, referenceID string, workspaceID string, amount float64, currency string, newTier string, tokensToAdd int) (bool, error) {
	if r.DB == nil {
		return false, fmt.Errorf("database connection is nil")
	}

	tx, err := r.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("failed to begin transaction: %w", err)
	}

	var committed bool
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
	}()

	currentWorkspaceID := GetWorkspaceID(ctx)
	if currentWorkspaceID == "" {
		currentWorkspaceID = workspaceID
	}
	if currentWorkspaceID != "" {
		_, _ = tx.ExecContext(ctx, "SELECT set_config('app.current_workspace_id', $1, true)", currentWorkspaceID)
	}

	// 1. If referenceID is provided, atomically insert into processed_billing_transactions
	if referenceID != "" {
		var wsUUID uuid.UUID
		if parsed, parseErr := uuid.Parse(workspaceID); parseErr == nil {
			wsUUID = parsed
		} else {
			wsUUID = uuid.NewSHA1(uuid.NameSpaceOID, []byte(workspaceID))
		}

		insertIdempotency := `
			INSERT INTO processed_billing_transactions (gateway, reference_id, workspace_id, amount, currency)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (gateway, reference_id) DO NOTHING;
		`
		res, execErr := tx.ExecContext(ctx, insertIdempotency, gateway, referenceID, wsUUID, amount, currency)
		if execErr != nil {
			_ = tx.Rollback()
			if pgErr, ok := execErr.(*pq.Error); ok && pgErr.Code == "23505" {
				slog.Info("duplicate billing transaction caught by constraint, no-op", slog.String("gateway", gateway), slog.String("reference_id", referenceID))
				return true, nil
			}
			return false, fmt.Errorf("failed to claim billing idempotency: %w", execErr)
		}

		rowsAffected, affErr := res.RowsAffected()
		if affErr != nil {
			_ = tx.Rollback()
			return false, fmt.Errorf("failed to get rows affected: %w", affErr)
		}

		if rowsAffected == 0 {
			_ = tx.Rollback()
			slog.Info("billing transaction already processed (rowsAffected == 0), no-op", slog.String("gateway", gateway), slog.String("reference_id", referenceID))
			return true, nil
		}
	}

	// 2. Lock and update workspace row
	var balance int
	var currentTier string
	selectQuery := `SELECT token_balance, subscription_tier FROM workspaces WHERE id = $1 FOR UPDATE`
	err = tx.QueryRowContext(ctx, selectQuery, workspaceID).Scan(&balance, &currentTier)
	if err != nil {
		if err == sql.ErrNoRows {
			insertQuery := `
				INSERT INTO workspaces (id, name, is_paused, token_balance, subscription_tier)
				VALUES ($1, $2, FALSE, $3, $4)
				ON CONFLICT (id) DO NOTHING;
			`
			name := "Workspace " + workspaceID
			_, err = tx.ExecContext(ctx, insertQuery, workspaceID, name, 50000+tokensToAdd, newTier)
			if err != nil {
				_ = tx.Rollback()
				return false, fmt.Errorf("failed to create workspace during upgrade: %w", err)
			}
		} else {
			_ = tx.Rollback()
			return false, fmt.Errorf("failed to select workspace for update: %w", err)
		}
	} else {
		newBalance := balance + tokensToAdd
		updateQuery := `UPDATE workspaces SET token_balance = $1, subscription_tier = $2 WHERE id = $3`
		_, err = tx.ExecContext(ctx, updateQuery, newBalance, newTier, workspaceID)
		if err != nil {
			_ = tx.Rollback()
			return false, fmt.Errorf("failed to upgrade workspace: %w", err)
		}
	}

	// 3. Insert balanced double-entry ledger rows into journal_entries
	if referenceID != "" {
		ledgerAmount := amount
		if ledgerAmount <= 0 {
			ledgerAmount = float64(tokensToAdd)
		}
		desc := fmt.Sprintf("Workspace Subscription Upgrade to %s via %s", newTier, gateway)

		insertJournal := `
			INSERT INTO journal_entries (workspace_id, account_id, entry_type, amount, description, reference_id)
			VALUES ($1, $2, $3, $4, $5, $6);
		`
		// Side A: Debit Operation (Receivables / Gateway Clearing)
		_, err = tx.ExecContext(ctx, insertJournal, workspaceID, "GATEWAY_RECEIVABLES", "DEBIT", ledgerAmount, desc, referenceID)
		if err != nil {
			_ = tx.Rollback()
			return false, fmt.Errorf("failed to record debit journal entry: %w", err)
		}

		// Side B: Credit Operation (Revenue Earned)
		_, err = tx.ExecContext(ctx, insertJournal, workspaceID, "SME_REVENUE", "CREDIT", ledgerAmount, desc, referenceID)
		if err != nil {
			_ = tx.Rollback()
			return false, fmt.Errorf("failed to record credit journal entry: %w", err)
		}
	}

	// 4. Commit strictly when all updates succeed
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("failed to commit billing transaction: %w", err)
	}
	committed = true

	return false, nil
}

// UpgradeWorkspaceTier updates the subscription tier and replenishes the token balance under an atomic transaction.
// It delegates to ProcessBillingUpgrade to ensure idempotency and double-entry accounting integrity.
func (r *RelationalStore) UpgradeWorkspaceTier(ctx context.Context, workspaceID string, newTier string, tokensToAdd int, referenceID string) error {
	gateway := "flutterwave"
	if strings.HasPrefix(referenceID, "pstk_") || strings.HasPrefix(referenceID, "pstk-") {
		gateway = "paystack"
	}
	_, err := r.ProcessBillingUpgrade(ctx, gateway, referenceID, workspaceID, 0, "USD", newTier, tokensToAdd)
	return err
}


