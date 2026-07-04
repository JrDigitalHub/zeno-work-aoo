package memory

import (
	"database/sql"
	"fmt"
)

// GetTokenWallet returns the token balance and subscription tier for a given workspace.
// If the workspace does not exist, it inserts a default row and returns default values.
func (r *RelationalStore) GetTokenWallet(workspaceID string) (int, string, error) {
	var balance int
	var tier string
	query := `SELECT token_balance, subscription_tier FROM workspaces WHERE id = $1`
	err := r.DB.QueryRow(query, workspaceID).Scan(&balance, &tier)
	if err == sql.ErrNoRows {
		// Auto-insert default workspace wallet
		balance = 50000
		tier = "Trial"
		insertQuery := `
			INSERT INTO workspaces (id, name, is_paused, token_balance, subscription_tier)
			VALUES ($1, $2, FALSE, $3, $4)
			ON CONFLICT (id) DO NOTHING
		`
		_, err = r.DB.Exec(insertQuery, workspaceID, "Workspace "+workspaceID, balance, tier)
		if err != nil {
			return 0, "", fmt.Errorf("failed to auto-create workspace wallet: %v", err)
		}
		// Query again to verify insert and retrieve correct state (handles concurrent insert)
		err = r.DB.QueryRow(query, workspaceID).Scan(&balance, &tier)
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
func (r *RelationalStore) DeductTokens(workspaceID string, amount int) (int, error) {
	var newBalance int
	err := r.ExecuteTransaction(func(tx *sql.Tx) error {
		var balance int
		query := `SELECT token_balance FROM workspaces WHERE id = $1 FOR UPDATE`
		err := tx.QueryRow(query, workspaceID).Scan(&balance)
		if err == sql.ErrNoRows {
			// Auto-insert default if not found
			balance = 50000
			tier := "Trial"
			insertQuery := `
				INSERT INTO workspaces (id, name, is_paused, token_balance, subscription_tier)
				VALUES ($1, $2, FALSE, $3, $4)
				ON CONFLICT (id) DO NOTHING
			`
			_, err = tx.Exec(insertQuery, workspaceID, "Workspace "+workspaceID, balance, tier)
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
		_, err = tx.Exec(updateQuery, newBalance, workspaceID)
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
