package memory

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq" // The Postgres Driver
)

// RelationalStore manages global state for targets, system logs, and financial ledgers.
type RelationalStore struct {
	DB *sql.DB
}

// NewRelationalStore connects to Supabase, builds the core schema, and sets connection limits.
func NewRelationalStore(connectionString string) (*RelationalStore, error) {
	// Opens the connection pool to Supabase
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to open Postgres database: %v", err)
	}

	// ENTERPRISE UPGRADE: Connection Pool Limits
	db.SetMaxOpenConns(25)                 // Max simultaneous connections
	db.SetMaxIdleConns(25)                 // Keep connections warm in memory
	db.SetConnMaxLifetime(5 * time.Minute) // Safely recycle stale connections

	// Verify the connection is actually alive
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping Postgres database: %v", err)
	}

	// Create the core tracking table optimized for Postgres (Outbound Pipeline)
	schema := `
	CREATE TABLE IF NOT EXISTS outbound_ledger (
		target_id VARCHAR(255) PRIMARY KEY,
		name VARCHAR(255) NOT NULL,
		email VARCHAR(255) UNIQUE NOT NULL,
		qualification_score INT,
		is_qualified BOOLEAN,
		status VARCHAR(50),
		timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);`

	_, err = db.Exec(schema)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize Postgres schema: %v", err)
	}

	fmt.Println("🗄️ [Supabase] Relational state ledger connected and verified (Pooled).")
	return &RelationalStore{DB: db}, nil
}

// TargetExists checks if ZENO has already processed this specific business.
func (r *RelationalStore) TargetExists(email string) bool {
	var exists bool
	// Postgres strictly uses $1 for parameterized variables to prevent SQL injection
	query := `SELECT EXISTS(SELECT 1 FROM outbound_ledger WHERE email = $1)`

	err := r.DB.QueryRow(query, email).Scan(&exists)
	if err != nil {
		log.Printf("⚠️ [Supabase] Error checking target existence: %v\n", err)
		return false
	}
	return exists
}

// LogTarget inserts or updates a lead's state securely in the database.
func (r *RelationalStore) LogTarget(targetID, name, email string, score int, isQualified bool, status string) error {
	query := `
	INSERT INTO outbound_ledger (target_id, name, email, qualification_score, is_qualified, status, timestamp)
	VALUES ($1, $2, $3, $4, $5, $6, $7)
	ON CONFLICT (target_id) DO UPDATE SET 
		status = EXCLUDED.status,
		timestamp = EXCLUDED.timestamp;`

	_, err := r.DB.Exec(query, targetID, name, email, score, isQualified, status, time.Now())
	if err != nil {
		return fmt.Errorf("failed to log target state: %v", err)
	}
	return nil
}

// ExecuteTransaction executes financial/operational actions under an ACID isolation bubble
func (r *RelationalStore) ExecuteTransaction(fn func(*sql.Tx) error) error {
	tx, err := r.DB.Begin()
	if err != nil {
		return fmt.Errorf("could not initiate transaction context: %v", err)
	}

	// Defer handling panic recoveries or transaction rollbacks
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p) // re-throw panic after rollback safety
		}
	}()

	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

// LogDoubleEntry records balanced entries for the CFO service safely
func (r *RelationalStore) LogDoubleEntry(workspaceID string, debitAcc, creditAcc string, amount float64, desc string, refID string) error {
	return r.ExecuteTransaction(func(tx *sql.Tx) error {
		query := `
			INSERT INTO journal_entries (workspace_id, account_id, entry_type, amount, description, reference_id)
			VALUES ($1, $2, $3, $4, $5, $6);
		`

		// Side A: Debit Operation
		_, err := tx.Exec(query, workspaceID, debitAcc, "DEBIT", amount, desc, refID)
		if err != nil {
			return fmt.Errorf("debit allocation failed: %v", err)
		}

		// Side B: Credit Operation
		_, err = tx.Exec(query, workspaceID, creditAcc, "CREDIT", amount, desc, refID)
		if err != nil {
			return fmt.Errorf("credit allocation failed: %v", err)
		}

		return nil
	})
}

// Close gracefully shuts down the database connection pool.
func (r *RelationalStore) Close() {
	if r.DB != nil {
		r.DB.Close()
	}
}

// LedgerEntry represents a single row returned to the CFO Dashboard
type LedgerEntry struct {
	EntryID     string  `json:"entry_id"`
	AccountID   string  `json:"account_id"`
	EntryType   string  `json:"entry_type"`
	Amount      float64 `json:"amount"`
	Description string  `json:"description"`
	Timestamp   string  `json:"timestamp"`
}

// GetFinancialLedger fetches the immutable double-entry history for a workspace
func (r *RelationalStore) GetFinancialLedger(workspaceID string, limit int) ([]LedgerEntry, error) {
	query := `
		SELECT entry_id, account_id, entry_type, amount, description, timestamp
		FROM journal_entries 
		WHERE workspace_id = $1 
		ORDER BY timestamp DESC LIMIT $2
	`
	rows, err := r.DB.Query(query, workspaceID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch ledger: %v", err)
	}
	defer rows.Close()

	var entries []LedgerEntry
	for rows.Next() {
		var e LedgerEntry
		if err := rows.Scan(&e.EntryID, &e.AccountID, &e.EntryType, &e.Amount, &e.Description, &e.Timestamp); err != nil {
			log.Printf("⚠️ [CFO] Error parsing ledger row: %v", err)
			continue
		}
		entries = append(entries, e)
	}

	if entries == nil {
		entries = []LedgerEntry{}
	}
	return entries, nil
}
