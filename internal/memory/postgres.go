package memory

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq" // The Postgres Driver
)

type contextKey string
const WorkspaceIDKey contextKey = "workspace_id"

func GetWorkspaceID(ctx context.Context) string {
	if val, ok := ctx.Value(WorkspaceIDKey).(string); ok {
		return val
	}
	if val, ok := ctx.Value("workspace_id").(string); ok {
		return val
	}
	return ""
}

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
	db.SetMaxOpenConns(4)                  // Limit to 4 open connections to stay under Supabase limits
	db.SetMaxIdleConns(4)                  // Limit to 4 idle connections
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

	workspacesSchema := `
	CREATE TABLE IF NOT EXISTS workspaces (
		id VARCHAR(255) PRIMARY KEY,
		name VARCHAR(255) NOT NULL,
		is_paused BOOLEAN DEFAULT FALSE,
		token_balance INT DEFAULT 50000,
		subscription_tier VARCHAR(50) DEFAULT 'Trial',
		email VARCHAR(255)
	);
	ALTER TABLE workspaces ADD COLUMN IF NOT EXISTS token_balance INT DEFAULT 50000;
	ALTER TABLE workspaces ADD COLUMN IF NOT EXISTS subscription_tier VARCHAR(50) DEFAULT 'Trial';
	ALTER TABLE workspaces ADD COLUMN IF NOT EXISTS is_paused BOOLEAN DEFAULT FALSE;
	ALTER TABLE workspaces ADD COLUMN IF NOT EXISTS email VARCHAR(255);
	`
	_, err = db.Exec(workspacesSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize workspaces schema: %v", err)
	}

	jobsSchema := `
	CREATE TABLE IF NOT EXISTS background_jobs (
		id VARCHAR(255) PRIMARY KEY,
		workspace_id VARCHAR(255) NOT NULL,
		status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
		result TEXT,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);
	ALTER TABLE background_jobs ADD COLUMN IF NOT EXISTS idempotency_key TEXT;
	CREATE UNIQUE INDEX IF NOT EXISTS idx_background_jobs_idempotency_key 
		ON background_jobs (idempotency_key) 
		WHERE idempotency_key IS NOT NULL;
	`
	_, err = db.Exec(jobsSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize background_jobs schema: %v", err)
	}

	journalSchema := `
	CREATE TABLE IF NOT EXISTS journal_entries (
		entry_id SERIAL PRIMARY KEY,
		workspace_id VARCHAR(255) NOT NULL,
		account_id VARCHAR(255) NOT NULL,
		entry_type VARCHAR(50) NOT NULL,
		amount DECIMAL(12,2) NOT NULL,
		description TEXT,
		reference_id VARCHAR(255),
		timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);
	DO $$
	BEGIN
		-- Drop the old constraint if it exists and only covers 2 columns (workspace_id, reference_id)
		IF EXISTS (
			SELECT 1 FROM pg_constraint 
			WHERE conname = 'unique_workspace_reference' 
			AND array_length(conkey, 1) = 2
		) THEN
			ALTER TABLE journal_entries DROP CONSTRAINT unique_workspace_reference;
		END IF;

		IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'unique_workspace_reference') THEN
			ALTER TABLE journal_entries ADD CONSTRAINT unique_workspace_reference UNIQUE (workspace_id, reference_id, entry_type);
		END IF;
	END;
	$$;
	`
	_, err = db.Exec(journalSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize journal_entries schema: %v", err)
	}

	fmt.Println("🗄️ [Supabase] Relational state ledger connected and verified (Pooled).")
	return &RelationalStore{DB: db}, nil
}

// TargetExists checks if ZENO has already processed this specific business.
func (r *RelationalStore) TargetExists(ctx context.Context, email string) bool {
	var exists bool
	// Postgres strictly uses $1 for parameterized variables to prevent SQL injection
	query := `SELECT EXISTS(SELECT 1 FROM outbound_ledger WHERE email = $1)`

	err := r.DB.QueryRowContext(ctx, query, email).Scan(&exists)
	if err != nil {
		log.Printf("⚠️ [Supabase] Error checking target existence: %v\n", err)
		return false
	}
	return exists
}

// LogTarget inserts or updates a lead's state securely in the database.
func (r *RelationalStore) LogTarget(ctx context.Context, targetID, name, email string, score int, isQualified bool, status string) error {
	query := `
	INSERT INTO outbound_ledger (target_id, name, email, qualification_score, is_qualified, status, timestamp)
	VALUES ($1, $2, $3, $4, $5, $6, $7)
	ON CONFLICT (target_id) DO UPDATE SET 
		status = EXCLUDED.status,
		timestamp = EXCLUDED.timestamp;`

	_, err := r.DB.ExecContext(ctx, query, targetID, name, email, score, isQualified, status, time.Now())
	if err != nil {
		return fmt.Errorf("failed to log target state: %v", err)
	}
	return nil
}

// ExecuteTransaction executes financial/operational actions under an ACID isolation bubble
func (r *RelationalStore) ExecuteTransaction(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := r.DB.BeginTx(ctx, nil)
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

	// Register workspace context for RLS in the transaction session
	workspaceID := GetWorkspaceID(ctx)
	if workspaceID != "" {
		_, err = tx.ExecContext(ctx, "SELECT set_config('app.current_workspace_id', $1, true)", workspaceID)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to set workspace context for RLS: %v", err)
		}
	}

	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

// LogDoubleEntry records balanced entries for the CFO service safely
func (r *RelationalStore) LogDoubleEntry(ctx context.Context, workspaceID string, debitAcc, creditAcc string, amount float64, desc string, refID string) error {
	return r.ExecuteTransaction(ctx, func(tx *sql.Tx) error {
		query := `
			INSERT INTO journal_entries (workspace_id, account_id, entry_type, amount, description, reference_id)
			VALUES ($1, $2, $3, $4, $5, $6);
		`

		// Side A: Debit Operation
		_, err := tx.ExecContext(ctx, query, workspaceID, debitAcc, "DEBIT", amount, desc, refID)
		if err != nil {
			return fmt.Errorf("debit allocation failed: %v", err)
		}

		// Side B: Credit Operation
		_, err = tx.ExecContext(ctx, query, workspaceID, creditAcc, "CREDIT", amount, desc, refID)
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
func (r *RelationalStore) GetFinancialLedger(ctx context.Context, workspaceID string, limit int) ([]LedgerEntry, error) {
	query := `
		SELECT entry_id, account_id, entry_type, amount, description, timestamp
		FROM journal_entries 
		WHERE workspace_id = $1 
		ORDER BY timestamp DESC LIMIT $2
	`
	rows, err := r.DB.QueryContext(ctx, query, workspaceID, limit)
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

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating ledger rows: %v", err)
	}

	if entries == nil {
		entries = []LedgerEntry{}
	}
	return entries, nil
}
