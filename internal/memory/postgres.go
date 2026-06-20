package memory

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq" // The Postgres Driver
)

// RelationalStore manages global state for targets and system logs.
type RelationalStore struct {
	DB *sql.DB
}

// NewRelationalStore connects to Supabase and builds the core schema.
func NewRelationalStore(connectionString string) (*RelationalStore, error) {
	// Opens the connection pool to Supabase
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to open Postgres database: %v", err)
	}

	// Verify the connection is actually alive
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping Postgres database: %v", err)
	}

	// Create the core tracking table optimized for Postgres
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

	fmt.Println("🗄️ [Supabase] Relational state ledger connected and verified.")
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

// Close gracefully shuts down the database connection pool.
func (r *RelationalStore) Close() {
	if r.DB != nil {
		r.DB.Close()
	}
}