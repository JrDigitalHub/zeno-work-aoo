package memory

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// CreateBackgroundJob registers a new job with PENDING status in the database.
func (r *RelationalStore) CreateBackgroundJob(ctx context.Context, id string, idempotencyKey string, workspaceID string) error {
	query := `
		INSERT INTO background_jobs (id, idempotency_key, workspace_id, status, result, created_at)
		VALUES ($1, $2, $3, 'PENDING', NULL, $4)
	`
	_, err := r.DB.ExecContext(ctx, query, id, idempotencyKey, workspaceID, time.Now())
	if err != nil {
		return fmt.Errorf("failed to create background job: %v", err)
	}
	return nil
}

// GetJobByIdempotencyKey retrieves the job's UUID by its idempotency key.
func (r *RelationalStore) GetJobByIdempotencyKey(ctx context.Context, idempotencyKey string) (string, error) {
	var id string
	query := `SELECT id FROM background_jobs WHERE idempotency_key = $1`
	err := r.DB.QueryRowContext(ctx, query, idempotencyKey).Scan(&id)
	if err != nil {
		return "", err
	}
	return id, nil
}

// UpdateJobStatus updates the status and result payload of a background job.
func (r *RelationalStore) UpdateJobStatus(ctx context.Context, workspaceID string, jobID string, status string, result string) error {
	ctxWithWorkspace := context.WithValue(ctx, WorkspaceIDKey, workspaceID)

	return r.ExecuteTransaction(ctxWithWorkspace, func(tx *sql.Tx) error {
		var err error
		if result == "" {
			query := `UPDATE background_jobs SET status = $1 WHERE id = $2`
			_, err = tx.ExecContext(ctxWithWorkspace, query, status, jobID)
		} else {
			query := `UPDATE background_jobs SET status = $1, result = $2 WHERE id = $3`
			_, err = tx.ExecContext(ctxWithWorkspace, query, status, result, jobID)
		}
		if err != nil {
			return fmt.Errorf("failed to update job status: %w", err)
		}
		return nil
	})
}

// GetJobStatus retrieves the current status and result of a background job.
func (r *RelationalStore) GetJobStatus(ctx context.Context, jobID string) (string, string, error) {
	var status string
	var result sql.NullString
	query := `SELECT status, result FROM background_jobs WHERE id = $1`
	err := r.DB.QueryRowContext(ctx, query, jobID).Scan(&status, &result)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", "", fmt.Errorf("job not found")
		}
		return "", "", err
	}

	resultStr := ""
	if result.Valid {
		resultStr = result.String
	}
	return status, resultStr, nil
}

// CreateBackgroundJobTx registers a new job with PENDING status inside a database transaction.
func (r *RelationalStore) CreateBackgroundJobTx(ctx context.Context, tx *sql.Tx, id string, idempotencyKey string, workspaceID string) error {
	query := `
		INSERT INTO background_jobs (id, idempotency_key, workspace_id, status, result, created_at)
		VALUES ($1, $2, $3, 'PENDING', NULL, $4)
	`
	_, err := tx.ExecContext(ctx, query, id, idempotencyKey, workspaceID, time.Now())
	if err != nil {
		return fmt.Errorf("failed to create background job in tx: %w", err)
	}
	return nil
}

// GetJobByIdempotencyKeyTx retrieves the job's UUID by its idempotency key inside a database transaction.
func (r *RelationalStore) GetJobByIdempotencyKeyTx(ctx context.Context, tx *sql.Tx, idempotencyKey string) (string, error) {
	var id string
	query := `SELECT id FROM background_jobs WHERE idempotency_key = $1`
	err := tx.QueryRowContext(ctx, query, idempotencyKey).Scan(&id)
	if err != nil {
		return "", err
	}
	return id, nil
}
