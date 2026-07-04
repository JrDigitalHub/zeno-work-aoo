package memory

import (
	"database/sql"
	"fmt"
	"time"
)

// CreateBackgroundJob registers a new job with PENDING status in the database.
func (r *RelationalStore) CreateBackgroundJob(jobID string, workspaceID string) error {
	query := `
		INSERT INTO background_jobs (id, workspace_id, status, result, created_at)
		VALUES ($1, $2, 'PENDING', NULL, $3)
	`
	_, err := r.DB.Exec(query, jobID, workspaceID, time.Now())
	if err != nil {
		return fmt.Errorf("failed to create background job: %v", err)
	}
	return nil
}

// UpdateJobStatus updates the status and result payload of a background job.
func (r *RelationalStore) UpdateJobStatus(jobID string, status string, result string) error {
	var err error
	if result == "" {
		query := `UPDATE background_jobs SET status = $1 WHERE id = $2`
		_, err = r.DB.Exec(query, status, jobID)
	} else {
		query := `UPDATE background_jobs SET status = $1, result = $2 WHERE id = $3`
		_, err = r.DB.Exec(query, status, result, jobID)
	}
	if err != nil {
		return fmt.Errorf("failed to update job status: %v", err)
	}
	return nil
}

// GetJobStatus retrieves the current status and result of a background job.
func (r *RelationalStore) GetJobStatus(jobID string) (string, string, error) {
	var status string
	var result sql.NullString
	query := `SELECT status, result FROM background_jobs WHERE id = $1`
	err := r.DB.QueryRow(query, jobID).Scan(&status, &result)
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
