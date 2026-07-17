package backoffice

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
	"os"

	"github.com/JrDigitalHub/zeno-work-aoo/internal/memory"
)

// Task represents a business operation waiting for COO/Human approval.
// This directly maps to the Next.js frontend schema.
type Task struct {
	TaskID      string    `json:"task_id"`
	WorkspaceID string    `json:"workspace_id"`
	OwnerRole   string    `json:"owner_role"`
	Priority    string    `json:"priority"`
	Status      string    `json:"status"`
	Context     string    `json:"context"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// PipelineManager acts as the Chief Operating Officer (COO).
// It manages real-time pipeline capacity AND asynchronous operational tasks.
type PipelineManager struct {
	activePipelines map[string]int
	mu              sync.Mutex
	DB              *sql.DB // The Postgres connection for the State Machine
	Store           *memory.RelationalStore
}

// NewPipelineManager initializes the COO service
func NewPipelineManager(store *memory.RelationalStore) *PipelineManager {
	fmt.Println("🏢 [COO-SERVICE] Initializing Operational Workflow Manager & REST API...")
	var db *sql.DB
	if store != nil {
		db = store.DB
	}
	return &PipelineManager{
		activePipelines: make(map[string]int),
		DB:              db,
		Store:           store,
	}
}

// executeWithRLS executes the database callback inside a transaction that registers the workspace ID context for RLS
func (m *PipelineManager) executeWithRLS(ctx context.Context, workspaceID string, fn func(exec interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}) error) error {
	if m.Store == nil {
		if m.DB == nil {
			return fmt.Errorf("no database connection configured")
		}
		return fn(m.DB)
	}

	ctxWithWorkspace := context.WithValue(ctx, memory.WorkspaceIDKey, workspaceID)
	return m.Store.ExecuteTransaction(ctxWithWorkspace, func(tx *sql.Tx) error {
		return fn(tx)
	})
}

// =====================================================================
// 1. LEGACY PIPELINE CAPACITY TRACKING (Keeps Sentinel & Predator safe)
// =====================================================================

// ProvisionWorkspace sets up the initial load capacity for a tenant
func (m *PipelineManager) ProvisionWorkspace(ctx context.Context, workspaceID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.activePipelines[workspaceID]; !exists {
		m.activePipelines[workspaceID] = 0
		log.Printf("🏢 [BACK-OFFICE] Provisioned isolated operational pipeline state for Workspace [%s]\n", workspaceID)
	}
}

// ReservePipeline increments the active job count for a tenant
func (m *PipelineManager) ReservePipeline(ctx context.Context, workspaceID, targetID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.activePipelines[workspaceID]++
	fmt.Printf("🏢 [BACK-OFFICE] Pipeline capacity reserved for Workspace [%s] Target [%s]. Active Load: %d/10\n", workspaceID, targetID, m.activePipelines[workspaceID])
}

// ReleasePipeline decrements the active job count and frees the slot
func (m *PipelineManager) ReleasePipeline(ctx context.Context, workspaceID, targetID string) error {
	// 1. Update the persistent Task database (The Dashboard State)
	err := m.executeWithRLS(ctx, workspaceID, func(exec interface {
		ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
		QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
		QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	}) error {
		_, err := exec.ExecContext(ctx, "UPDATE tasks SET status = $1 WHERE workspace_id = $2 AND context = $3",
			"COMPLETED", workspaceID, targetID)
		return err
	})
	if err != nil {
		return fmt.Errorf("failed to release pipeline: %w", err)
	}

	// 2. Update the in-memory load balancer (The Concurrency Manager)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.activePipelines[workspaceID] > 0 {
		m.activePipelines[workspaceID]--
	}
	
	fmt.Printf("♻️ [BACK-OFFICE] Pipeline slot released for Workspace [%s] Target [%s]. Active Load: %d/10\n", workspaceID, targetID, m.activePipelines[workspaceID])
	return nil
}

// =====================================================================
// 2. ENTERPRISE COO STATE MACHINE (API-First Task Management)
// =====================================================================

// CreateTask safely inserts a new operational requirement into the Postgres ledger
// This is called internally by your Go agents when a human needs to review something.
func (m *PipelineManager) CreateTask(ctx context.Context, workspaceID, priority, contextStr string) error {
	query := `
		INSERT INTO tasks (workspace_id, owner_role, priority, status, context, created_at, updated_at)
		VALUES ($1, 'COO', $2, 'PENDING', $3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
	`
	err := m.executeWithRLS(ctx, workspaceID, func(exec interface {
		ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
		QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
		QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	}) error {
		_, err := exec.ExecContext(ctx, query, workspaceID, priority, contextStr)
		return err
	})
	if err != nil {
		return fmt.Errorf("failed to create COO task: %w", err)
	}
	
	log.Printf("📋 [COO] New %s priority task created for Workspace [%s]: %s", priority, workspaceID, contextStr)
	return nil
}

// HandleGetTasks acts as the REST API endpoint: GET /api/v1/coo/tasks
// It serves the active Kanban board to your Next.js application.
func (m *PipelineManager) HandleGetTasks(w http.ResponseWriter, r *http.Request) {
	// In production, your middleware passes the workspace_id through the request context.
	// We extract it here to ensure data isolation.
	ctxWorkspace := r.Context().Value("workspace_id")
	if ctxWorkspace == nil {
		http.Error(w, `{"error": "Unauthorized. Missing workspace context."}`, http.StatusUnauthorized)
		return
	}
	workspaceID := fmt.Sprintf("%v", ctxWorkspace)

	// Allow filtering by status (e.g., ?status=pending)
	statusFilter := r.URL.Query().Get("status")
	
	var tasks []Task
	err := m.executeWithRLS(r.Context(), workspaceID, func(exec interface {
		ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
		QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
		QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	}) error {
		var rows *sql.Rows
		var err error
		if statusFilter != "" {
			query := `SELECT task_id, workspace_id, owner_role, priority, status, context, created_at, updated_at FROM tasks WHERE workspace_id = $1 AND status = $2 ORDER BY created_at DESC`
			rows, err = exec.QueryContext(r.Context(), query, workspaceID, strings.ToUpper(statusFilter))
		} else {
			query := `SELECT task_id, workspace_id, owner_role, priority, status, context, created_at, updated_at FROM tasks WHERE workspace_id = $1 ORDER BY created_at DESC`
			rows, err = exec.QueryContext(r.Context(), query, workspaceID)
		}
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var t Task
			if err := rows.Scan(&t.TaskID, &t.WorkspaceID, &t.OwnerRole, &t.Priority, &t.Status, &t.Context, &t.CreatedAt, &t.UpdatedAt); err != nil {
				log.Printf("⚠️ [COO] Failed to parse a task row: %v", err)
				continue
			}
			tasks = append(tasks, t)
		}
		return rows.Err()
	})

	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error": "Database read failed: %v"}`, err), http.StatusInternalServerError)
		return
	}

	// Ensure we return an empty array [] instead of null if there are no tasks
	if tasks == nil {
		tasks = []Task{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tasks)
}

// HandleUpdateTask acts as the REST API endpoint: PATCH /api/v1/coo/tasks/{id}
// It allows the Next.js frontend to Approve, Reject, or Complete a task.
func (m *PipelineManager) HandleUpdateTask(w http.ResponseWriter, r *http.Request) {
	ctxWorkspace := r.Context().Value("workspace_id")
	if ctxWorkspace == nil {
		http.Error(w, `{"error": "Unauthorized"}`, http.StatusUnauthorized)
		return
	}
	workspaceID := fmt.Sprintf("%v", ctxWorkspace)

	if r.Method != http.MethodPatch {
		http.Error(w, `{"error": "Method not allowed. Use PATCH."}`, http.StatusMethodNotAllowed)
		return
	}

	// Extract Task ID from the URL path (e.g., /api/v1/coo/tasks/123e4567-e89b-12d3...)
	parts := strings.Split(r.URL.Path, "/")
	taskID := parts[len(parts)-1]

	var reqBody struct {
		Status string `json:"status"`
	}

	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		http.Error(w, `{"error": "Invalid JSON payload"}`, http.StatusBadRequest)
		return
	}

	// Validate the status strictly against our allowed enum values
	newStatus := strings.ToUpper(reqBody.Status)
	validStatuses := map[string]bool{"PENDING": true, "IN_REVIEW": true, "APPROVED": true, "REJECTED": true, "COMPLETED": true}
	if !validStatuses[newStatus] {
		http.Error(w, `{"error": "Invalid status value"}`, http.StatusBadRequest)
		return
	}

	// Update the database securely
	var rowsAffected int64
	err := m.executeWithRLS(r.Context(), workspaceID, func(exec interface {
		ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
		QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
		QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	}) error {
		query := `
			UPDATE tasks 
			SET status = $1, updated_at = CURRENT_TIMESTAMP 
			WHERE task_id = $2 AND workspace_id = $3
		`
		res, err := exec.ExecContext(r.Context(), query, newStatus, taskID, workspaceID)
		if err != nil {
			return err
		}
		rowsAffected, _ = res.RowsAffected()
		return nil
	})

	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error": "Database update failed: %v"}`, err), http.StatusInternalServerError)
		return
	}

	if rowsAffected == 0 {
		http.Error(w, `{"error": "Task not found or unauthorized"}`, http.StatusNotFound)
		return
	}

	log.Printf("✅ [COO] Task [%s] updated to [%s] by Executive Command", taskID, newStatus)
	
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"success": true, "message": "Task state updated successfully"}`))
}
// WorkflowConfig represents the dynamic rules for the COO
type WorkflowConfig struct {
	AutoApproveScore int      `json:"auto_approve_threshold_score"`
	RequireHuman     bool     `json:"require_human_audit_on_fail"`
	Currencies       []string `json:"supported_currencies"`
	MaxComputeCost   float64  `json:"max_compute_cost_per_session_usd"`
}

// LoadWorkflowRules reads the JSON config into memory
func (m *PipelineManager) LoadWorkflowRules() (*WorkflowConfig, error) {
	file, err := os.ReadFile("configs/workflows.json")
	if err != nil {
		return nil, fmt.Errorf("could not load workflow rules: %v", err)
	}

	var wrapper struct {
		Rules WorkflowConfig `json:"coo_agent_rules"`
	}
	
	if err := json.Unmarshal(file, &wrapper); err != nil {
		return nil, fmt.Errorf("failed to parse workflow JSON: %v", err)
	}
	
	log.Printf("🏢 [COO] Workflow rules loaded. Auto-Approve Threshold: %d", wrapper.Rules.AutoApproveScore)
	return &wrapper.Rules, nil
}
// =====================================================================
// 3. EXTERNAL WEBHOOK INGESTION (The COO's Ear)
// =====================================================================

// Ingest receives raw data payloads from external sources (webhooks, Shopline, etc.)
// and automatically routes them into actionable COO tasks on the Kanban board.
func (m *PipelineManager) Ingest(ctx context.Context, workspaceID, source, payload string) {
	log.Printf("📥 [COO] Raw data ingested for Workspace [%s] from Source [%s]", workspaceID, source)

	// Convert this ingestion into an actionable task for the dashboard
	contextMsg := fmt.Sprintf("Review inbound data from %s. Payload summary: %s", source, payload)

	// Route it to the database as a MEDIUM priority task
	err := m.CreateTask(ctx, workspaceID, "MEDIUM", contextMsg)
	if err != nil {
		log.Printf("⚠️ [COO] Failed to route external ingestion to task list: %v", err)
	}
}
func (m *PipelineManager) CheckCapacity(ctx context.Context, workspaceID string) bool {
    var count int
    err := m.executeWithRLS(ctx, workspaceID, func(exec interface {
		ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
		QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
		QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	}) error {
		return exec.QueryRowContext(ctx, "SELECT COUNT(*) FROM tasks WHERE workspace_id = $1 AND status = 'PENDING'", workspaceID).Scan(&count)
	})
    if err != nil {
        return false
    }
    return count < 50
}

func (m *PipelineManager) RegisterPipeline(ctx context.Context, workspaceID, targetID string) error {
    err := m.executeWithRLS(ctx, workspaceID, func(exec interface {
		ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
		QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
		QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	}) error {
		_, err := exec.ExecContext(ctx, "INSERT INTO tasks (workspace_id, owner_role, status, context) VALUES ($1, $2, $3, $4)", 
			workspaceID, "SENTINEL", "PROCESSING", targetID)
		return err
	})
    if err != nil {
        return fmt.Errorf("failed to register pipeline: %w", err)
    }
    return nil
}

