package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/JrDigitalHub/zeno-work-aoo/internal/memory"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/orchestrator"
	"github.com/JrDigitalHub/zeno-work-aoo/pkg/protocol"
	"github.com/lib/pq"
	"github.com/riverqueue/river"
)

// ScenarioPayload maps the business variables the client tests from the UI
type ScenarioPayload struct {
	ScenarioName    string  `json:"scenario_name"` // e.g., "Q3 Aggressive Growth"
	StartingCapital float64 `json:"starting_capital"`
	MonthlyBurn     float64 `json:"monthly_burn"`
	AdSpend         float64 `json:"ad_spend"`
	CostPerLead     float64 `json:"cost_per_lead"`
	ConversionRate  float64 `json:"conversion_rate"`
	AverageDealSize float64 `json:"average_deal_size"`
}

// ProjectionResult holds the calculated deterministic outcome
type ProjectionResult struct {
	NewCustomers int     `json:"new_customers"`
	ProjectedRev float64 `json:"projected_revenue"`
	NetProfit    float64 `json:"net_profit"`
	RunwayMonths float64 `json:"runway_months"`
}

// FinancialModeler acts as the Chief Financial Officer (CFO).
// It manages double-entry bookkeeping, unit economics, invoice OCR processing,
// and deterministic financial projections.
type FinancialModeler struct {
	router *orchestrator.EventRouter
	DB     *memory.RelationalStore // 👉 Injected to persist client scenarios and ledger
}

// NewFinancialModeler initializes the CFO service
func NewFinancialModeler(r *orchestrator.EventRouter, db *memory.RelationalStore) *FinancialModeler {
	fmt.Println("📊 [CFO/MODELER] Deterministic Financial & Ledger Engine initialized.")
	return &FinancialModeler{
		router: r,
		DB:     db,
	}
}

func (f *FinancialModeler) React(ctx context.Context, e protocol.Event) error {
	// =========================================================================
	// 1. FORWARD PROJECTIONS: Intercept math requests from the UI
	// =========================================================================
	if e.Source == "UI_FINANCIAL_TEST" {
		fmt.Printf("📊 [MODELER] Processing financial scenario for Workspace [%s]...\n", e.WorkspaceID)

		var scenario ScenarioPayload
		if err := json.Unmarshal([]byte(e.Payload), &scenario); err != nil {
			fmt.Printf("❌ [MODELER] Invalid scenario payload for Workspace [%s]: %v\n", e.WorkspaceID, err)
			return err
		}

		// 👉 SANITIZATION: Protect against Division by Zero and Negative Inputs
		if scenario.CostPerLead <= 0 {
			scenario.CostPerLead = 1.0 // Set a safe minimum baseline
		}
		if scenario.ConversionRate < 0 {
			scenario.ConversionRate = 0
		}
		if scenario.MonthlyBurn < 0 {
			scenario.MonthlyBurn = 0
		}

		// 👉 DETERMINISTIC MATH
		leadsGenerated := scenario.AdSpend / scenario.CostPerLead
		newCustomers := leadsGenerated * (scenario.ConversionRate / 100.0)
		projectedRevenue := newCustomers * scenario.AverageDealSize

		totalExpenses := scenario.MonthlyBurn + scenario.AdSpend
		netProfit := projectedRevenue - totalExpenses

		newCapital := scenario.StartingCapital + netProfit
		runway := 0.0
		if scenario.MonthlyBurn > 0 {
			runway = newCapital / scenario.MonthlyBurn
		}

		result := ProjectionResult{
			NewCustomers: int(newCustomers),
			ProjectedRev: projectedRevenue,
			NetProfit:    netProfit,
			RunwayMonths: runway,
		}

		fmt.Printf("💾 [MODELER] Scenario processed for Workspace [%s].\n", e.WorkspaceID)

		resultJSON, _ := json.Marshal(result)
		fmt.Printf("✅ [MODELER] Scenario calculated. Projected Revenue: $%.2f | Net Profit: $%.2f\n", result.ProjectedRev, result.NetProfit)

		// 👉 BROADCAST: Route the results back to the WebSocket engine
		return f.router.Publish(ctx, protocol.Event{
			WorkspaceID: e.WorkspaceID,
			ID:          fmt.Sprintf("PROJ-%d", time.Now().Unix()),
			Source:      "MODELER_RESULT",
			Payload:     string(resultJSON),
			Timestamp:   time.Now().Unix(),
		})
	}

	// =========================================================================
	// 2. REAL-TIME ACCOUNTING: Intercept system events to log compute costs
	// =========================================================================
	if e.Source == "SENTINEL_LEAD_QUALIFIED" {
		// Log a 5-cent compute expense every time a lead is successfully processed
		if f.DB != nil {
			err := f.DB.LogDoubleEntry(
				ctx,
				e.WorkspaceID,
				"COMPUTE_EXPENSE",
				"OPERATING_CASH",
				0.05,
				fmt.Sprintf("AI Compute Cost for Lead: %s", e.Payload),
				"SYS_AUTO",
			)
			if err != nil {
				log.Printf("⚠️ [CFO] Failed to log internal compute cost: %v", err)
			}
		}
	}
	return nil
}

// =========================================================================
// ENTERPRISE CFO API HANDLERS (Called by Next.js Frontend)
// =========================================================================

// HandleGetLedger serves GET /api/v1/cfo/ledger
func (f *FinancialModeler) HandleGetLedger(w http.ResponseWriter, r *http.Request) {
	// Extract workspace_id securely via the context injected by the auth middleware
	ctxWorkspace := r.Context().Value("workspace_id")
	if ctxWorkspace == nil {
		http.Error(w, `{"error": "Unauthorized. Missing workspace context."}`, http.StatusUnauthorized)
		return
	}
	workspaceID := fmt.Sprintf("%v", ctxWorkspace)

	limitStr := r.URL.Query().Get("limit")
	limit := 50 // Default
	if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
		limit = l
	}

	entries, err := f.DB.GetFinancialLedger(r.Context(), workspaceID, limit)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error": "Failed to fetch ledger: %v"}`, err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(entries)
}

// HandleIngestInvoice serves POST /api/v1/cfo/invoices
func (f *FinancialModeler) HandleIngestInvoice(w http.ResponseWriter, r *http.Request) {
	// Extract workspace_id securely via the context injected by the auth middleware
	ctxWorkspace := r.Context().Value("workspace_id")
	if ctxWorkspace == nil {
		http.Error(w, `{"error": "Unauthorized. Missing workspace context."}`, http.StatusUnauthorized)
		return
	}
	workspaceID := fmt.Sprintf("%v", ctxWorkspace)

	if r.Method != http.MethodPost {
		http.Error(w, `{"error": "Method not allowed. Use POST."}`, http.StatusMethodNotAllowed)
		return
	}

	if f.DB == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"error": "Database subsystem offline"})
		return
	}

	// Parse the raw payload wrapper
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, `{"error": "Failed to read request body"}`, http.StatusBadRequest)
		return
	}

	// Derive JobID deterministically: hash of workspaceID + bodyBytes + upload timestamp rounded to a 5-minute window
	timeWindow := time.Now().UTC().Truncate(5 * time.Minute).Format(time.RFC3339)
	hasher := sha256.New()
	hasher.Write([]byte(workspaceID))
	hasher.Write([]byte(":"))
	hasher.Write(bodyBytes)
	hasher.Write([]byte(":"))
	hasher.Write([]byte(timeWindow))
	jobID := hex.EncodeToString(hasher.Sum(nil))

	var isDuplicate bool

	// Write PENDING state to database
	if err := f.DB.CreateBackgroundJob(r.Context(), jobID, workspaceID); err != nil {
		if pgErr, ok := err.(*pq.Error); ok && pgErr.Code == "23505" {
			isDuplicate = true
		} else {
			slog.Error("Failed to create background job", slog.String("job_id", jobID), slog.Any("error", err))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("Failed to initialize background job: %v", err)})
			return
		}
	}

	if !isDuplicate {
		// Launch background River job or fallback if DB/River is offline
		rc := f.router.RiverClient()
		if rc == nil {
			slog.Warn("DB/River offline, running invoice processing in raw goroutine", slog.String("job_id", jobID))
			go func() {
				_ = f.processInvoiceAsync(context.Background(), jobID, workspaceID, bodyBytes)
			}()
		} else {
			insertRes, err := rc.Insert(r.Context(), orchestrator.ProcessInvoiceJobArgs{
				JobID:       jobID,
				WorkspaceID: workspaceID,
				Payload:     bodyBytes,
			}, nil)
			if err != nil {
				slog.Error("Failed to insert ProcessInvoice job, falling back to raw goroutine", slog.String("job_id", jobID), slog.Any("error", err))
				go func() {
					_ = f.processInvoiceAsync(context.Background(), jobID, workspaceID, bodyBytes)
				}()
			} else if insertRes.UniqueSkippedAsDuplicate {
				isDuplicate = true
			}
		}
	}

	// Instantly return 202 Accepted status with job_id, distinguishing new job from duplicate
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	status := "queued"
	if isDuplicate {
		status = "duplicate"
	}
	json.NewEncoder(w).Encode(map[string]string{
		"status": status,
		"job_id": jobID,
	})
}

// processInvoiceAsync executes the resource-heavy task in a background goroutine or River worker
func (f *FinancialModeler) processInvoiceAsync(ctx context.Context, jobID string, workspaceID string, payload []byte) error {
	// Thread workspace ID through context
	ctx = context.WithValue(ctx, memory.WorkspaceIDKey, workspaceID)

	// 1. Update status to PROCESSING
	_ = f.DB.UpdateJobStatus(ctx, jobID, "PROCESSING", "")

	// 2. Simulate resource-heavy AI OCR processing (sleep for 3 seconds)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(3 * time.Second):
	}

	// Mock structured response from OCR/Agent parsing
	resultData := map[string]interface{}{
		"vendor":       "Zeno Cloud Services",
		"amount":       1500.0,
		"tax":          120.0,
		"status":       "PROCESSED",
		"processed_at": time.Now().Format(time.RFC3339),
	}
	resultBytes, _ := json.Marshal(resultData)

	// Record a double entry to log the outcome of the job
	err := f.DB.LogDoubleEntry(ctx, workspaceID, "EXPENSE", "CASH", 1500.0, "AI OCR Ingested: Zeno Cloud Services", jobID)
	if err != nil {
		slog.Error("Failed to record double entry for invoice", slog.String("job_id", jobID), slog.Any("error", err))
		return err
	}

	// 3. Save outcome as COMPLETED
	err = f.DB.UpdateJobStatus(ctx, jobID, "COMPLETED", string(resultBytes))
	if err != nil {
		slog.Error("Failed to update background job status to COMPLETED", slog.String("job_id", jobID), slog.Any("error", err))
		return err
	}
	return nil
}

// HandleGetJobStatus serves GET /api/v1/cfo/jobs/{id}
func (f *FinancialModeler) HandleGetJobStatus(w http.ResponseWriter, r *http.Request) {
	ctxWorkspace := r.Context().Value("workspace_id")
	if ctxWorkspace == nil {
		http.Error(w, `{"error": "Unauthorized. Missing workspace context."}`, http.StatusUnauthorized)
		return
	}

	if r.Method != http.MethodGet {
		http.Error(w, `{"error": "Method not allowed. Use GET."}`, http.StatusMethodNotAllowed)
		return
	}

	if f.DB == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"error": "Database subsystem offline"})
		return
	}

	// Extract job ID from the URL path (e.g. /api/v1/cfo/jobs/xyz)
	parts := strings.Split(r.URL.Path, "/")
	jobID := parts[len(parts)-1]
	jobID = strings.TrimSpace(jobID)

	if jobID == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "Job ID is required"})
		return
	}

	status, result, err := f.DB.GetJobStatus(r.Context(), jobID)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("Job not found: %v", err)})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"job_id": jobID,
		"status": status,
		"result": result,
	})
}

// --- Modeler River Worker ---

type ProcessInvoiceWorker struct {
	river.WorkerDefaults[orchestrator.ProcessInvoiceJobArgs]
	Modeler *FinancialModeler
}

func (w *ProcessInvoiceWorker) Work(ctx context.Context, job *river.Job[orchestrator.ProcessInvoiceJobArgs]) (err error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	defer func() {
		if r := recover(); r != nil {
			slog.Error("Panic in ProcessInvoiceWorker", slog.Any("panic", r), slog.String("job_id", job.Args.JobID))
			err = fmt.Errorf("panic: %v", r)
		}
	}()

	return w.Modeler.processInvoiceAsync(ctx, job.Args.JobID, job.Args.WorkspaceID, job.Args.Payload)
}

