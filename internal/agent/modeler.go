package agent

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/JrDigitalHub/zeno-work-aoo/internal/memory"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/orchestrator"
	"github.com/JrDigitalHub/zeno-work-aoo/pkg/protocol"
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

func (f *FinancialModeler) React(e protocol.Event) {
	// =========================================================================
	// 1. FORWARD PROJECTIONS: Intercept math requests from the UI
	// =========================================================================
	if e.Source == "UI_FINANCIAL_TEST" {
		fmt.Printf("📊 [MODELER] Processing financial scenario for Workspace [%s]...\n", e.WorkspaceID)

		var scenario ScenarioPayload
		if err := json.Unmarshal([]byte(e.Payload), &scenario); err != nil {
			fmt.Printf("❌ [MODELER] Invalid scenario payload for Workspace [%s]: %v\n", e.WorkspaceID, err)
			return
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
		f.router.Publish(protocol.Event{
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

	entries, err := f.DB.GetFinancialLedger(workspaceID, limit)
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

	// 1. In a full production setup, parse the multipart/form-data PDF upload here
	// 2. Send the raw text/image to Gemini Flash via your Oracle agent
	// 3. Gemini returns structured JSON (Vendor, Amount, Tax)
	// 4. Pass that JSON into f.DB.LogDoubleEntry()

	// For now, we return a mock success to verify the API pipeline connection
	log.Println("🧾 [CFO] Invoice ingestion endpoint hit. Awaiting OCR integration.")

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{"status": "PROCESSING", "message": "Invoice received for OCR extraction."}`))
}
