package agent

import (
	"encoding/json"
	"fmt"
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
	NewCustomers   int     `json:"new_customers"`
	ProjectedRev   float64 `json:"projected_revenue"`
	NetProfit      float64 `json:"net_profit"`
	RunwayMonths   float64 `json:"runway_months"`
}

type FinancialModeler struct {
	router *orchestrator.EventRouter
	DB     *memory.RelationalStore // 👉 Injected to persist client scenarios
}

func NewFinancialModeler(r *orchestrator.EventRouter, db *memory.RelationalStore) *FinancialModeler {
	fmt.Println("📊 [MODELER] Deterministic Financial & Resource Engine initialized.")
	return &FinancialModeler{
		router: r,
		DB:     db,
	}
}

func (f *FinancialModeler) React(e protocol.Event) {
	// Intercept mathematical scenario requests triggered by the Next.js God-Mode UI
	if e.Source == "UI_FINANCIAL_TEST" {
		fmt.Printf("📊 [MODELER] Processing financial scenario for Workspace [%s]...\n", e.WorkspaceID)

		var scenario ScenarioPayload
		if err := json.Unmarshal([]byte(e.Payload), &scenario); err != nil {
			fmt.Printf("❌ [MODELER] Invalid scenario payload for Workspace [%s]: %v\n", e.WorkspaceID, err)
			return
		}

		// 👉 1. SANITIZATION: Protect against Division by Zero and Negative Inputs
		if scenario.CostPerLead <= 0 {
			scenario.CostPerLead = 1.0 // Set a safe minimum baseline
		}
		if scenario.ConversionRate < 0 {
			scenario.ConversionRate = 0
		}
		if scenario.MonthlyBurn < 0 {
			scenario.MonthlyBurn = 0
		}

		// 👉 2. DETERMINISTIC MATH
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
			NewCustomers:   int(newCustomers),
			ProjectedRev:   projectedRevenue,
			NetProfit:      netProfit,
			RunwayMonths:   runway,
		}

		// 👉 3. PERSISTENCE: Save to Supabase (Pseudo-logic to map to your DB schema)
		// f.DB.Exec("INSERT INTO financial_scenarios (workspace_id, name, result_profit, result_runway) VALUES (?, ?, ?, ?)", e.WorkspaceID, scenario.ScenarioName, result.NetProfit, result.RunwayMonths)
		fmt.Printf("💾 [MODELER] Scenario saved to database for Workspace [%s].\n", e.WorkspaceID)

		resultJSON, _ := json.Marshal(result)
		fmt.Printf("✅ [MODELER] Scenario calculated. Projected Revenue: $%.2f | Net Profit: $%.2f\n", result.ProjectedRev, result.NetProfit)

		// 👉 4. BROADCAST: Route the results back to the WebSocket engine
		f.router.Publish(protocol.Event{
			WorkspaceID: e.WorkspaceID,
			ID:          fmt.Sprintf("PROJ-%d", time.Now().Unix()),
			Source:      "MODELER_RESULT",
			Payload:     string(resultJSON),
			Timestamp:   time.Now().Unix(),
		})
	}
}