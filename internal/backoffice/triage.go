package backoffice

import (
	"strings"
)

// IntentMatrix holds the classification result
type IntentMatrix struct {
	Category string
	Urgency  string
	Action   string // System-agnostic action flags (e.g., "CREATE_TICKET", "PAGE_TEAM")
}

// AnalyzePayload is the autonomous brain that scans incoming internal noise
func AnalyzePayload(rawPayload string) IntentMatrix {
	payload := strings.ToLower(rawPayload)

	// Default state
	matrix := IntentMatrix{
		Category: "OPERATIONS",
		Urgency:  "LOW",
		Action:   "LOG_EVENT_TO_DB",
	}

	// 1. Engineering & Infrastructure Domain
	if containsAny(payload, []string{"bug", "crash", "error", "500", "down", "offline", "latency"}) {
		matrix.Category = "ENGINEERING"
		matrix.Urgency  = "HIGH"
		matrix.Action   = "CREATE_TICKET" // Brand agnostic (Works for Jira, Linear, Asana, etc.)
		
		// Escalate to critical if it's a core system failure
		if containsAny(payload, []string{"database", "server", "breach", "payment failed", "production"}) {
			matrix.Urgency = "CRITICAL"
			matrix.Action  = "PAGE_TEAM" // Triggers PagerDuty, SMS, or high-priority Slack ping
		}
		return matrix
	}

	// 2. Financial & Resource Domain
	if containsAny(payload, []string{"invoice", "billing", "receipt", "payment", "wire", "budget"}) {
		matrix.Category = "FINANCE"
		matrix.Urgency  = "NORMAL"
		matrix.Action   = "ROUTE_TO_FINANCE_PIPELINE"
		return matrix
	}

	// 3. Client & Growth Domain
	if containsAny(payload, []string{"client", "upsell", "churn", "demo", "onboarding"}) {
		matrix.Category = "GROWTH"
		matrix.Urgency  = "HIGH"
		matrix.Action   = "NOTIFY_ACCOUNT_OWNER"
		return matrix
	}

	return matrix
}

// Helper function to scan for multiple keywords efficiently
func containsAny(s string, keywords []string) bool {
	for _, kw := range keywords {
		if strings.Contains(s, kw) {
			return true
		}
	}
	return false
}