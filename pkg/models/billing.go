package models

import "strings"

// Plan Tier Names
const (
	TierStarter      = "Starter"
	TierProfessional = "Professional"
	TierTrial        = "Trial"
)

// Plan Pricing Constants (in Kobo: 1 NGN = 100 Kobo)
const (
	// Starter: ₦14,999 -> 1,499,900 kobo
	AmountStarterKobo = 1499900

	// Professional: ₦99,999 -> 9,999,900 kobo
	AmountProfessionalKobo = 9999900
)

// Plan Token Allocations
const (
	TokensStarter      = 500000
	TokensProfessional = 2000000
	TokensTrialDefault = 50000
)

// PlanDetails defines the billing details of a subscription tier
type PlanDetails struct {
	Tier       string `json:"tier"`
	AmountKobo int    `json:"amount_kobo"`
	Tokens     int    `json:"tokens"`
}

// ResolvePlan normalizes and maps incoming plan/tier identifiers to canonical plan details.
// Supports case-insensitive matches (e.g., "starter", "Starter", "professional", "Professional", "pro", "Pro").
func ResolvePlan(identifier string) (PlanDetails, bool) {
	switch strings.ToLower(strings.TrimSpace(identifier)) {
	case "starter":
		return PlanDetails{
			Tier:       TierStarter,
			AmountKobo: AmountStarterKobo,
			Tokens:     TokensStarter,
		}, true
	case "professional", "pro":
		return PlanDetails{
			Tier:       TierProfessional,
			AmountKobo: AmountProfessionalKobo,
			Tokens:     TokensProfessional,
		}, true
	default:
		return PlanDetails{}, false
	}
}

// ResolvePlanByAmount maps payment amount in kobo to the canonical plan details.
func ResolvePlanByAmount(amountKobo int) (PlanDetails, bool) {
	switch amountKobo {
	case AmountStarterKobo:
		return PlanDetails{
			Tier:       TierStarter,
			AmountKobo: AmountStarterKobo,
			Tokens:     TokensStarter,
		}, true
	case AmountProfessionalKobo:
		return PlanDetails{
			Tier:       TierProfessional,
			AmountKobo: AmountProfessionalKobo,
			Tokens:     TokensProfessional,
		}, true
	default:
		return PlanDetails{}, false
	}
}
