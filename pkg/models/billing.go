package models

import (
	"math"
	"os"
	"strings"
)

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

// International USD Pricing Constants
const (
	AmountStarterUSD      = 29.0
	AmountProfessionalUSD = 99.0
)

// Currencies Supported
const (
	CurrencyNGN = "NGN"
	CurrencyUSD = "USD"
)

// Plan Token Allocations
const (
	TokensStarter      = 500000
	TokensProfessional = 2000000
	TokensTrialDefault = 50000
)

// Payment Gateways
const (
	GatewayFlutterwave = "flutterwave"
	GatewayPaystack    = "paystack"
)

// PlanDetails defines the billing details of a subscription tier
type PlanDetails struct {
	Tier       string  `json:"tier"`
	AmountKobo int     `json:"amount_kobo"`
	AmountUSD  float64 `json:"amount_usd"`
	Tokens     int     `json:"tokens"`
}

// AmountMajor returns the subscription pricing amount in major currency units (e.g. NGN 14999 not 1499900 kobo).
func (p PlanDetails) AmountMajor() float64 {
	return float64(p.AmountKobo) / 100.0
}

// ResolvePlan normalizes and maps incoming plan/tier identifiers to canonical plan details.
// Supports case-insensitive matches (e.g., "starter", "Starter", "professional", "Professional", "pro", "Pro").
func ResolvePlan(identifier string) (PlanDetails, bool) {
	switch strings.ToLower(strings.TrimSpace(identifier)) {
	case "starter":
		return PlanDetails{
			Tier:       TierStarter,
			AmountKobo: AmountStarterKobo,
			AmountUSD:  AmountStarterUSD,
			Tokens:     TokensStarter,
		}, true
	case "professional", "pro":
		return PlanDetails{
			Tier:       TierProfessional,
			AmountKobo: AmountProfessionalKobo,
			AmountUSD:  AmountProfessionalUSD,
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
			AmountUSD:  AmountStarterUSD,
			Tokens:     TokensStarter,
		}, true
	case AmountProfessionalKobo:
		return PlanDetails{
			Tier:       TierProfessional,
			AmountKobo: AmountProfessionalKobo,
			AmountUSD:  AmountProfessionalUSD,
			Tokens:     TokensProfessional,
		}, true
	default:
		return PlanDetails{}, false
	}
}

// ResolvePlanByAmountAndCurrency maps payment amount in major units to canonical plan details,
// supporting multi-currency (NGN and USD) with rounding tolerance for floating-point comparisons.
func ResolvePlanByAmountAndCurrency(amount float64, currency string) (PlanDetails, bool) {
	if amount <= 0 {
		return PlanDetails{}, false
	}
	curr := strings.ToUpper(strings.TrimSpace(currency))
	switch curr {
	case CurrencyUSD:
		cents := int(math.Round(amount * 100))
		switch cents {
		case int(AmountStarterUSD * 100):
			return PlanDetails{
				Tier:       TierStarter,
				AmountKobo: AmountStarterKobo,
				AmountUSD:  AmountStarterUSD,
				Tokens:     TokensStarter,
			}, true
		case int(AmountProfessionalUSD * 100):
			return PlanDetails{
				Tier:       TierProfessional,
				AmountKobo: AmountProfessionalKobo,
				AmountUSD:  AmountProfessionalUSD,
				Tokens:     TokensProfessional,
			}, true
		default:
			return PlanDetails{}, false
		}
	case CurrencyNGN, "":
		kobo := int(math.Round(amount * 100))
		return ResolvePlanByAmount(kobo)
	default:
		return PlanDetails{}, false
	}
}

// ResolvePlanByMajorAmount maps payment amount in major units (NGN or USD fallback) to canonical plan details,
// supporting floating-point comparison with rounding tolerance.
func ResolvePlanByMajorAmount(amount float64) (PlanDetails, bool) {
	if plan, ok := ResolvePlanByAmountAndCurrency(amount, CurrencyNGN); ok {
		return plan, true
	}
	return ResolvePlanByAmountAndCurrency(amount, CurrencyUSD)
}

// GetActivePaymentGateway returns the configured active gateway from ACTIVE_PAYMENT_GATEWAY,
// defaulting to "flutterwave" and falling back to "paystack".
func GetActivePaymentGateway() string {
	gw := strings.ToLower(strings.TrimSpace(os.Getenv("ACTIVE_PAYMENT_GATEWAY")))
	if gw == GatewayPaystack {
		return GatewayPaystack
	}
	return GatewayFlutterwave
}

// FlutterwaveConfig encapsulates the Flutterwave environment settings.
type FlutterwaveConfig struct {
	PublicKey  string
	SecretKey  string
	SecretHash string
	BaseURL    string
}

// GetFlutterwaveConfig reads Flutterwave environment variables.
func GetFlutterwaveConfig() FlutterwaveConfig {
	baseURL := strings.TrimRight(strings.TrimSpace(os.Getenv("FLW_BASE_URL")), "/")
	if baseURL == "" {
		baseURL = "https://api.flutterwave.com/v3"
	}
	return FlutterwaveConfig{
		PublicKey:  strings.TrimSpace(os.Getenv("FLW_PUBLIC_KEY")),
		SecretKey:  strings.TrimSpace(os.Getenv("FLW_SECRET_KEY")),
		SecretHash: strings.TrimSpace(os.Getenv("FLW_SECRET_HASH")),
		BaseURL:    baseURL,
	}
}
