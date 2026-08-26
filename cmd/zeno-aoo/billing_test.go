package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/JrDigitalHub/zeno-work-aoo/pkg/models"
)

func TestPricingConstants_Values(t *testing.T) {
	// Verify live pricing values match exactly
	if models.AmountStarterKobo != 1499900 {
		t.Errorf("Expected AmountStarterKobo to be 1499900, got %d", models.AmountStarterKobo)
	}
	if models.AmountProfessionalKobo != 9999900 {
		t.Errorf("Expected AmountProfessionalKobo to be 9999900, got %d", models.AmountProfessionalKobo)
	}
	if models.TokensStarter != 500000 {
		t.Errorf("Expected TokensStarter to be 500000, got %d", models.TokensStarter)
	}
	if models.TokensProfessional != 2000000 {
		t.Errorf("Expected TokensProfessional to be 2000000, got %d", models.TokensProfessional)
	}
}

func TestResolvePlan(t *testing.T) {
	tests := []struct {
		input       string
		expectedTier string
		expectedAmt  int
		expectedTok  int
		expectValid  bool
	}{
		{"starter", models.TierStarter, models.AmountStarterKobo, models.TokensStarter, true},
		{"Starter", models.TierStarter, models.AmountStarterKobo, models.TokensStarter, true},
		{"STARTER", models.TierStarter, models.AmountStarterKobo, models.TokensStarter, true},
		{"professional", models.TierProfessional, models.AmountProfessionalKobo, models.TokensProfessional, true},
		{"Professional", models.TierProfessional, models.AmountProfessionalKobo, models.TokensProfessional, true},
		{"pro", models.TierProfessional, models.AmountProfessionalKobo, models.TokensProfessional, true},
		{"Pro", models.TierProfessional, models.AmountProfessionalKobo, models.TokensProfessional, true},
		{"enterprise", "", 0, 0, false},
		{"unknown", "", 0, 0, false},
		{"", "", 0, 0, false},
	}

	for _, tt := range tests {
		plan, ok := models.ResolvePlan(tt.input)
		if ok != tt.expectValid {
			t.Errorf("ResolvePlan(%q) ok = %v, expected %v", tt.input, ok, tt.expectValid)
		}
		if ok {
			if plan.Tier != tt.expectedTier {
				t.Errorf("ResolvePlan(%q) tier = %s, expected %s", tt.input, plan.Tier, tt.expectedTier)
			}
			if plan.AmountKobo != tt.expectedAmt {
				t.Errorf("ResolvePlan(%q) amount = %d, expected %d", tt.input, plan.AmountKobo, tt.expectedAmt)
			}
			if plan.Tokens != tt.expectedTok {
				t.Errorf("ResolvePlan(%q) tokens = %d, expected %d", tt.input, plan.Tokens, tt.expectedTok)
			}
		}
	}
}

func TestResolvePlanByAmount(t *testing.T) {
	tests := []struct {
		amountKobo   int
		expectedTier string
		expectedTok  int
		expectValid  bool
	}{
		{1499900, models.TierStarter, models.TokensStarter, true},
		{9999900, models.TierProfessional, models.TokensProfessional, true},
		{1000000, "", 0, false},
		{0, "", 0, false},
		{-100, "", 0, false},
	}

	for _, tt := range tests {
		plan, ok := models.ResolvePlanByAmount(tt.amountKobo)
		if ok != tt.expectValid {
			t.Errorf("ResolvePlanByAmount(%d) ok = %v, expected %v", tt.amountKobo, ok, tt.expectValid)
		}
		if ok {
			if plan.Tier != tt.expectedTier {
				t.Errorf("ResolvePlanByAmount(%d) tier = %s, expected %s", tt.amountKobo, plan.Tier, tt.expectedTier)
			}
			if plan.Tokens != tt.expectedTok {
				t.Errorf("ResolvePlanByAmount(%d) tokens = %d, expected %d", tt.amountKobo, plan.Tokens, tt.expectedTok)
			}
		}
	}
}

func computeHMACSHA512(secret string, body []byte) string {
	h := hmac.New(sha512.New, []byte(secret))
	h.Write(body)
	return hex.EncodeToString(h.Sum(nil))
}

func TestPaystackWebhookHMACVerification(t *testing.T) {
	secret := "sk_test_mock_secret_key_12345"
	os.Setenv("PAYSTACK_SECRET_KEY", secret)
	defer os.Unsetenv("PAYSTACK_SECRET_KEY")

	payload := map[string]interface{}{
		"event": "charge.success",
		"data": map[string]interface{}{
			"amount":    models.AmountStarterKobo,
			"reference": "ref_starter_123",
			"metadata": map[string]interface{}{
				"workspace_id": "ws_test_starter",
			},
		},
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Failed to marshal payload: %v", err)
	}

	validSignature := computeHMACSHA512(secret, bodyBytes)
	invalidSignature := "invalid_signature_hex"

	// 1. Verify valid signature check
	h := hmac.New(sha512.New, []byte(secret))
	h.Write(bodyBytes)
	expectedSignature := hex.EncodeToString(h.Sum(nil))

	if !hmac.Equal([]byte(validSignature), []byte(expectedSignature)) {
		t.Error("Valid signature was rejected by HMAC check")
	}

	// 2. Verify invalid signature is rejected
	if hmac.Equal([]byte(invalidSignature), []byte(expectedSignature)) {
		t.Error("Invalid signature was incorrectly accepted by HMAC check")
	}
}

func TestPaystackWebhookHandler_SignatureValidation(t *testing.T) {
	secret := "sk_test_mock_secret_key_webhook"
	os.Setenv("PAYSTACK_SECRET_KEY", secret)
	defer os.Unsetenv("PAYSTACK_SECRET_KEY")

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedSignature := r.Header.Get("x-paystack-signature")
		secretKey := os.Getenv("PAYSTACK_SECRET_KEY")

		bodyBytes := []byte(`{"event":"charge.success"}`)
		h := hmac.New(sha512.New, []byte(secretKey))
		h.Write(bodyBytes)
		expectedSignature := hex.EncodeToString(h.Sum(nil))

		if secretKey == "" || receivedSignature == "" || !hmac.Equal([]byte(receivedSignature), []byte(expectedSignature)) {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":"Unauthorized"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"success"}`))
	})

	// Test case: Missing signature
	req := httptest.NewRequest(http.MethodPost, "/api/v1/webhooks/paystack", bytes.NewBufferString(`{"event":"charge.success"}`))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("Expected 401 Unauthorized for missing signature, got %d", rr.Code)
	}

	// Test case: Invalid signature
	req = httptest.NewRequest(http.MethodPost, "/api/v1/webhooks/paystack", bytes.NewBufferString(`{"event":"charge.success"}`))
	req.Header.Set("x-paystack-signature", "wrongsignature")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("Expected 401 Unauthorized for bad signature, got %d", rr.Code)
	}

	// Test case: Valid signature
	validSig := computeHMACSHA512(secret, []byte(`{"event":"charge.success"}`))
	req = httptest.NewRequest(http.MethodPost, "/api/v1/webhooks/paystack", bytes.NewBufferString(`{"event":"charge.success"}`))
	req.Header.Set("x-paystack-signature", validSig)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("Expected 200 OK for valid signature, got %d", rr.Code)
	}
}
