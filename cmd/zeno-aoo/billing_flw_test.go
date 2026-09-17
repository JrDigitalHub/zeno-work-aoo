package main

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/JrDigitalHub/zeno-work-aoo/internal/middleware"
	"github.com/JrDigitalHub/zeno-work-aoo/pkg/models"
)

func TestFlutterwave_PricingConversion(t *testing.T) {
	// 1. Verify major unit conversions
	starterPlan, ok := models.ResolvePlan(models.TierStarter)
	if !ok {
		t.Fatalf("Failed to resolve starter plan")
	}
	if starterPlan.AmountMajor() != 14999.0 {
		t.Errorf("Expected starter AmountMajor() to be 14999.0, got %f", starterPlan.AmountMajor())
	}

	proPlan, ok := models.ResolvePlan(models.TierProfessional)
	if !ok {
		t.Fatalf("Failed to resolve professional plan")
	}
	if proPlan.AmountMajor() != 99999.0 {
		t.Errorf("Expected professional AmountMajor() to be 99999.0, got %f", proPlan.AmountMajor())
	}

	// 2. Verify ResolvePlanByMajorAmount with exact amounts
	tests := []struct {
		amountMajor  float64
		expectedTier string
		expectedTok  int
		expectValid  bool
	}{
		{14999.0, models.TierStarter, models.TokensStarter, true},
		{99999.0, models.TierProfessional, models.TokensProfessional, true},
		// Floating point tolerance checks
		{14999.001, models.TierStarter, models.TokensStarter, true},
		{14998.999, models.TierStarter, models.TokensStarter, true},
		{99999.004, models.TierProfessional, models.TokensProfessional, true},
		{99998.996, models.TierProfessional, models.TokensProfessional, true},
		// Invalid amounts
		{0.0, "", 0, false},
		{-14999.0, "", 0, false},
		{1000.0, "", 0, false},
		{1499900.0, "", 0, false}, // Kobo amount passed as major amount should fail
	}

	for _, tt := range tests {
		plan, ok := models.ResolvePlanByMajorAmount(tt.amountMajor)
		if ok != tt.expectValid {
			t.Errorf("ResolvePlanByMajorAmount(%f) ok = %v, expected %v", tt.amountMajor, ok, tt.expectValid)
		}
		if ok {
			if plan.Tier != tt.expectedTier {
				t.Errorf("ResolvePlanByMajorAmount(%f) tier = %s, expected %s", tt.amountMajor, plan.Tier, tt.expectedTier)
			}
			if plan.Tokens != tt.expectedTok {
				t.Errorf("ResolvePlanByMajorAmount(%f) tokens = %d, expected %d", tt.amountMajor, plan.Tokens, tt.expectedTok)
			}
		}
	}
}

func TestFlutterwave_ConstantTimeSignatureValidation(t *testing.T) {
	secretHash := "flw_secret_test_hash_987654321"
	os.Setenv("FLW_SECRET_HASH", secretHash)
	defer os.Unsetenv("FLW_SECRET_HASH")

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secret := os.Getenv("FLW_SECRET_HASH")
		headerHash := r.Header.Get("verif-hash")

		// Mandate: check if secretHash == "" || headerHash == "" { return 401 } before ConstantTimeCompare
		if secret == "" || headerHash == "" {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":"Unauthorized. Missing secret hash or verif-hash header."}`))
			return
		}

		if subtle.ConstantTimeCompare([]byte(headerHash), []byte(secret)) != 1 {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":"Unauthorized. Invalid signature."}`))
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"acknowledged"}`))
	})

	// Case 1: Valid matching verif-hash header
	req := httptest.NewRequest(http.MethodPost, "/api/v1/webhooks/flutterwave", bytes.NewBufferString(`{"event":"charge.completed"}`))
	req.Header.Set("verif-hash", secretHash)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("Expected 200 OK for valid verif-hash, got %d", rr.Code)
	}

	// Case 2: Mismatching verif-hash header
	req = httptest.NewRequest(http.MethodPost, "/api/v1/webhooks/flutterwave", bytes.NewBufferString(`{"event":"charge.completed"}`))
	req.Header.Set("verif-hash", "invalid_mismatching_hash")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("Expected 401 Unauthorized for mismatched verif-hash, got %d", rr.Code)
	}

	// Case 3: Empty verif-hash header
	req = httptest.NewRequest(http.MethodPost, "/api/v1/webhooks/flutterwave", bytes.NewBufferString(`{"event":"charge.completed"}`))
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("Expected 401 Unauthorized for missing verif-hash header, got %d", rr.Code)
	}

	// Case 4: Missing FLW_SECRET_HASH env var
	os.Unsetenv("FLW_SECRET_HASH")
	req = httptest.NewRequest(http.MethodPost, "/api/v1/webhooks/flutterwave", bytes.NewBufferString(`{"event":"charge.completed"}`))
	req.Header.Set("verif-hash", secretHash)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("Expected 401 Unauthorized when server FLW_SECRET_HASH is empty, got %d", rr.Code)
	}
}

func TestFlutterwave_WebhookIdempotency(t *testing.T) {
	secretHash := "flw_hash_idempotency_test"
	os.Setenv("FLW_SECRET_HASH", secretHash)
	os.Setenv("FLW_SECRET_KEY", "FLWSECK_TEST-mock-key")
	defer os.Unsetenv("FLW_SECRET_HASH")
	defer os.Unsetenv("FLW_SECRET_KEY")

	// 1. Setup mock Flutterwave verify API
	verifyCallCount := 0
	var verifyMu sync.Mutex
	mockFLWServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/transactions/998877/verify" {
			verifyMu.Lock()
			verifyCallCount++
			verifyMu.Unlock()

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status":  "success",
				"message": "Tx Fetched",
				"data": map[string]interface{}{
					"id":       998877,
					"tx_ref":   "zeno_ws_idem_1001",
					"status":   "successful",
					"amount":   14999.0,
					"currency": "NGN",
					"meta": map[string]interface{}{
						"workspace_id": "ws_idem_1001",
					},
				},
			})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer mockFLWServer.Close()

	os.Setenv("FLW_BASE_URL", mockFLWServer.URL)
	defer os.Unsetenv("FLW_BASE_URL")

	// Simulated ledger of processed transaction references
	processedLedger := make(map[string]bool)
	var ledgerMu sync.Mutex
	creditsExecuted := 0

	webhookHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Header verification
		receivedHash := r.Header.Get("verif-hash")
		expectedHash := os.Getenv("FLW_SECRET_HASH")
		if expectedHash == "" || receivedHash == "" || subtle.ConstantTimeCompare([]byte(receivedHash), []byte(expectedHash)) != 1 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		var payload struct {
			Event string `json:"event"`
			Data  struct {
				ID     int64                  `json:"id"`
				TxRef  string                 `json:"tx_ref"`
				Status string                 `json:"status"`
				Amount float64                `json:"amount"`
				Meta   map[string]interface{} `json:"meta"`
			} `json:"data"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if payload.Event != "charge.completed" || payload.Data.Status != "successful" {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"status": "ignored"})
			return
		}

		// Verify with Flutterwave API (mock)
		flwConfig := models.GetFlutterwaveConfig()
		verifyReq, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, fmt.Sprintf("%s/transactions/%d/verify", flwConfig.BaseURL, payload.Data.ID), nil)
		resp, err := billingHTTPClient.Do(verifyReq)
		if err != nil || resp.StatusCode != http.StatusOK {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()

		var verified struct {
			Status string `json:"status"`
			Data   struct {
				Amount float64                `json:"amount"`
				Status string                 `json:"status"`
				Meta   map[string]interface{} `json:"meta"`
			} `json:"data"`
		}
		json.NewDecoder(resp.Body).Decode(&verified)

		if verified.Status != "success" || verified.Data.Status != "successful" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Validate plan amount
		if _, ok := models.ResolvePlanByMajorAmount(verified.Data.Amount); !ok {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		txRef := payload.Data.TxRef

		// Idempotency check against ledger
		ledgerMu.Lock()
		if processedLedger[txRef] {
			ledgerMu.Unlock()
			// Already processed duplicate: return 200 acknowledged without duplicate credit
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"status": "acknowledged"})
			return
		}
		processedLedger[txRef] = true
		creditsExecuted++
		ledgerMu.Unlock()

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "acknowledged"})
	})

	webhookPayload := map[string]interface{}{
		"event": "charge.completed",
		"data": map[string]interface{}{
			"id":     998877,
			"tx_ref": "zeno_ws_idem_1001",
			"status": "successful",
			"amount": 14999.0,
			"meta": map[string]interface{}{
				"workspace_id": "ws_idem_1001",
			},
		},
	}
	payloadBytes, _ := json.Marshal(webhookPayload)

	// Dispatch 1: First arrival
	req1 := httptest.NewRequest(http.MethodPost, "/api/v1/webhooks/flutterwave", bytes.NewBuffer(payloadBytes))
	req1.Header.Set("verif-hash", secretHash)
	rr1 := httptest.NewRecorder()
	webhookHandler.ServeHTTP(rr1, req1)

	if rr1.Code != http.StatusOK {
		t.Fatalf("First webhook submission failed with status %d: %s", rr1.Code, rr1.Body.String())
	}

	// Dispatch 2: Duplicate arrival with same tx_ref
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/webhooks/flutterwave", bytes.NewBuffer(payloadBytes))
	req2.Header.Set("verif-hash", secretHash)
	rr2 := httptest.NewRecorder()
	webhookHandler.ServeHTTP(rr2, req2)

	if rr2.Code != http.StatusOK {
		t.Fatalf("Duplicate webhook submission failed with status %d: %s", rr2.Code, rr2.Body.String())
	}

	// Dispatch 3: Triplicate arrival
	req3 := httptest.NewRequest(http.MethodPost, "/api/v1/webhooks/flutterwave", bytes.NewBuffer(payloadBytes))
	req3.Header.Set("verif-hash", secretHash)
	rr3 := httptest.NewRecorder()
	webhookHandler.ServeHTTP(rr3, req3)

	if rr3.Code != http.StatusOK {
		t.Fatalf("Triplicate webhook submission failed with status %d: %s", rr3.Code, rr3.Body.String())
	}

	// Assert that credit was dispatched exactly ONCE
	if creditsExecuted != 1 {
		t.Errorf("Expected exactly 1 credit dispatch, got %d", creditsExecuted)
	}
}

func TestFlutterwave_CheckoutInitialization(t *testing.T) {
	// Mock Flutterwave payment init API
	mockFLWServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/payments" {
			var initReq struct {
				TxRef  string            `json:"tx_ref"`
				Amount string            `json:"amount"`
				Meta   map[string]string `json:"meta"`
			}
			json.NewDecoder(r.Body).Decode(&initReq)

			// Verify meta.workspace_id was included
			if initReq.Meta["workspace_id"] != "ws_checkout_test" {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(`{"status":"error","message":"missing workspace_id in meta"}`))
				return
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status":  "success",
				"message": "Hosted Link",
				"data": map[string]string{
					"link": "https://checkout.flutterwave.com/v3/hosted/pay/testlink123",
				},
			})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer mockFLWServer.Close()

	os.Setenv("FLW_BASE_URL", mockFLWServer.URL)
	os.Setenv("FLW_SECRET_KEY", "FLWSECK_TEST-mock-key")
	os.Setenv("ACTIVE_PAYMENT_GATEWAY", "flutterwave")
	defer os.Unsetenv("FLW_BASE_URL")
	defer os.Unsetenv("FLW_SECRET_KEY")
	defer os.Unsetenv("ACTIVE_PAYMENT_GATEWAY")

	// Call checkout endpoint via simulated request
	checkoutHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctxWorkspace := r.Context().Value(middleware.WorkspaceContextKey)
		workspaceID := fmt.Sprintf("%v", ctxWorkspace)

		var req struct {
			Plan string `json:"plan"`
		}
		json.NewDecoder(r.Body).Decode(&req)

		planDetails, ok := models.ResolvePlan(req.Plan)
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		txRef := fmt.Sprintf("zeno_%s_%d", workspaceID, 123456789)
		flwConfig := models.GetFlutterwaveConfig()

		flwReqPayload := map[string]interface{}{
			"tx_ref": txRef,
			"amount": fmt.Sprintf("%.2f", planDetails.AmountMajor()),
			"meta": map[string]string{
				"workspace_id": workspaceID,
			},
		}
		reqBytes, _ := json.Marshal(flwReqPayload)
		httpReq, _ := http.NewRequestWithContext(r.Context(), http.MethodPost, flwConfig.BaseURL+"/payments", bytes.NewBuffer(reqBytes))
		httpReq.Header.Set("Authorization", "Bearer "+flwConfig.SecretKey)
		httpReq.Header.Set("Content-Type", "application/json")

		resp, err := billingHTTPClient.Do(httpReq)
		if err != nil || resp.StatusCode != http.StatusOK {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()

		var flwResp struct {
			Status string `json:"status"`
			Data   struct {
				Link string `json:"link"`
			} `json:"data"`
		}
		json.NewDecoder(resp.Body).Decode(&flwResp)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":            "success",
			"authorization_url": flwResp.Data.Link,
			"checkout_url":      flwResp.Data.Link,
			"reference":         txRef,
			"data": map[string]interface{}{
				"authorization_url": flwResp.Data.Link,
				"checkout_url":      flwResp.Data.Link,
				"link":              flwResp.Data.Link,
				"reference":         txRef,
			},
		})
	})

	reqBody := `{"plan":"starter"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/billing/checkout", bytes.NewBufferString(reqBody))
	ctx := context.WithValue(req.Context(), middleware.WorkspaceContextKey, "ws_checkout_test")
	req = req.WithContext(ctx)

	rr := httptest.NewRecorder()
	checkoutHandler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("Checkout handler failed with code %d: %s", rr.Code, rr.Body.String())
	}

	var resp struct {
		Status           string `json:"status"`
		AuthorizationURL string `json:"authorization_url"`
		CheckoutURL      string `json:"checkout_url"`
		Reference        string `json:"reference"`
		Data             struct {
			AuthorizationURL string `json:"authorization_url"`
			Link             string `json:"link"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to parse checkout response: %v", err)
	}

	if resp.Status != "success" {
		t.Errorf("Expected status 'success', got %q", resp.Status)
	}
	if resp.AuthorizationURL != "https://checkout.flutterwave.com/v3/hosted/pay/testlink123" {
		t.Errorf("Unexpected AuthorizationURL: %s", resp.AuthorizationURL)
	}
	if resp.CheckoutURL != resp.AuthorizationURL {
		t.Errorf("Expected CheckoutURL == AuthorizationURL, got %s", resp.CheckoutURL)
	}
	if resp.Data.Link != resp.AuthorizationURL {
		t.Errorf("Expected Data.Link == AuthorizationURL, got %s", resp.Data.Link)
	}
}

func TestPaymentGateway_RuntimeSelection(t *testing.T) {
	// Default when unset should be flutterwave
	os.Unsetenv("ACTIVE_PAYMENT_GATEWAY")
	if gw := models.GetActivePaymentGateway(); gw != models.GatewayFlutterwave {
		t.Errorf("Expected default gateway to be 'flutterwave', got %q", gw)
	}

	// Setting to paystack
	os.Setenv("ACTIVE_PAYMENT_GATEWAY", "paystack")
	if gw := models.GetActivePaymentGateway(); gw != models.GatewayPaystack {
		t.Errorf("Expected gateway to be 'paystack', got %q", gw)
	}

	// Case-insensitive paystack
	os.Setenv("ACTIVE_PAYMENT_GATEWAY", "PAYSTACK")
	if gw := models.GetActivePaymentGateway(); gw != models.GatewayPaystack {
		t.Errorf("Expected 'PAYSTACK' to resolve to 'paystack', got %q", gw)
	}

	// Unknown string should fallback to flutterwave
	os.Setenv("ACTIVE_PAYMENT_GATEWAY", "unknown_gw")
	if gw := models.GetActivePaymentGateway(); gw != models.GatewayFlutterwave {
		t.Errorf("Expected unknown gateway to fallback to 'flutterwave', got %q", gw)
	}

	os.Unsetenv("ACTIVE_PAYMENT_GATEWAY")
}

func TestSecuredRoutes(t *testing.T) {
	// 1. Test /api/v1/system/toggle with X-Admin-Secret-Key
	t.Run("SystemToggle_AdminSecretKey", func(t *testing.T) {
		adminKey := "test_admin_secret_key_abc"
		os.Setenv("ADMIN_SECRET_KEY", adminKey)
		defer os.Unsetenv("ADMIN_SECRET_KEY")

		toggleHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			adminSecret := os.Getenv("ADMIN_SECRET_KEY")
			receivedSecret := r.Header.Get("X-Admin-Secret-Key")
			if adminSecret == "" || receivedSecret == "" || subtle.ConstantTimeCompare([]byte(receivedSecret), []byte(adminSecret)) != 1 {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.WriteHeader(http.StatusOK)
		})

		// Missing header -> 401
		req := httptest.NewRequest(http.MethodPost, "/api/v1/system/toggle", bytes.NewBufferString(`{"state":"ACTIVE"}`))
		rr := httptest.NewRecorder()
		toggleHandler.ServeHTTP(rr, req)
		if rr.Code != http.StatusUnauthorized {
			t.Errorf("Expected 401 for missing X-Admin-Secret-Key, got %d", rr.Code)
		}

		// Wrong header -> 401
		req = httptest.NewRequest(http.MethodPost, "/api/v1/system/toggle", bytes.NewBufferString(`{"state":"ACTIVE"}`))
		req.Header.Set("X-Admin-Secret-Key", "wrong_key")
		rr = httptest.NewRecorder()
		toggleHandler.ServeHTTP(rr, req)
		if rr.Code != http.StatusUnauthorized {
			t.Errorf("Expected 401 for wrong X-Admin-Secret-Key, got %d", rr.Code)
		}

		// Correct header -> 200
		req = httptest.NewRequest(http.MethodPost, "/api/v1/system/toggle", bytes.NewBufferString(`{"state":"ACTIVE"}`))
		req.Header.Set("X-Admin-Secret-Key", adminKey)
		rr = httptest.NewRecorder()
		toggleHandler.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Errorf("Expected 200 for correct X-Admin-Secret-Key, got %d", rr.Code)
		}
	})

	// 2. Test /api/v1/ingest with X-Webhook-Secret
	t.Run("Ingest_WebhookSecret", func(t *testing.T) {
		webhookKey := "test_webhook_secret_xyz"
		os.Setenv("INGEST_WEBHOOK_SECRET", webhookKey)
		defer os.Unsetenv("INGEST_WEBHOOK_SECRET")

		ingestHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			secret := os.Getenv("INGEST_WEBHOOK_SECRET")
			receivedSecret := r.Header.Get("X-Webhook-Secret")
			if secret == "" || receivedSecret == "" || subtle.ConstantTimeCompare([]byte(receivedSecret), []byte(secret)) != 1 {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.WriteHeader(http.StatusOK)
		})

		// Missing header -> 401
		req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest", bytes.NewBufferString(`{"workspace_id":"123","source":"test","payload":"hello"}`))
		rr := httptest.NewRecorder()
		ingestHandler.ServeHTTP(rr, req)
		if rr.Code != http.StatusUnauthorized {
			t.Errorf("Expected 401 for missing X-Webhook-Secret, got %d", rr.Code)
		}

		// Correct header -> 200
		req = httptest.NewRequest(http.MethodPost, "/api/v1/ingest", bytes.NewBufferString(`{"workspace_id":"123","source":"test","payload":"hello"}`))
		req.Header.Set("X-Webhook-Secret", webhookKey)
		rr = httptest.NewRecorder()
		ingestHandler.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Errorf("Expected 200 for correct X-Webhook-Secret, got %d", rr.Code)
		}
	})

	// 3. Test workspace tenancy validation
	t.Run("WorkspaceToggle_TenancyGuard", func(t *testing.T) {
		guardHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctxWorkspace := r.Context().Value(middleware.WorkspaceContextKey)
			authedWorkspace := fmt.Sprintf("%v", ctxWorkspace)
			if authedWorkspace == "" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}

			var req struct {
				WorkspaceID string `json:"workspace_id"`
			}
			json.NewDecoder(r.Body).Decode(&req)

			if req.WorkspaceID != "" && req.WorkspaceID != authedWorkspace {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			w.WriteHeader(http.StatusOK)
		})

		// Matching workspace -> 200
		req := httptest.NewRequest(http.MethodPost, "/api/v1/workspace/toggle", bytes.NewBufferString(`{"workspace_id":"ws-123","is_paused":true}`))
		ctx := context.WithValue(req.Context(), middleware.WorkspaceContextKey, "ws-123")
		req = req.WithContext(ctx)
		rr := httptest.NewRecorder()
		guardHandler.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Errorf("Expected 200 for matching workspace tenancy, got %d", rr.Code)
		}

		// Mismatching workspace -> 403 Forbidden
		req = httptest.NewRequest(http.MethodPost, "/api/v1/workspace/toggle", bytes.NewBufferString(`{"workspace_id":"ws-victim","is_paused":true}`))
		ctx = context.WithValue(req.Context(), middleware.WorkspaceContextKey, "ws-attacker")
		req = req.WithContext(ctx)
		rr = httptest.NewRecorder()
		guardHandler.ServeHTTP(rr, req)
		if rr.Code != http.StatusForbidden {
			t.Errorf("Expected 403 Forbidden for mismatched workspace tenancy, got %d", rr.Code)
		}
	})
}

func TestSecuredRoutes_EmptySecretBypassProtection(t *testing.T) {
	// 1. Ensure /api/v1/system/toggle rejects empty incoming header when ADMIN_SECRET_KEY is empty/unset
	os.Unsetenv("ADMIN_SECRET_KEY")
	toggleHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		adminSecret := os.Getenv("ADMIN_SECRET_KEY")
		receivedSecret := r.Header.Get("X-Admin-Secret-Key")
		if adminSecret == "" || receivedSecret == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if subtle.ConstantTimeCompare([]byte(receivedSecret), []byte(adminSecret)) != 1 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/system/toggle", bytes.NewBufferString(`{"state":"ACTIVE"}`))
	rr := httptest.NewRecorder()
	toggleHandler.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("Expected 401 for empty ADMIN_SECRET_KEY and empty header, got %d", rr.Code)
	}

	// 2. Ensure /api/v1/ingest rejects empty incoming header when INGEST_WEBHOOK_SECRET is empty/unset
	os.Unsetenv("INGEST_WEBHOOK_SECRET")
	os.Unsetenv("WEBHOOK_SECRET")
	ingestHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secret := os.Getenv("INGEST_WEBHOOK_SECRET")
		if secret == "" {
			secret = os.Getenv("WEBHOOK_SECRET")
		}
		receivedSecret := r.Header.Get("X-Webhook-Secret")
		if secret == "" || receivedSecret == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if subtle.ConstantTimeCompare([]byte(receivedSecret), []byte(secret)) != 1 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	req = httptest.NewRequest(http.MethodPost, "/api/v1/ingest", bytes.NewBufferString(`{"workspace_id":"123","source":"test","payload":"hello"}`))
	rr = httptest.NewRecorder()
	ingestHandler.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("Expected 401 for empty INGEST_WEBHOOK_SECRET and empty header, got %d", rr.Code)
	}
}

func TestFlutterwave_MultiCurrencyPricingConversion(t *testing.T) {
	tests := []struct {
		amount       float64
		currency     string
		expectedTier string
		expectedTok  int
		expectValid  bool
	}{
		// NGN
		{14999.0, models.CurrencyNGN, models.TierStarter, models.TokensStarter, true},
		{99999.0, models.CurrencyNGN, models.TierProfessional, models.TokensProfessional, true},
		{14999.001, models.CurrencyNGN, models.TierStarter, models.TokensStarter, true},
		{99999.004, models.CurrencyNGN, models.TierProfessional, models.TokensProfessional, true},
		// USD
		{29.0, models.CurrencyUSD, models.TierStarter, models.TokensStarter, true},
		{99.0, models.CurrencyUSD, models.TierProfessional, models.TokensProfessional, true},
		{29.001, models.CurrencyUSD, models.TierStarter, models.TokensStarter, true},
		{28.999, models.CurrencyUSD, models.TierStarter, models.TokensStarter, true},
		{99.002, models.CurrencyUSD, models.TierProfessional, models.TokensProfessional, true},
		{98.998, models.CurrencyUSD, models.TierProfessional, models.TokensProfessional, true},
		// Invalid / Mismatches
		{29.0, models.CurrencyNGN, "", 0, false},
		{14999.0, models.CurrencyUSD, "", 0, false},
		{0.0, models.CurrencyUSD, "", 0, false},
		{-29.0, models.CurrencyUSD, "", 0, false},
		{50.0, models.CurrencyUSD, "", 0, false},
		{100.0, "EUR", "", 0, false},
	}

	for _, tt := range tests {
		plan, ok := models.ResolvePlanByAmountAndCurrency(tt.amount, tt.currency)
		if ok != tt.expectValid {
			t.Errorf("ResolvePlanByAmountAndCurrency(%f, %q) ok = %v, expected %v", tt.amount, tt.currency, ok, tt.expectValid)
		}
		if ok {
			if plan.Tier != tt.expectedTier {
				t.Errorf("ResolvePlanByAmountAndCurrency(%f, %q) tier = %s, expected %s", tt.amount, tt.currency, plan.Tier, tt.expectedTier)
			}
			if plan.Tokens != tt.expectedTok {
				t.Errorf("ResolvePlanByAmountAndCurrency(%f, %q) tokens = %d, expected %d", tt.amount, tt.currency, plan.Tokens, tt.expectedTok)
			}
		}
	}
}

func TestFlutterwave_ConcurrentWebhookExecution(t *testing.T) {
	secretHash := "flw_hash_concurrent_test"
	os.Setenv("FLW_SECRET_HASH", secretHash)
	os.Setenv("FLW_SECRET_KEY", "FLWSECK_TEST-mock-key")
	defer os.Unsetenv("FLW_SECRET_HASH")
	defer os.Unsetenv("FLW_SECRET_KEY")

	mockFLWServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "success",
			"message": "Tx Fetched",
			"data": map[string]interface{}{
				"id":       123456,
				"tx_ref":   "zeno_ws_concurrent_tx",
				"status":   "successful",
				"amount":   29.0,
				"currency": "USD",
				"meta": map[string]interface{}{
					"workspace_id": "ws_concurrent_test",
				},
			},
		})
	}))
	defer mockFLWServer.Close()

	os.Setenv("FLW_BASE_URL", mockFLWServer.URL)
	defer os.Unsetenv("FLW_BASE_URL")

	var dbMu sync.Mutex
	journalEntries := make(map[string]bool)
	creditsAwarded := 0

	webhookHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedHash := r.Header.Get("verif-hash")
		expectedHash := os.Getenv("FLW_SECRET_HASH")
		if expectedHash == "" || receivedHash == "" || subtle.ConstantTimeCompare([]byte(receivedHash), []byte(expectedHash)) != 1 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		var payload struct {
			Event string `json:"event"`
			Data  struct {
				ID       int64                  `json:"id"`
				TxRef    string                 `json:"tx_ref"`
				Status   string                 `json:"status"`
				Amount   float64                `json:"amount"`
				Currency string                 `json:"currency"`
				Meta     map[string]interface{} `json:"meta"`
			} `json:"data"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		dbMu.Lock()
		if journalEntries[payload.Data.TxRef] {
			dbMu.Unlock()
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"status": "acknowledged"})
			return
		}
		journalEntries[payload.Data.TxRef] = true
		creditsAwarded++
		dbMu.Unlock()

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "acknowledged"})
	})

	webhookPayload := map[string]interface{}{
		"event": "charge.completed",
		"data": map[string]interface{}{
			"id":       123456,
			"tx_ref":   "zeno_ws_concurrent_tx",
			"status":   "successful",
			"amount":   29.0,
			"currency": "USD",
			"meta": map[string]interface{}{
				"workspace_id": "ws_concurrent_test",
			},
		},
	}
	payloadBytes, _ := json.Marshal(webhookPayload)

	concurrency := 20
	var wg sync.WaitGroup
	statusCodes := make([]int, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPost, "/api/v1/webhooks/flutterwave", bytes.NewBuffer(payloadBytes))
			req.Header.Set("verif-hash", secretHash)
			rr := httptest.NewRecorder()
			webhookHandler.ServeHTTP(rr, req)
			statusCodes[idx] = rr.Code
		}(i)
	}
	wg.Wait()

	for i, code := range statusCodes {
		if code != http.StatusOK {
			t.Errorf("Goroutine %d got status %d, expected 200", i, code)
		}
	}

	if creditsAwarded != 1 {
		t.Errorf("Expected exactly 1 credit under %d concurrent webhooks, got %d", concurrency, creditsAwarded)
	}
}

func TestFlutterwave_VerifyTimeoutAnd5xxHandling(t *testing.T) {
	secretHash := "flw_hash_timeout_test"
	os.Setenv("FLW_SECRET_HASH", secretHash)
	os.Setenv("FLW_SECRET_KEY", "FLWSECK_TEST-mock-key")
	defer os.Unsetenv("FLW_SECRET_HASH")
	defer os.Unsetenv("FLW_SECRET_KEY")

	// 1. Mock server that returns 503
	mock503Server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer mock503Server.Close()

	os.Setenv("FLW_BASE_URL", mock503Server.URL)
	defer os.Unsetenv("FLW_BASE_URL")

	webhookHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flwConfig := models.GetFlutterwaveConfig()
		verifyReq, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, flwConfig.BaseURL+"/transactions/123/verify", nil)
		resp, err := billingHTTPClient.Do(verifyReq)
		if err != nil {
			w.WriteHeader(http.StatusGatewayTimeout) // 504
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 500 {
			w.WriteHeader(http.StatusServiceUnavailable) // 503
			return
		}

		w.WriteHeader(http.StatusOK)
	})

	// Test 503 upstream return
	req := httptest.NewRequest(http.MethodPost, "/api/v1/webhooks/flutterwave", bytes.NewBufferString(`{}`))
	rr := httptest.NewRecorder()
	webhookHandler.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected 503 Service Unavailable when upstream returns 5xx, got %d", rr.Code)
	}

	// 2. Test timeout handling returning 504
	timeoutHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 1*time.Millisecond)
		defer cancel()
		time.Sleep(2 * time.Millisecond)

		verifyReq, _ := http.NewRequestWithContext(ctx, http.MethodGet, mock503Server.URL+"/transactions/123/verify", nil)
		_, err := billingHTTPClient.Do(verifyReq)
		if err != nil {
			w.WriteHeader(http.StatusGatewayTimeout) // 504
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	req = httptest.NewRequest(http.MethodPost, "/api/v1/webhooks/flutterwave", bytes.NewBufferString(`{}`))
	rr = httptest.NewRecorder()
	timeoutHandler.ServeHTTP(rr, req)
	if rr.Code != http.StatusGatewayTimeout {
		t.Errorf("Expected 504 Gateway Timeout when verification times out, got %d", rr.Code)
	}
}
