package middleware

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestEngineSecurityGuard_ValidES256Token(t *testing.T) {
	// 1. Generate a P-256 Elliptic Curve key pair locally
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	// 2. Convert public key parameters to JWK coordinates
	xBytes := privateKey.PublicKey.X.Bytes()
	yBytes := privateKey.PublicKey.Y.Bytes()
	xBase64 := base64.RawURLEncoding.EncodeToString(xBytes)
	yBase64 := base64.RawURLEncoding.EncodeToString(yBytes)

	jwk := JWK{
		Kty: "EC",
		Alg: "ES256",
		Crv: "P-256",
		Kid: "test-key-id",
		X:   xBase64,
		Y:   yBase64,
	}

	jwks := JWKS{
		Keys: []JWK{jwk},
	}

	// 3. Spin up a local mock JWKS HTTP server serving our JWK
	mockJWKSServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/.well-known/jwks.json" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(jwks)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer mockJWKSServer.Close()

	// 4. Issue and sign a test token using the generated private key
	token := jwt.NewWithClaims(jwt.SigningMethodES256, jwt.MapClaims{
		"iss":  mockJWKSServer.URL, // Point issuer claim to our local mock server
		"sub":  "2cc257d7-a5b0-4790-a781-9071f961b4e3",
		"aud":  "authenticated",
		"exp":  time.Now().Add(1 * time.Hour).Unix(),
		"role": "authenticated",
	})
	token.Header["kid"] = "test-key-id"

	signedToken, err := token.SignedString(privateKey)
	if err != nil {
		t.Fatal(err)
	}

	// 5. Build and execute a request through the EngineSecurityGuard middleware
	req, err := http.NewRequest("GET", "/api/v1/cfo/invoices", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+signedToken)

	rr := httptest.NewRecorder()

	handlerCalled := false
	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
		w.WriteHeader(http.StatusAccepted)
	})

	guard := EngineSecurityGuard(nextHandler)
	guard.ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Errorf("expected status 202 Accepted, got %d. Body: %s", rr.Code, rr.Body.String())
	}

	if !handlerCalled {
		t.Errorf("expected next handler to be called")
	}
}
