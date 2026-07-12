package middleware

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/time/rate"
)

type contextKey string

const WorkspaceContextKey contextKey = "workspace_id"

// =========================================================================
// 1. IP-BASED RATE LIMITER (Multi-Tenant Safe)
// =========================================================================

// visitors tracks rate limiters by IP address
var (
	visitors = make(map[string]*rate.Limiter)
	mu       sync.Mutex
)

// getVisitor retrieves or creates a rate limiter for a specific IP
func getVisitor(ip string) *rate.Limiter {
	mu.Lock()
	defer mu.Unlock()

	limiter, exists := visitors[ip]
	if !exists {
		// 10 requests per second, burst of 20. Adjust as needed for SMEs.
		limiter = rate.NewLimiter(10, 20)
		visitors[ip] = limiter
	}
	return limiter
}

// =========================================================================
// 1.5. JWKS CACHING & FETCHING SUITE (Asymmetric ES256 Support)
// =========================================================================

type JWK struct {
	Kty string `json:"kty"`
	Alg string `json:"alg"`
	Crv string `json:"crv"`
	Kid string `json:"kid"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

type JWKS struct {
	Keys []JWK `json:"keys"`
}

var (
	jwkCache   = make(map[string]*ecdsa.PublicKey)
	jwkCacheMu sync.RWMutex
	lastFetch  time.Time
)

func getPublicKeyFromJWKS(kid string, iss string) (*ecdsa.PublicKey, error) {
	jwkCacheMu.RLock()
	pubKey, exists := jwkCache[kid]
	jwkCacheMu.RUnlock()

	if exists {
		return pubKey, nil
	}

	jwkCacheMu.Lock()
	defer jwkCacheMu.Unlock()

	// Double check inside lock
	if pubKey, exists = jwkCache[kid]; exists {
		return pubKey, nil
	}

	// Rate limit fetching to once per minute to avoid DDoS / rate throttling
	if time.Since(lastFetch) < 1*time.Minute && len(jwkCache) > 0 {
		return nil, fmt.Errorf("public key not found in cache and JWKS fetch rate-limited")
	}

	jwksURL := fmt.Sprintf("%s/.well-known/jwks.json", strings.TrimSuffix(iss, "/"))
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(jwksURL)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch JWKS from %s: %v", jwksURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("JWKS request to %s returned status %d", jwksURL, resp.StatusCode)
	}

	var jwks JWKS
	if err := json.NewDecoder(resp.Body).Decode(&jwks); err != nil {
		return nil, fmt.Errorf("failed to parse JWKS payload: %v", err)
	}

	lastFetch = time.Now()

	// Rebuild and update public key cache
	for _, key := range jwks.Keys {
		if key.Kty == "EC" && key.Crv == "P-256" && key.X != "" && key.Y != "" {
			xBytes, err := base64.RawURLEncoding.DecodeString(key.X)
			if err != nil {
				continue
			}
			yBytes, err := base64.RawURLEncoding.DecodeString(key.Y)
			if err != nil {
				continue
			}

			pk := &ecdsa.PublicKey{
				Curve: elliptic.P256(),
				X:     new(big.Int).SetBytes(xBytes),
				Y:     new(big.Int).SetBytes(yBytes),
			}
			jwkCache[key.Kid] = pk
		}
	}

	pubKey, exists = jwkCache[kid]
	if !exists {
		return nil, fmt.Errorf("public key with kid %s not found in JWKS", kid)
	}

	return pubKey, nil
}

// =========================================================================
// 2. THE SECURITY GUARD (JWT & Traffic Enforcement)
// =========================================================================

func EngineSecurityGuard(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// --- A. Extract Client IP ---
		ip, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			ip = r.RemoteAddr
		}
		// Trusting X-Forwarded-For if deployed behind Render/Fly.io proxies
		if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
			ip = strings.Split(forwarded, ",")[0]
		}

		// --- B. Rate Limiter Assessment ---
		limiter := getVisitor(ip)
		if !limiter.Allow() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte(`{"error": {"code": "RATE_LIMIT_EXCEEDED", "message": "Too many requests. Resources are throttled."}}`))
			log.Printf("⚠️ [SECURITY] Rate limit exceeded for IP: %s", ip)
			return
		}

		// --- C. Extract Authorization Header ---
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error": {"code": "UNAUTHORIZED", "message": "Missing or malformed security token."}}`))
			return
		}

		tokenString := strings.TrimPrefix(authHeader, "Bearer ")

		// --- D. Supabase JWT Cryptographic Verification (HMAC + ECDSA Fallback) ---
		token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
			alg, _ := token.Header["alg"].(string)

			// 1. Symmetric Fallback (HS256)
			if strings.HasPrefix(alg, "HS") {
				if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
					return nil, fmt.Errorf("unexpected HMAC signing method: %v", token.Header["alg"])
				}
				jwtSecret := os.Getenv("SUPABASE_JWT_SECRET")
				if jwtSecret == "" {
					return nil, fmt.Errorf("SUPABASE_JWT_SECRET is missing from .env for HMAC token verification")
				}
				return []byte(jwtSecret), nil
			}

			// 2. Asymmetric Primary (ES256)
			if strings.HasPrefix(alg, "ES") {
				if _, ok := token.Method.(*jwt.SigningMethodECDSA); !ok {
					return nil, fmt.Errorf("unexpected ECDSA signing method: %v", token.Header["alg"])
				}

				kid, ok := token.Header["kid"].(string)
				if !ok || kid == "" {
					return nil, fmt.Errorf("missing kid in token header")
				}

				// Parse claims unverified first to dynamically resolve issuer URL
				var claims jwt.MapClaims
				parser := jwt.NewParser()
				_, _, err := parser.ParseUnverified(tokenString, &claims)
				if err != nil {
					return nil, fmt.Errorf("failed to parse unverified claims: %v", err)
				}

				iss, ok := claims["iss"].(string)
				if !ok || iss == "" {
					return nil, fmt.Errorf("missing issuer (iss) in claims")
				}

				return getPublicKeyFromJWKS(kid, iss)
			}

			return nil, fmt.Errorf("unsupported signing method: %v", alg)
		})

		if err != nil || !token.Valid {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error": {"code": "INVALID_TOKEN", "message": "Access signature could not be verified or has expired."}}`))
			log.Printf("⚠️ [SECURITY] Failed JWT verification attempt from IP: %s. Error: %v", ip, err)
			return
		}

		// --- E. Extract Workspace Context ---
		claims, ok := token.Claims.(jwt.MapClaims)
		if !ok {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error": {"code": "INVALID_CLAIMS", "message": "Token structure is invalid."}}`))
			return
		}

		// Supabase stores the user's UUID in the "sub" (subject) claim.
		// For an SME OS, the User ID effectively acts as their isolated Workspace ID.
		userID, ok := claims["sub"].(string)
		if !ok || userID == "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error": {"code": "MISSING_IDENTITY", "message": "Identity could not be extracted from token."}}`))
			return
		}

		// Inject verified User/Workspace ID down into the request execution pipeline
		ctx := context.WithValue(r.Context(), WorkspaceContextKey, userID)
		ctx = context.WithValue(ctx, "workspace_id", userID)
		next(w, r.WithContext(ctx))
	}
}