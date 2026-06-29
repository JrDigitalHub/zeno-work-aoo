package middleware

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"

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

		// --- D. Supabase JWT Cryptographic Verification ---
		jwtSecret := os.Getenv("SUPABASE_JWT_SECRET")
		if jwtSecret == "" {
			log.Println("❌ CRITICAL: SUPABASE_JWT_SECRET is missing from .env")
			http.Error(w, `{"error": "Internal server configuration error"}`, http.StatusInternalServerError)
			return
		}

		// Parse and validate the JWT signature
		token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
			// Ensure the token uses the correct signing method (HMAC)
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
			}
			return []byte(jwtSecret), nil
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
		next(w, r.WithContext(ctx))
	}
}