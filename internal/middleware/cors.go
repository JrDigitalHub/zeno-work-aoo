package middleware

import (
	"net/http"
	"os"
	"strings"
)

// CorsGuard protects your API from unauthorized browser requests
func CorsGuard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Fetch allowed domains from Render environment variables
		allowedEnv := os.Getenv("ALLOWED_ORIGINS")
		if allowedEnv == "" {
			// Fallback strictly for local testing if the env var isn't set
			allowedEnv = "http://localhost:3000" 
		}
		
		// Split by comma to support multiple domains (e.g., "https://app.com,https://admin.app.com")
		allowedOrigins := strings.Split(allowedEnv, ",")
		origin := r.Header.Get("Origin")

		// Check if the incoming request matches our allowed list
		for _, o := range allowedOrigins {
			if origin == strings.TrimSpace(o) {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				break
			}
		}

		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, Authorization, X-CSRF-Token")
		w.Header().Set("Access-Control-Allow-Credentials", "true")

		// Handle preflight browser checks instantly
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}