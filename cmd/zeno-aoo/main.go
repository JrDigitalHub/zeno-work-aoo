package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv" // Securely load your secrets

	"github.com/JrDigitalHub/zeno-work-aoo/internal/agent"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/backoffice"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/comms"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/memory"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/middleware"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/orchestrator"
	"github.com/JrDigitalHub/zeno-work-aoo/pkg/protocol"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverdatabasesql"
	"github.com/riverqueue/river/rivermigrate"
)

// 👉 Global Master Kill Switch State
var (
	systemStatusMutex sync.RWMutex
	isSystemActive    bool = true // Defaults to ONLINE at boot
)

func main() {
	fmt.Println("🧠 Zeno OS: Booting Unified Neural Infrastructure [Graph + Vector + Relational + Back-Office + Oracle]...")

	// 👉 SECURE VAULT INIT: Load environment variables from your local .env file
	if err := godotenv.Load(); err != nil {
		fmt.Println("⚠️  No .env file found, relying on system environment variables.")
	}

	// Configure global JSON logger for production readability
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	slog.Info("ZENO OS Boot Sequence Initiated",
		slog.String("module", "core_system"),
		slog.String("status", "booting"),
	)

	// 1. Ignite Neo4j (NOW CLOUD READY)
	neo4jURI := os.Getenv("NEO4J_URI")
	if neo4jURI == "" {
		neo4jURI = "bolt://localhost:7687"
	}
	neo4jUser := os.Getenv("NEO4J_USERNAME")
	if neo4jUser == "" {
		neo4jUser = "neo4j"
	}
	neo4jPass := os.Getenv("NEO4J_PASSWORD")
	if neo4jPass == "" {
		neo4jPass = "zeno_admin_password"
	}

	graphBrain, err := memory.NewSovereignStore(neo4jURI, neo4jUser, neo4jPass)
	if err != nil {
		fmt.Printf("⚠️ WARNING: Graph Memory offline. Bypassing for frontend development: %v\n", err)
	} else {
		defer graphBrain.Close()
		fmt.Println("🧠 [MEMORY] Neural Graph (Neo4j) connected successfully.")
	}

	// 2. Ignite Qdrant (NOW CLOUD READY)
	qdrantURL := os.Getenv("QDRANT_URL")
	if qdrantURL == "" {
		qdrantURL = "localhost:6334"
	}

	var vectorBrain *memory.VectorStore
	vectorBrain, err = memory.NewVectorStore(qdrantURL, "zeno_intel_vectors_v3")
	if err != nil {
		fmt.Printf("⚠️ WARNING: Vector Memory offline. Bypassing for frontend development: %v\n", err)
	} else {
		defer vectorBrain.Close()
		fmt.Println("📐 [VECTOR] Semantic Memory connected successfully.")
	}

	// 2.5 Ignite Relational Brain (Supabase / Postgres)
	var relationalBrain *memory.RelationalStore
	var dbConn *sql.DB // Safely extract the *sql.DB for the COO service
	var riverClient *river.Client[*sql.Tx]

	supabaseURL := os.Getenv("SUPABASE_URL")
	if supabaseURL == "" {
		fmt.Println("⚠️ WARNING: SUPABASE_URL not found in environment. State persistence is offline.")
	} else {
		store, err := memory.NewRelationalStore(supabaseURL)
		if err != nil {
			panic(fmt.Sprintf("❌ CRITICAL: Failed to boot Relational Memory: %v", err))
		}
		relationalBrain = store
		dbConn = store.DB
		defer relationalBrain.Close()

		// Run River schema migrations at startup
		slog.Info("Running River migrations...")
		driver := riverdatabasesql.New(dbConn)
		migrator, err := rivermigrate.New(driver, nil)
		if err != nil {
			panic(fmt.Sprintf("❌ CRITICAL: Failed to initialize River migrator: %v", err))
		}
		_, err = migrator.Migrate(context.Background(), rivermigrate.DirectionUp, nil)
		if err != nil {
			panic(fmt.Sprintf("❌ CRITICAL: Failed to run River migrations: %v", err))
		}
		slog.Info("River migrations completed successfully.")
	}

	// Properly pass the relationalBrain store so the COO can manage tasks securely with RLS
	opsManager := backoffice.NewPipelineManager(relationalBrain)

	// 4. Ignite Router
	router := orchestrator.NewEventRouter()
	router.Start()

	// 5. Initialize the ORACLE
	oracleAgent := agent.NewOracle(graphBrain, router)
	router.Subscribe(oracleAgent.React)

	// 6. Initialize Sentinel
	sentinelAgent := agent.NewSentinel(graphBrain, vectorBrain, opsManager, router)
	router.Subscribe(sentinelAgent.React)

	// 7. Initialize Predator & Voice
	predatorAgent := agent.NewPredator(router)
	router.Subscribe(predatorAgent.React)

	voiceEngine := comms.NewVoiceEngine("http://localhost:8000", "http://localhost:4321")
	router.Subscribe(voiceEngine.React)

	// 7.5. Initialize Autonomous Zoho Outbound Engine
	emailEngine := comms.NewEmailEngine(
		"smtp.zoho.com",
		"465",
		"system@jrdigitalhubltd.com",
		os.Getenv("ZOHO_SYSTEM_PASSWORD"),
		"JR Digital Hub | System",
		relationalBrain,
	)
	router.Subscribe(func(ctx context.Context, event protocol.Event) error {
		return emailEngine.React(ctx, event)
	})

	// 7.7. Initialize Real-Time WebSocket State Engine
	wsEngine := comms.NewWebSocketEngine()
	go wsEngine.Run()
	router.Subscribe(wsEngine.React)

	// 8. Initialize Discovery Agent
	discoveryAgent := agent.NewDiscoveryAgent(router, relationalBrain)

	// 8.5 Initialize Financial Modeler
	modelerAgent := agent.NewFinancialModeler(router, relationalBrain)
	router.Subscribe(modelerAgent.React)

	// --- BOOT RIVER BACKGROUND JOB CLIENT ---
	if dbConn != nil {
		slog.Info("Starting River background job engine...")
		driver := riverdatabasesql.New(dbConn)
		workers := river.NewWorkers()

		river.AddWorker(workers, &orchestrator.DiscoveryWorker{Router: router})
		river.AddWorker(workers, &orchestrator.PredatorWorker{Router: router})
		river.AddWorker(workers, &orchestrator.SentinelTextOutputWorker{Router: router})
		river.AddWorker(workers, &orchestrator.ModelerResultWorker{Router: router})
		river.AddWorker(workers, &agent.ProcessInvoiceWorker{Modeler: modelerAgent})
		river.AddWorker(workers, &orchestrator.UpgradeWorkspaceWorker{DB: relationalBrain})

		client, err := river.NewClient(driver, &river.Config{
			Queues: map[string]river.QueueConfig{
				river.QueueDefault: {MaxWorkers: 1},
				"modeler":          {MaxWorkers: 1},
			},
			Workers: workers,
		})
		if err != nil {
			panic(fmt.Sprintf("❌ CRITICAL: Failed to initialize River client: %v", err))
		}
		riverClient = client

		// Pass the client to the router
		router.SetRiverClient(riverClient)

		if err := riverClient.Start(context.Background()); err != nil {
			slog.Warn("⚠️ WARNING: Failed to start River client, bypassing for API testing", slog.Any("error", err))
		} else {
			slog.Info("River background job engine online.")
		}
	}

	// --- ENTERPRISE HTTP ROUTING --- //

	// 👉 Expose the WebSocket channel safely for Render
	http.Handle("/ws", wsEngine)

	// =========================================================================
	// 🛡️ NEW ENTERPRISE COO API ROUTES (PROTECTED BY AUTHENTICATION MIDDLEWARE)
	// =========================================================================

	// GET /api/v1/coo/tasks - Returns active kanban tasks to the UI
	http.HandleFunc("/api/v1/coo/tasks", middleware.EngineSecurityGuard(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		if r.Method == http.MethodGet {
			opsManager.HandleGetTasks(w, r)
		} else {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}))

	// PATCH /api/v1/coo/tasks/{id} - Allows the CEO to approve/reject tasks
	http.HandleFunc("/api/v1/coo/tasks/", middleware.EngineSecurityGuard(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "PATCH, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		if r.Method == http.MethodPatch {
			opsManager.HandleUpdateTask(w, r)
		} else {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}))

	// =========================================================================
	// 🛡️ NEW ENTERPRISE CFO API ROUTES (FINANCE & LEDGER)
	// =========================================================================

	// GET /api/v1/cfo/ledger - Returns the immutable double-entry history
	http.HandleFunc("/api/v1/cfo/ledger", middleware.EngineSecurityGuard(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		if r.Method == http.MethodGet {
			modelerAgent.HandleGetLedger(w, r)
		} else {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}))

	// POST /api/v1/cfo/invoices - Accepts documents for AI OCR categorization
	http.HandleFunc("/api/v1/cfo/invoices", middleware.EngineSecurityGuard(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		if r.Method == http.MethodPost {
			modelerAgent.HandleIngestInvoice(w, r)
		} else {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}))

	// GET /api/v1/cfo/jobs/{id} - Secure polling endpoint for background jobs
	http.HandleFunc("/api/v1/cfo/jobs/", middleware.EngineSecurityGuard(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		if r.Method == http.MethodGet {
			modelerAgent.HandleGetJobStatus(w, r)
		} else {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}))

	// =========================================================================
	// 🛡️ NEW TOKEN WALLET & CHAT ROUTER ROUTES
	// =========================================================================

	// GET /api/v1/wallet - Returns current token balance and subscription tier
	http.HandleFunc("/api/v1/wallet", middleware.EngineSecurityGuard(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		ctxWorkspace := r.Context().Value(middleware.WorkspaceContextKey)
		if ctxWorkspace == nil {
			ctxWorkspace = r.Context().Value("workspace_id")
		}
		if ctxWorkspace == nil {
			http.Error(w, `{"error": "Unauthorized. Missing workspace context."}`, http.StatusUnauthorized)
			return
		}
		workspaceID := fmt.Sprintf("%v", ctxWorkspace)

		var balance int
		var tier string
		var err error

		if relationalBrain != nil {
			balance, tier, err = relationalBrain.GetTokenWallet(r.Context(), workspaceID)
			if err != nil {
				http.Error(w, fmt.Sprintf(`{"error": "Failed to fetch wallet: %v"}`, err), http.StatusInternalServerError)
				return
			}
		} else {
			// Mock default values if DB is offline
			balance = 50000
			tier = "Trial"
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"token_balance":     balance,
			"subscription_tier": tier,
		})
	}))

	// POST /api/v1/chat - Routes agent prompt directly to Gemini, deducting tokens
	http.HandleFunc("/api/v1/chat", middleware.EngineSecurityGuard(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Parse request payload
		var req struct {
			WorkspaceID string `json:"workspace_id"`
			AgentType   string `json:"agent_type"`
			Message     string `json:"message"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error": "Invalid request payload"}`, http.StatusBadRequest)
			return
		}

		ctxWorkspace := r.Context().Value(middleware.WorkspaceContextKey)
		if ctxWorkspace == nil {
			ctxWorkspace = r.Context().Value("workspace_id")
		}
		workspaceID := ""
		if ctxWorkspace != nil {
			workspaceID = fmt.Sprintf("%v", ctxWorkspace)
		}
		if workspaceID == "" {
			workspaceID = req.WorkspaceID
		}
		if workspaceID == "" {
			http.Error(w, `{"error": "Workspace ID is required"}`, http.StatusBadRequest)
			return
		}

		// 1. Deduct tokens (150 tokens)
		var newBalance int
		if relationalBrain != nil {
			var err error
			newBalance, err = relationalBrain.DeductTokens(r.Context(), workspaceID, 150)
			if err != nil {
				if strings.Contains(err.Error(), "insufficient tokens") {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusTooManyRequests) // 429
					json.NewEncoder(w).Encode(map[string]string{"error": "Token limit reached"})
					return
				}
				http.Error(w, fmt.Sprintf(`{"error": "Database error: %v"}`, err), http.StatusInternalServerError)
				return
			}
		} else {
			// Mock default deduction if DB is offline
			newBalance = 49850
			slog.Info("DB offline, mock token deduction successful", slog.String("workspace_id", workspaceID))
		}

		// 2. Call Gemini API
		apiKey := os.Getenv("GEMINI_API_KEY")
		if apiKey == "" {
			http.Error(w, `{"error": "Gemini API key is not configured"}`, http.StatusInternalServerError)
			return
		}

		geminiURL := fmt.Sprintf("https://generativelanguage.googleapis.com/v1beta/models/gemini-3.6-flash:generateContent?key=%s", apiKey)

		// Set system instruction prompt based on agent type
		systemPrompt := "You are the Zeno OS Autonomous Operations Officer. Help the user."
		switch strings.ToUpper(req.AgentType) {
		case "COO":
			systemPrompt = "You are the Zeno OS Autonomous Operations Officer (COO). You handle operational tasks, kanban boards, process automation, and workflow orchestration. Keep responses professional, clear, and action-oriented."
		case "CFO":
			systemPrompt = "You are the Zeno OS Chief Financial Officer (CFO). You manage double-entry bookkeeping, cash flow projections, invoice audits, and unit economics. Keep responses precise, analytical, and structured."
		case "ORACLE":
			systemPrompt = "You are the Zeno OS Oracle. You analyze the Neo4j Knowledge Graph, evaluate strategic business opportunities, identify bottlenecks, and advise on market growth. Keep responses insightful, visionary, and qualitative."
		}

		type GeminiPart struct {
			Text string `json:"text"`
		}
		type GeminiContent struct {
			Parts []GeminiPart `json:"parts"`
		}
		type GeminiSystemInstr struct {
			Parts []GeminiPart `json:"parts"`
		}
		type GeminiRequest struct {
			Contents          []GeminiContent    `json:"contents"`
			SystemInstruction *GeminiSystemInstr `json:"systemInstruction,omitempty"`
		}

		geminiReq := GeminiRequest{
			Contents: []GeminiContent{
				{
					Parts: []GeminiPart{
						{Text: req.Message},
					},
				},
			},
			SystemInstruction: &GeminiSystemInstr{
				Parts: []GeminiPart{
					{Text: systemPrompt},
				},
			},
		}

		reqBytes, err := json.Marshal(geminiReq)
		if err != nil {
			http.Error(w, `{"error": "Failed to build Gemini request"}`, http.StatusInternalServerError)
			return
		}

		resp, err := http.Post(geminiURL, "application/json", bytes.NewBuffer(reqBytes))
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error": "Gemini API call failed: %v"}`, err), http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			var errResp map[string]interface{}
			json.NewDecoder(resp.Body).Decode(&errResp)
			slog.Error("Gemini API returned error", slog.Any("details", errResp))
			http.Error(w, fmt.Sprintf(`{"error": "Gemini API returned status %d"}`, resp.StatusCode), http.StatusInternalServerError)
			return
		}

		type GeminiResponse struct {
			Candidates []struct {
				Content struct {
					Parts []struct {
						Text string `json:"text"`
					} `json:"parts"`
				} `json:"content"`
			} `json:"candidates"`
		}

		var geminiResp GeminiResponse
		if err := json.NewDecoder(resp.Body).Decode(&geminiResp); err != nil {
			http.Error(w, `{"error": "Failed to decode Gemini response"}`, http.StatusInternalServerError)
			return
		}

		outputText := ""
		if len(geminiResp.Candidates) > 0 && len(geminiResp.Candidates[0].Content.Parts) > 0 {
			outputText = geminiResp.Candidates[0].Content.Parts[0].Text
		}

		// Return response
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"response":      outputText,
			"workspace_id":  workspaceID,
			"agent_type":    req.AgentType,
			"token_balance": newBalance,
		})
	}))

	// =========================================================================
	// LEGACY ROUTES
	// =========================================================================

	// 👉 API Master Kill Switch (Protects API Credits Globally)
	http.HandleFunc("/api/v1/system/toggle", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		var req struct {
			State string `json:"state"` // "ACTIVE" or "STANDBY"
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid payload", http.StatusBadRequest)
			return
		}

		systemStatusMutex.Lock()
		if req.State == "STANDBY" {
			isSystemActive = false
			fmt.Println("🛑 [SYSTEM] Master Kill Switch Engaged. ZENO OS is now in STANDBY.")
		} else {
			isSystemActive = true
			fmt.Println("🟢 [SYSTEM] Systems Online. ZENO OS is now ACTIVE.")
		}
		systemStatusMutex.Unlock()

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "Acknowledged", "current_state": req.State})
	})

	// 👉 Client Workspace Campaign Toggle (Pause/Resume Client-Specific Pipeline)
	http.HandleFunc("/api/v1/workspace/toggle", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		var req struct {
			WorkspaceID string `json:"workspace_id"`
			IsPaused    bool   `json:"is_paused"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.WorkspaceID == "" {
			http.Error(w, "Invalid payload. 'workspace_id' and 'is_paused' status required.", http.StatusBadRequest)
			return
		}

		if relationalBrain == nil {
			http.Error(w, "Relational Store storage subsystem offline.", http.StatusServiceUnavailable)
			return
		}

		// Update the specific workspace status dynamically inside Supabase Postgres
		_, err := relationalBrain.DB.Exec("UPDATE workspaces SET is_paused = $1 WHERE id = $2", req.IsPaused, req.WorkspaceID)
		if err != nil {
			fmt.Printf("❌ [SYSTEM] Database workspace toggle crash: %v\n", err)
			http.Error(w, "Database runtime transaction failed.", http.StatusInternalServerError)
			return
		}

		stateMsg := "ACTIVE"
		if req.IsPaused {
			stateMsg = "PAUSED"
		}
		fmt.Printf("⏸️  [SYSTEM] Workspace [%s] campaign status updated to: %s\n", req.WorkspaceID, stateMsg)

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "Acknowledged", "workspace_state": stateMsg, "workspace_id": req.WorkspaceID})
	})

	// 👉 The CEO Directive API Endpoint (Multi-Tenant + Protected)
	http.HandleFunc("/api/directive", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		// 1. Check Kill Switch
		systemStatusMutex.RLock()
		active := isSystemActive
		systemStatusMutex.RUnlock()
		if !active {
			http.Error(w, `{"error": "ZENO is in STANDBY mode. Toggle system to ACTIVE to proceed."}`, http.StatusServiceUnavailable)
			return
		}

		// 2. Extract payload (Now includes 'mode' for multi-vector hunting)
		var req struct {
			WorkspaceID string `json:"workspace_id"`
			Target      string `json:"target"`
			Mode        string `json:"mode"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Target == "" || req.WorkspaceID == "" {
			http.Error(w, "Invalid payload. 'workspace_id' and 'target' required.", http.StatusBadRequest)
			return
		}

		// Set a default mode if the UI doesn't provide one yet
		if req.Mode == "" {
			req.Mode = "LOCAL_SCANNER"
		}

		fmt.Printf("\n⚡ [API] Directive Received for Workspace [%s]: '%s' via %s. Rerouting...\n", req.WorkspaceID, req.Target, req.Mode)

		// 👉 Send the request securely to the multi-vector Discovery Engine
		go discoveryAgent.ExtractLeads(context.Background(), req.WorkspaceID, req.Target, req.Mode)

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "Directive Engaged", "workspace_id": req.WorkspaceID, "mode": req.Mode})
	})

	// 👉 Back-Office Ingestion Webhook (The Invisible COO's Ear)
	http.HandleFunc("/api/v1/ingest", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		var req struct {
			WorkspaceID string `json:"workspace_id"`
			Source      string `json:"source"`
			Payload     string `json:"payload"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.WorkspaceID == "" || req.Payload == "" {
			http.Error(w, "Invalid payload. 'workspace_id', 'source', and 'payload' required.", http.StatusBadRequest)
			return
		}

		opsManager.Ingest(r.Context(), req.WorkspaceID, req.Source, req.Payload)

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ZENO_ACKNOWLEDGED", "workspace_id": req.WorkspaceID})
	})

	// 👉 Supabase Auth Webhook (Unauthenticated, protected by X-Webhook-Secret)
	http.HandleFunc("/api/v1/webhooks/supabase-auth", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Webhook-Secret")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		if r.Method != http.MethodPost {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusMethodNotAllowed)
			json.NewEncoder(w).Encode(map[string]string{"error": "Method not allowed"})
			return
		}

		// Security Check
		secret := os.Getenv("SUPABASE_WEBHOOK_SECRET")
		receivedHeader := r.Header.Get("X-Webhook-Secret")
		if secret == "" || receivedHeader != secret {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(map[string]string{"error": "Unauthorized. Missing or invalid secret."})
			return
		}

		// Struct to decode the incoming payload dynamically
		type SupabaseAuthRecord struct {
			ID    string `json:"id"`
			Email string `json:"email"`
		}

		type SupabaseAuthWebhookPayload struct {
			Type   string              `json:"type"`
			Table  string              `json:"table"`
			Schema string              `json:"schema"`
			Record *SupabaseAuthRecord `json:"record"`
			User   *SupabaseAuthRecord `json:"user"`
			ID     string              `json:"id"`
			Email  string              `json:"email"`
		}

		var payload SupabaseAuthWebhookPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "Invalid request payload"})
			return
		}

		var workspaceID string
		var email string

		if payload.Record != nil {
			workspaceID = payload.Record.ID
			email = payload.Record.Email
		} else if payload.User != nil {
			workspaceID = payload.User.ID
			email = payload.User.Email
		} else {
			workspaceID = payload.ID
			email = payload.Email
		}

		workspaceID = strings.TrimSpace(workspaceID)
		email = strings.TrimSpace(email)

		if workspaceID == "" || email == "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "Invalid payload: missing id or email"})
			return
		}

		if relationalBrain != nil {
			if err := relationalBrain.ProvisionNewWorkspace(r.Context(), workspaceID, email); err != nil {
				slog.Error("Failed to provision workspace", slog.String("workspace_id", workspaceID), slog.Any("error", err))
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("Provisioning failed: %v", err)})
				return
			}
		} else {
			slog.Warn("Relational DB is offline, skipping actual provisioning", slog.String("workspace_id", workspaceID))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"error": "Database subsystem offline"})
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"status":  "success",
			"message": "Wallet provisioned successfully",
		})
	})

	// 👉 Paystack Webhook (Unauthenticated, protected by x-paystack-signature header verification)
	http.HandleFunc("/api/v1/webhooks/paystack", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, x-paystack-signature")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		if r.Method != http.MethodPost {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusMethodNotAllowed)
			json.NewEncoder(w).Encode(map[string]string{"error": "Method not allowed"})
			return
		}

		// Security Check (CRITICAL)
		secret := os.Getenv("PAYSTACK_SECRET_KEY")
		receivedSignature := r.Header.Get("x-paystack-signature")

		// Read the raw request body
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "Failed to read request body"})
			return
		}
		// Restore body for any subsequent parsing if needed
		r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

		// Compute HMAC SHA512 of raw bodyBytes
		h := hmac.New(sha512.New, []byte(secret))
		h.Write(bodyBytes)
		expectedSignature := hex.EncodeToString(h.Sum(nil))

		// Validate signature (compare received signature against expectedSignature)
		if secret == "" || receivedSignature == "" || !hmac.Equal([]byte(receivedSignature), []byte(expectedSignature)) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(map[string]string{"error": "Unauthorized. Invalid signature."})
			return
		}

		// Paystack event payload struct definitions
		type PaystackMetadata struct {
			WorkspaceID string `json:"workspace_id"`
		}
		type PaystackData struct {
			Amount    int              `json:"amount"`
			Reference string           `json:"reference"`
			Metadata  PaystackMetadata `json:"metadata"`
		}
		type PaystackWebhookEvent struct {
			Event string       `json:"event"`
			Data  PaystackData `json:"data"`
		}

		var event PaystackWebhookEvent
		if err := json.Unmarshal(bodyBytes, &event); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "Invalid request payload"})
			return
		}

		// Listen specifically for the charge.success event
		if event.Event != "charge.success" {
			// Acknowledge other events with 200 OK without upgrading
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"status": "ignored", "message": "Event is not charge.success"})
			return
		}

		workspaceID := strings.TrimSpace(event.Data.Metadata.WorkspaceID)
		amount := event.Data.Amount
		reference := event.Data.Reference

		if workspaceID == "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "Invalid payload: missing workspace_id in metadata"})
			return
		}

		// Determine new tier and tokens to add based on the amount paid (in kobo)
		var newTier string
		var tokensToAdd int

		switch amount {
		case 1499900: // Starter Plan: ₦14,999 -> 500,000 tokens
			newTier = "Starter"
			tokensToAdd = 500000
		case 9999900: // Pro Plan: ₦99,999 -> 2,000,000 tokens
			newTier = "Professional"
			tokensToAdd = 2000000
		default:
			slog.Warn("Received charge.success with unexpected amount", slog.Int("amount", amount), slog.String("workspace_id", workspaceID))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("Unexpected payment amount: %d", amount)})
			return
		}

		if riverClient != nil {
			_, err = riverClient.Insert(r.Context(), orchestrator.UpgradeWorkspaceJobArgs{
				WorkspaceID: workspaceID,
				NewTier:     newTier,
				TokensToAdd: tokensToAdd,
				ReferenceID: reference,
			}, nil)
			if err != nil {
				slog.Error("Failed to enqueue UpgradeWorkspaceJob in River via Paystack", slog.String("workspace_id", workspaceID), slog.Any("error", err))
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]string{"error": "Failed to queue upgrade"})
				return
			}
		} else if relationalBrain != nil {
			slog.Warn("River client is offline, falling back to synchronous execution")
			ctx := context.WithValue(r.Context(), memory.WorkspaceIDKey, workspaceID)
			if err := relationalBrain.UpgradeWorkspaceTier(ctx, workspaceID, newTier, tokensToAdd, reference); err != nil {
				slog.Error("Failed to upgrade workspace tier synchronously", slog.String("workspace_id", workspaceID), slog.String("tier", newTier), slog.Any("error", err))
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("Upgrade failed: %v", err)})
				return
			}
		} else {
			slog.Warn("Relational DB is offline, skipping actual upgrade", slog.String("workspace_id", workspaceID))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"error": "Database subsystem offline"})
			return
		}

		slog.Info("Successfully upgraded workspace tier via Paystack", slog.String("workspace_id", workspaceID), slog.String("tier", newTier), slog.Int("tokens_added", tokensToAdd))

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"status":  "success",
			"message": "Subscription upgraded successfully",
		})
	})

	// 👉 Polar Webhook (Unauthenticated, protected by Standard Webhooks / Svix signature verification)
	http.HandleFunc("/api/v1/webhooks/polar", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, webhook-id, webhook-timestamp, webhook-signature, svix-id, svix-timestamp, svix-signature")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		if r.Method != http.MethodPost {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusMethodNotAllowed)
			json.NewEncoder(w).Encode(map[string]string{"error": "Method not allowed"})
			return
		}

		// Security Check (CRITICAL)
		webhookID := r.Header.Get("webhook-id")
		if webhookID == "" {
			webhookID = r.Header.Get("svix-id")
		}
		webhookTimestamp := r.Header.Get("webhook-timestamp")
		if webhookTimestamp == "" {
			webhookTimestamp = r.Header.Get("svix-timestamp")
		}
		webhookSignatureHeader := r.Header.Get("webhook-signature")
		if webhookSignatureHeader == "" {
			webhookSignatureHeader = r.Header.Get("svix-signature")
		}

		if webhookID == "" || webhookTimestamp == "" || webhookSignatureHeader == "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(map[string]string{"error": "Unauthorized. Missing signature headers."})
			return
		}

		// Replay Attack Protection (5 minutes skew window)
		timestampInt, err := strconv.ParseInt(webhookTimestamp, 10, 64)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "Invalid webhook timestamp"})
			return
		}
		now := time.Now().Unix()
		diff := now - timestampInt
		if diff < -300 || diff > 300 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(map[string]string{"error": "Unauthorized. Request expired or timestamp skew too large."})
			return
		}

		// Read the raw request body
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "Failed to read request body"})
			return
		}
		// Restore body for any subsequent parsing
		r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

		// Decode the webhook secret
		secret := os.Getenv("POLAR_WEBHOOK_SECRET")
		secret = strings.TrimPrefix(secret, "whsec_")
		key, err := base64.StdEncoding.DecodeString(secret)
		if err != nil {
			key = []byte(secret)
		}

		// Compute HMAC SHA256 of: id.timestamp.body
		signedContent := fmt.Sprintf("%s.%s.%s", webhookID, webhookTimestamp, string(bodyBytes))
		h := hmac.New(sha256.New, key)
		h.Write([]byte(signedContent))
		calculatedSignature := base64.StdEncoding.EncodeToString(h.Sum(nil))

		// Compare computed signature against signature header list (delimited by spaces)
		parts := strings.Split(webhookSignatureHeader, " ")
		var matched bool
		for _, part := range parts {
			subParts := strings.Split(part, ",")
			if len(subParts) == 2 && subParts[0] == "v1" {
				receivedSig := subParts[1]
				if hmac.Equal([]byte(receivedSig), []byte(calculatedSignature)) {
					matched = true
					break
				}
			}
		}

		if !matched {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(map[string]string{"error": "Unauthorized. Invalid signature."})
			return
		}

		// Struct to decode the incoming payload dynamically
		type PolarMetadata struct {
			WorkspaceID string `json:"workspace_id"`
		}
		type PolarCustomFieldData struct {
			WorkspaceID string `json:"workspace_id"`
		}
		type PolarData struct {
			Amount          int                  `json:"amount"`
			PriceAmount     int                  `json:"price_amount"`
			Metadata        PolarMetadata        `json:"metadata"`
			CustomFieldData PolarCustomFieldData `json:"custom_field_data"`
		}
		type PolarWebhookEvent struct {
			Type string    `json:"type"`
			Data PolarData `json:"data"`
		}

		var event PolarWebhookEvent
		if err := json.Unmarshal(bodyBytes, &event); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "Invalid request payload"})
			return
		}

		// Listen specifically for subscription.created or subscription.updated
		if event.Type != "subscription.created" && event.Type != "subscription.updated" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"status": "ignored", "message": "Event is not handled"})
			return
		}

		// Extract workspace_id (prioritize metadata, fallback to custom_field_data)
		workspaceID := strings.TrimSpace(event.Data.Metadata.WorkspaceID)
		if workspaceID == "" {
			workspaceID = strings.TrimSpace(event.Data.CustomFieldData.WorkspaceID)
		}

		if workspaceID == "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "Invalid payload: missing workspace_id"})
			return
		}

		// Extract amount (cents)
		amount := event.Data.Amount
		if amount == 0 {
			amount = event.Data.PriceAmount
		}

		var newTier string
		var tokensToAdd int

		switch amount {
		case 1900: // Starter Plan: $19 -> 500,000 tokens
			newTier = "Starter"
			tokensToAdd = 500000
		case 9900: // Professional Plan: $99 -> 2,000,000 tokens
			newTier = "Professional"
			tokensToAdd = 2000000
		default:
			slog.Warn("Received Polar subscription event with unexpected amount", slog.Int("amount", amount), slog.String("workspace_id", workspaceID))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("Unexpected payment amount: %d", amount)})
			return
		}

		if riverClient != nil {
			_, err = riverClient.Insert(r.Context(), orchestrator.UpgradeWorkspaceJobArgs{
				WorkspaceID: workspaceID,
				NewTier:     newTier,
				TokensToAdd: tokensToAdd,
				ReferenceID: webhookID,
			}, nil)
			if err != nil {
				slog.Error("Failed to enqueue UpgradeWorkspaceJob in River via Polar", slog.String("workspace_id", workspaceID), slog.Any("error", err))
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]string{"error": "Failed to queue upgrade"})
				return
			}
		} else if relationalBrain != nil {
			slog.Warn("River client is offline, falling back to synchronous execution")
			ctx := context.WithValue(r.Context(), memory.WorkspaceIDKey, workspaceID)
			if err := relationalBrain.UpgradeWorkspaceTier(ctx, workspaceID, newTier, tokensToAdd, webhookID); err != nil {
				slog.Error("Failed to upgrade workspace tier synchronously via Polar", slog.String("workspace_id", workspaceID), slog.String("tier", newTier), slog.Any("error", err))
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("Upgrade failed: %v", err)})
				return
			}
		} else {
			slog.Warn("Relational DB is offline, skipping actual upgrade", slog.String("workspace_id", workspaceID))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"error": "Database subsystem offline"})
			return
		}

		slog.Info("Successfully upgraded workspace tier via Polar", slog.String("workspace_id", workspaceID), slog.String("tier", newTier), slog.Int("tokens_added", tokensToAdd))

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"status":  "success",
			"message": "Subscription processed successfully",
		})
	})

	// =========================================================================
	// 🛡️ THE PRODUCTION SERVER ENGINE & GRACEFUL SHUTDOWN
	// =========================================================================

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// 1. Wrap the entire default router in the CORS middleware
	corsProtectedRouter := middleware.CorsGuard(http.DefaultServeMux)

	// 2. Instantiate the HTTP Server object properly for production
	srv := &http.Server{
		Addr:    "0.0.0.0:" + port,
		Handler: corsProtectedRouter,
	}

	// 3. Boot the server in a separate goroutine
	go func() {
		slog.Info("🌐 [HTTP] Streaming state socket & API listening", slog.String("port", port))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("CRITICAL: Server runtime crash", slog.Any("error", err))
		}
	}()

	slog.Info("🛡️  [SYSTEM] ZENO Backend Online. Waiting for client directives...")

	// 4. Set up the Render/Fly.io Kill Signal Catcher
	quit := make(chan os.Signal, 1)
	// We also need to make sure we import "os/signal" and "syscall" at the top of main.go for this to work
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	// 5. Block the main thread here until a shutdown signal is received
	<-quit
	slog.Info("🛑 [SYSTEM] SIGTERM received. Initiating graceful shutdown sequence...")

	// 6. Give the CFO, background jobs, and database 10 seconds to finish writing transactions
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if riverClient != nil {
		slog.Info("🛑 Stopping River background job client...")
		if err := riverClient.Stop(ctx); err != nil {
			slog.Error("River client forced to stop", slog.Any("error", err))
		} else {
			slog.Info("River client stopped successfully.")
		}
	}

	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("Server forced to shutdown", slog.Any("error", err))
	}

	// 7. Safely close the database connection pool
	if relationalBrain != nil {
		relationalBrain.Close()
	}

	slog.Info("💤 [SYSTEM] Zeno OS offline. Safe to restart.")
}
