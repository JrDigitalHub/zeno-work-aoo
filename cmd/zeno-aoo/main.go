package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"

	"github.com/joho/godotenv" // Securely load your secrets

	"github.com/JrDigitalHub/zeno-work-aoo/internal/agent"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/backoffice"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/comms"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/memory"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/orchestrator"
	"github.com/JrDigitalHub/zeno-work-aoo/pkg/protocol"
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
		panic(fmt.Sprintf("❌ CRITICAL: Failed to boot Graph Memory: %v", err))
	}
	defer graphBrain.Close()
	fmt.Println("🧠 [MEMORY] Neural Graph (Neo4j) connected successfully.")

	// 2. Ignite Qdrant (NOW CLOUD READY)
	qdrantURL := os.Getenv("QDRANT_URL")
	if qdrantURL == "" {
		qdrantURL = "localhost:6334"
	}

	vectorBrain, err := memory.NewVectorStore(qdrantURL, "zeno_intel_vectors_v3")
	if err != nil {
		panic(fmt.Sprintf("❌ CRITICAL: Failed to boot Vector Memory: %v", err))
	}
	defer vectorBrain.Close()
	fmt.Println("📐 [VECTOR] Semantic Memory connected successfully.")

	// 2.5 Ignite Relational Brain (Supabase / Postgres)
	var relationalBrain *memory.RelationalStore

	supabaseURL := os.Getenv("SUPABASE_URL")
	if supabaseURL == "" {
		fmt.Println("⚠️ WARNING: SUPABASE_URL not found in environment. State persistence is offline.")
	} else {
		store, err := memory.NewRelationalStore(supabaseURL)
		if err != nil {
			panic(fmt.Sprintf("❌ CRITICAL: Failed to boot Relational Memory: %v", err))
		}
		relationalBrain = store
		defer relationalBrain.Close()
	}

	// 3. Initialize Back-Office (Enterprise Multi-Tenant Mode)
	opsManager := backoffice.NewManager(10, 3)

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
	router.Subscribe(func(event protocol.Event) {
		emailEngine.React(event)
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

	// --- ENTERPRISE HTTP ROUTING --- //

	// 👉 Expose the WebSocket channel safely for Render
	http.Handle("/ws", wsEngine)

	// 👉 API Master Kill Switch (Protects API Credits)
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

		// 2. Extract payload
		var req struct {
			WorkspaceID string `json:"workspace_id"`
			Target      string `json:"target"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Target == "" || req.WorkspaceID == "" {
			http.Error(w, "Invalid payload. 'workspace_id' and 'target' required.", http.StatusBadRequest)
			return
		}

		fmt.Printf("\n⚡ [API] Directive Received for Workspace [%s]: '%s'. Rerouting...\n", req.WorkspaceID, req.Target)

		// 👉 FIXED: Now correctly passing BOTH WorkspaceID and Target
		go discoveryAgent.ExtractLeads(req.WorkspaceID, req.Target)

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "Directive Engaged", "workspace_id": req.WorkspaceID})
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

		opsManager.Ingest(req.WorkspaceID, req.Source, req.Payload)

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ZENO_ACKNOWLEDGED", "workspace_id": req.WorkspaceID})
	})

	// Boot the HTTP/Websocket Server
	go func() {
		port := os.Getenv("PORT")
		if port == "" {
			port = "8080"
		}
		fmt.Printf("🌐 [HTTP] Streaming state socket & API listening on port: %s\n", port)

		if err := http.ListenAndServe("0.0.0.0:"+port, nil); err != nil {
			fmt.Printf("❌ CRITICAL: Server tracking socket runtime crash: %v\n", err)
		}
	}()

	fmt.Println("\n🛡️  [SYSTEM] ZENO Backend Online. Waiting for client directives...")

	// Server runs 24/7 now.
	select {}
}
