package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv" // Added godotenv to securely load your secrets

	"github.com/JrDigitalHub/zeno-work-aoo/internal/agent"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/backoffice"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/comms"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/memory"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/orchestrator"
	"github.com/JrDigitalHub/zeno-work-aoo/pkg/protocol"
)

func main() {
	// Look for this specific header to confirm the Oracle is injected!
	fmt.Println("🧠 Zeno OS: Booting Unified Neural Infrastructure [Graph + Vector + Relational + Back-Office + Oracle]...")

	// 👉 SECURE VAULT INIT: Load environment variables from your local .env file
	if err := godotenv.Load(); err != nil {
		fmt.Println("⚠️  No .env file found, relying on system environment variables.")
	}

	searchQuery := "Hardware automation startups"
	if len(os.Args) > 1 {
		searchQuery = strings.Join(os.Args[1:], " ")
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

	// 3. Initialize Back-Office
	opsManager := backoffice.NewManager(3)

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
		os.Getenv("ZOHO_SYSTEM_PASSWORD"), // Now safely grabbing from .env
		"JR Digital Hub | System",
		relationalBrain, // 👉 INJECT THE SUPABASE CONNECTION HERE
	)
	router.Subscribe(func(event protocol.Event) {
		emailEngine.React(event)
	})

	// 7.7. Initialize Real-Time WebSocket State Engine
	wsEngine := comms.NewWebSocketEngine()
	go wsEngine.Run()                // Boot the thread state loop
	router.Subscribe(wsEngine.React) // Bind it to intercept discovery/email events

	// 8. Initialize Discovery Agent
	discoveryAgent := agent.NewDiscoveryAgent(router, relationalBrain)

	// 👉 Expose the WebSocket channel safely for Render
	http.Handle("/ws", wsEngine)

	// 👉 NEW: The CEO Directive API Endpoint
	http.HandleFunc("/api/directive", func(w http.ResponseWriter, r *http.Request) {
		// 1. Configure CORS to allow your Next.js dashboard to communicate securely
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		// Handle preflight requests
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		// 2. Extract the target command from the JSON payload
		var req struct {
			Target string `json:"target"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Target == "" {
			http.Error(w, "Invalid payload structure.", http.StatusBadRequest)
			return
		}

		fmt.Printf("\n⚡ [API] Manual CEO Directive Received: '%s'. Rerouting Discovery Engine...\n", req.Target)

		// 3. Fire the Discovery Agent asynchronously without blocking the server
		go discoveryAgent.ExtractLeads(req.Target)

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "Directive Engaged", "target": req.Target})
	})

	// Boot the HTTP/Websocket Server
	go func() {
		port := os.Getenv("PORT")
		if port == "" {
			port = "8080" // Fallback for local testing
		}
		fmt.Printf("🌐 [HTTP] Streaming state socket & API listening on port: %s\n", port)

		// Render requires listening on 0.0.0.0
		if err := http.ListenAndServe("0.0.0.0:"+port, nil); err != nil {
			fmt.Printf("❌ CRITICAL: Server tracking socket runtime crash: %v\n", err)
		}
	}()

	// 9. Deploy Initial Autonomous Pipeline
	go discoveryAgent.ExtractLeads(searchQuery)

	// Sleep timer explicitly set to 180 seconds to allow all LLM execution
	// NOTE: In the future, if you want this server running 24/7 forever, we will swap this sleep timer for a blank `select {}` block!
	time.Sleep(180 * time.Second)
	fmt.Println("\n🛑 [SYSTEM] Execution lifecycle complete. Powering down.")
}
