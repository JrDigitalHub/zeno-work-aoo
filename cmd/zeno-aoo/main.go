package main

import (
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

	// 1. Ignite Neo4j
	graphBrain, err := memory.NewSovereignStore("bolt://localhost:7687", "neo4j", "zeno_admin_password")
	if err != nil {
		panic(fmt.Sprintf("❌ CRITICAL: Failed to boot Graph Memory: %v", err))
	}
	defer graphBrain.Close()

	// 2. Ignite Qdrant
	vectorBrain, err := memory.NewVectorStore("localhost:6334", "zeno_intel_vectors_v3")
	if err != nil {
		panic(fmt.Sprintf("❌ CRITICAL: Failed to boot Vector Memory: %v", err))
	}
	defer vectorBrain.Close()

	// 2.5 Ignite Relational Brain (Supabase / Postgres)
	var relationalBrain *memory.RelationalStore // 👉 DECLARE IT HERE SO THE WHOLE SYSTEM CAN SEE IT

	supabaseURL := os.Getenv("SUPABASE_URL")
	if supabaseURL == "" {
		fmt.Println("⚠️ WARNING: SUPABASE_URL not found in environment. State persistence is offline.")
	} else {
		// Initialize it and assign it to our globally scoped variable
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

	// Expose the WebSocket channel to incoming local connections
	http.Handle("/ws", wsEngine)
	go func() {
		fmt.Println("🌐 [HTTP] Streaming state socket initialized on: ws://localhost:8080/ws")
		if err := http.ListenAndServe(":8080", nil); err != nil {
			fmt.Printf("❌ CRITICAL: Server tracking socket runtime crash: %v\n", err)
		}
	}()

	// 8. Deploy Pipeline
	discoveryAgent := agent.NewDiscoveryAgent(router, relationalBrain) // 👉 Pass it in here!
	go discoveryAgent.ExtractLeads(searchQuery)

	// Sleep timer explicitly set to 180 seconds to allow all LLM execution
	time.Sleep(180 * time.Second)
	fmt.Println("\n🛑 [SYSTEM] Execution lifecycle complete. Powering down.")
}
