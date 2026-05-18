package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/JrDigitalHub/zeno-work-aoo/internal/agent"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/memory"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/orchestrator"
)

func main() {
	fmt.Println("🧠 Zeno OS: Booting Autonomous Neural Infrastructure...")

	// 1. Parse terminal inputs dynamically
	searchQuery := "Lagos supply chain logistics PLC" // Default fallback parameter
	if len(os.Args) > 1 {
		// Joins any phrase passed after the run command
		searchQuery = strings.Join(os.Args[1:], " ")
	}

	// 2. Ignite the Neo4j Memory Store
	brain, err := memory.NewSovereignStore("bolt://localhost:7687", "neo4j", "zeno_admin_password")
	if err != nil {
		panic(fmt.Sprintf("❌ CRITICAL: Failed to boot Neural Graph: %v", err))
	}
	defer brain.Close() 

	// 3. Ignite the Router
	router := orchestrator.NewEventRouter()
	router.Start()

	// 4. Initialize and Subscribe the Sentinel (Brain)
	sentinelAgent := agent.NewSentinel(brain)
	router.Subscribe(sentinelAgent.React)

	// 5. Initialize and Subscribe the Predator (Eyes)
	predatorAgent := agent.NewPredator(router)
	router.Subscribe(predatorAgent.React) 

	// 6. Initialize the Discovery Agent (The Ingestion Funnel)
	discoveryAgent := agent.NewDiscoveryAgent(router)
	
	// 7. TRIGGER THE LOOPS: Hand the dynamic parameters to the pipeline
	go discoveryAgent.ExtractLeads(searchQuery)

	// 8. Keep runtime alive for async channel lifecycle execution
	time.Sleep(60 * time.Second)
	fmt.Println("\n🛑 [SYSTEM] Execution lifecycle complete. Powering down.")
}