package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/JrDigitalHub/zeno-work-aoo/internal/agent"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/backoffice"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/comms"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/memory"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/orchestrator"
)

func main() {
	// Look for this specific header to confirm the Oracle is injected!
	fmt.Println("🧠 Zeno OS: Booting Unified Neural Infrastructure [Graph + Vector + Back-Office + Oracle]...")

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
	vectorBrain, err := memory.NewVectorStore("localhost:6334", "zeno_intel_vectors")
	if err != nil {
		panic(fmt.Sprintf("❌ CRITICAL: Failed to boot Vector Memory: %v", err))
	}
	defer vectorBrain.Close()

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

	// 8. Deploy Pipeline
	discoveryAgent := agent.NewDiscoveryAgent(router)
	go discoveryAgent.ExtractLeads(searchQuery)

	// Sleep timer explicitly set to 180 seconds to allow all LLM execution
	time.Sleep(180 * time.Second)
	fmt.Println("\n🛑 [SYSTEM] Execution lifecycle complete. Powering down.")
}
