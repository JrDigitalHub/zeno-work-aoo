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
	fmt.Println("🧠 Zeno OS: Booting Unified Neural Infrastructure [Graph + Vector Memory + Back-Office]...")

	searchQuery := "Corporate law firms in Lagos"
	if len(os.Args) > 1 {
		searchQuery = strings.Join(os.Args[1:], " ")
	}

	// 1. Ignite the Neo4j Graph Memory Store
	graphBrain, err := memory.NewSovereignStore("bolt://localhost:7687", "neo4j", "zeno_admin_password")
	if err != nil {
		panic(fmt.Sprintf("❌ CRITICAL: Failed to boot Graph Memory: %v", err))
	}
	defer graphBrain.Close() 

	// 2. Ignite the Qdrant Vector Memory Store
	vectorBrain, err := memory.NewVectorStore("localhost:6334", "zeno_intel_vectors")
	if err != nil {
		panic(fmt.Sprintf("❌ CRITICAL: Failed to boot Vector Memory: %v", err))
	}
	defer vectorBrain.Close()

	// 3. Initialize the Back-Office Engine with a strict capacity pipeline of 3
	opsManager := backoffice.NewManager(3)

	// 4. Ignite the Router Bus
	router := orchestrator.NewEventRouter()
	router.Start()

	// 5. Initialize the Sentinel with access to BOTH brains AND the Back-Office
	sentinelAgent := agent.NewSentinel(graphBrain, vectorBrain, opsManager, router)
	router.Subscribe(sentinelAgent.React)

	// 6. Initialize other agents
	predatorAgent := agent.NewPredator(router)
	router.Subscribe(predatorAgent.React) 

	voiceEngine := comms.NewVoiceEngine("http://localhost:8000", "http://localhost:4321")
	router.Subscribe(voiceEngine.React)

	discoveryAgent := agent.NewDiscoveryAgent(router)
	
	// 7. Deploy the automated extraction chain reaction
	go discoveryAgent.ExtractLeads(searchQuery)

	time.Sleep(60 * time.Second)
	fmt.Println("\n🛑 [SYSTEM] Execution lifecycle complete. Powering down.")
}