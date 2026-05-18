package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/JrDigitalHub/zeno-work-aoo/internal/agent"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/comms"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/memory"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/orchestrator"
)

func main() {
	fmt.Println("🧠 Zeno OS: Booting Autonomous Neural Infrastructure with Voice Core...")

	searchQuery := "Lagos supply chain logistics PLC"
	if len(os.Args) > 1 {
		searchQuery = strings.Join(os.Args[1:], " ")
	}

	// 1. Ignite the Neo4j Memory Store
	brain, err := memory.NewSovereignStore("bolt://localhost:7687", "neo4j", "zeno_admin_password")
	if err != nil {
		panic(fmt.Sprintf("❌ CRITICAL: Failed to boot Neural Graph: %v", err))
	}
	defer brain.Close() 

	// 2. Ignite the Router
	router := orchestrator.NewEventRouter()
	router.Start()

	// 3. Initialize and Subscribe the Sentinel (Now passes router link)
	sentinelAgent := agent.NewSentinel(brain, router)
	router.Subscribe(sentinelAgent.React)

	// 4. Initialize and Subscribe the Predator (Scraper)
	predatorAgent := agent.NewPredator(router)
	router.Subscribe(predatorAgent.React) 

	// 5. Initialize and Subscribe the Sovereign Voice Core (Whisper/StyleTTS2 handles)
	// Port 4321 is the standard default port for local StyleTTS2 inference endpoints
	voiceEngine := comms.NewVoiceEngine("http://localhost:8000", "http://localhost:4321")
	router.Subscribe(voiceEngine.React)

	// 6. Initialize the Discovery Agent 
	discoveryAgent := agent.NewDiscoveryAgent(router)
	
	// 7. Fire the loop
	go discoveryAgent.ExtractLeads(searchQuery)

	time.Sleep(60 * time.Second)
	fmt.Println("\n🛑 [SYSTEM] Execution lifecycle complete. Powering down.")
}