package main

import (
	"fmt"
	"time"

	"github.com/JrDigitalHub/zeno-work-aoo/internal/agent"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/memory"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/orchestrator"
)

func main() {
	fmt.Println("🧠 Zeno OS: Booting Neural Infrastructure...")

	// 1. Ignite the Neo4j Memory Store
	// Port 7687 is the high-speed Bolt protocol tunnel
	brain, err := memory.NewSovereignStore("bolt://localhost:7687", "neo4j", "zeno_admin_password")
	if err != nil {
		panic(fmt.Sprintf("❌ CRITICAL: Failed to boot Neural Graph: %v", err))
	}
	defer brain.Close() // Ensures the DB connection closes safely on shutdown

	// 2. Ignite the Router
	router := orchestrator.NewEventRouter()
	router.Start()

	// 3. Initialize the Sentinel (with graph access)
	sentinelAgent := agent.NewSentinel(brain)
	router.Subscribe(sentinelAgent.React)

	// 4. Initialize the Predator
	predatorAgent := agent.NewPredator(router)
	
	// TEST THE GRAPH: Send the Predator out twice
	go predatorAgent.Hunt("https://example.com")
	time.Sleep(25 * time.Second) 
	
	fmt.Println("\n🦅 [SYSTEM] Deploying Predator to the same target again to verify Graph Memory...")
	go predatorAgent.Hunt("https://example.com")

	time.Sleep(15 * time.Second)
	fmt.Println("\n🛑 [SYSTEM] Execution lifecycle complete. Powering down.")
}