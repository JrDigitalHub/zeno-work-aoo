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
	brain, err := memory.NewSovereignStore("bolt://localhost:7687", "neo4j", "zeno_admin_password")
	if err != nil {
		panic(fmt.Sprintf("❌ CRITICAL: Failed to boot Neural Graph: %v", err))
	}
	defer brain.Close() 

	// 2. Ignite the Router
	router := orchestrator.NewEventRouter()
	router.Start()

	// 3. Initialize the Sentinel
	sentinelAgent := agent.NewSentinel(brain)
	router.Subscribe(sentinelAgent.React)

	// 4. Initialize the Predator
	predatorAgent := agent.NewPredator(router)
	
	// 5. Define a pipeline of targets (Using safe, highly available public text domains for validation)
	targets := []string{
		"https://example.com",
		"https://www.iana.org/domains/reserved",
	}

	fmt.Printf("🦅 [SYSTEM] Deploying Predator concurrently across %d targets...\n", len(targets))
	
	// 6. Launch the Predator across all targets using concurrent goroutines
	for _, target := range targets {
		go predatorAgent.Hunt(target)
	}

	// 7. Keep the core runtime alive long enough to process all async events through the bus
	time.Sleep(45 * time.Second)
	fmt.Println("\n🛑 [SYSTEM] Execution lifecycle complete. Powering down.")
}