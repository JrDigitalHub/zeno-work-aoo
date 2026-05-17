package main

import (
	"fmt"
	"time"

	"github.com/JrDigitalHub/zeno-work-aoo/internal/agent"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/orchestrator"
)

func main() {
	fmt.Println("🧠 Zeno OS: Booting Neural Infrastructure...")

	// 1. Ignite the Router
	router := orchestrator.NewEventRouter()
	router.Start()

	// 2. Initialize the Sentinel
	sentinelAgent := agent.NewSentinel()
	
	// 3. Plug the Sentinel's "React" function into the Router
	router.Subscribe(sentinelAgent.React)

	// 4. Initialize and deploy the Predator with a live target
	predatorAgent := agent.NewPredator(router)
	go predatorAgent.Hunt("https://example.com")

	// 5. Keep the system alive long enough for the full lifecycle to execute
	time.Sleep(15 * time.Second)
	fmt.Println("\n🛑 [SYSTEM] Execution lifecycle complete. Powering down.")
}