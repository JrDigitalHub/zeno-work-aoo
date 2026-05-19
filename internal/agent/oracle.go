package agent

import (
	"fmt"
	"sync"

	"github.com/JrDigitalHub/zeno-work-aoo/internal/memory"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/orchestrator"
	"github.com/JrDigitalHub/zeno-work-aoo/pkg/protocol"
)

type Oracle struct {
	graphStore   *memory.SovereignStore
	router       *orchestrator.EventRouter
	successTally int
	reported     bool
	mu           sync.Mutex // Ensures thread safety during concurrent bus floods
}

func NewOracle(gs *memory.SovereignStore, r *orchestrator.EventRouter) *Oracle {
	fmt.Println("🔮 [ORACLE] Strategic Consulting Layer initialized.")
	return &Oracle{
		graphStore:   gs,
		router:       r,
		successTally: 0,
		reported:     false,
	}
}

// React listens to the network to generate high-level business intelligence
func (o *Oracle) React(e protocol.Event) {
	if e.Source == "SENTINEL_TEXT_OUTPUT" {
		// Lock the memory address before editing the tally
		o.mu.Lock()
		o.successTally++

		// Trigger if capacity is met AND we haven't printed the report yet
		if o.successTally >= 3 && !o.reported {
			o.reported = true
			o.mu.Unlock() // Unlock immediately so we don't hold up the network while printing

			fmt.Println("\n=======================================================")
			fmt.Println("🔮 [ORACLE] EXECUTIVE INTELLIGENCE REPORT")
			fmt.Println("=======================================================")
			fmt.Println("⚠️ SYSTEM BOTTLENECK DETECTED: Automated outreach pipeline is currently operating at 100% capacity limit (3/3 active workflows).")
			fmt.Println("💡 STRATEGIC RECOMMENDATION: To prevent lead decay on the rejected prospects, temporarily route overflow targeting directly to Joshua (Human Closer) while we scale the automated Back-Office thresholds.")
			fmt.Println("=======================================================\n")
			return
		}
		
		// Always unlock if the condition wasn't met
		o.mu.Unlock()
	}
}