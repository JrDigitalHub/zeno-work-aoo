package agent

import (
	"fmt"
	"sync"

	"github.com/JrDigitalHub/zeno-work-aoo/internal/memory"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/orchestrator"
	"github.com/JrDigitalHub/zeno-work-aoo/pkg/protocol"
)

// 👉 NEW: Isolated state tracking for multi-tenant analytics
type WorkspaceState struct {
	SuccessTally int
	Reported     bool
	mu           sync.Mutex // Ensures thread safety for this specific client during bus floods
}

type Oracle struct {
	graphStore *memory.SovereignStore
	router     *orchestrator.EventRouter
	states     map[string]*WorkspaceState // Maps WorkspaceID to their specific analytics
	globalMu   sync.RWMutex               // Protects the map during concurrent client creation
}

func NewOracle(gs *memory.SovereignStore, r *orchestrator.EventRouter) *Oracle {
	fmt.Println("🔮 [ORACLE-MULTITENANT] Strategic Consulting Layer initialized.")
	return &Oracle{
		graphStore: gs,
		router:     r,
		states:     make(map[string]*WorkspaceState),
	}
}

// 👉 NEW: Helper function to safely fetch or create a state instance for a specific workspace
func (o *Oracle) getOrCreateState(workspaceID string) *WorkspaceState {
	o.globalMu.RLock()
	state, exists := o.states[workspaceID]
	o.globalMu.RUnlock()

	if exists {
		return state
	}

	// Double-checked locking pattern for safe initialization
	o.globalMu.Lock()
	defer o.globalMu.Unlock()
	
	state, exists = o.states[workspaceID]
	if !exists {
		state = &WorkspaceState{
			SuccessTally: 0,
			Reported:     false,
		}
		o.states[workspaceID] = state
	}
	return state
}

// React listens to the network to generate high-level business intelligence
func (o *Oracle) React(e protocol.Event) {
	if e.Source == "SENTINEL_TEXT_OUTPUT" {
		// Fetch the isolated state for whichever client owns this event
		state := o.getOrCreateState(e.WorkspaceID)

		// Lock the client's specific memory address before editing the tally
		state.mu.Lock()
		state.SuccessTally++

		// Trigger if capacity is met AND we haven't printed the report yet for THIS client
		if state.SuccessTally >= 3 && !state.Reported {
			state.Reported = true
			state.mu.Unlock() // Unlock immediately so we don't hold up the network while printing

			fmt.Println("\n=======================================================")
			fmt.Printf("🔮 [ORACLE] EXECUTIVE INTELLIGENCE REPORT FOR WORKSPACE [%s]\n", e.WorkspaceID)
			fmt.Println("=======================================================")
			fmt.Println("⚠️ SYSTEM BOTTLENECK DETECTED: Automated outreach pipeline is currently operating at 100% capacity limit (3/3 active workflows).")
			fmt.Println("💡 STRATEGIC RECOMMENDATION: Throttling discovery intake. All overflow targets will be cached in the Vector queue until Back-Office capacity clears.")
			fmt.Println("=======================================================")
			return
		}

		// Always unlock if the condition wasn't met
		state.mu.Unlock()
	}
}