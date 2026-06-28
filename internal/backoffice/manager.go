package backoffice

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// --- 1. SYSTEM STATE & STRUCTS ---

// SystemState holds the real-time operational capacity of a specific business workspace
type SystemState struct {
	ActivePipelines int
	MaxCapacity     int
	mu              sync.Mutex // Mutex ensures accurate counting during highly concurrent crawls per client
}

// InternalTicket represents an ingested operational task bound to a specific workspace
type InternalTicket struct {
	WorkspaceID string    `json:"workspace_id"` // Enterprise isolation key
	ID          string    `json:"id"`
	Source      string    `json:"source"` // e.g., "SLACK", "EMAIL"
	Payload     string    `json:"payload"`
	Category    string    `json:"category"`
	Urgency     string    `json:"urgency"`
	Status      string    `json:"status"`
	Timestamp   time.Time `json:"timestamp"`
}

type Manager struct {
	DefaultMaxCapacity int                     // Fallback max capacity for newly registered workspaces
	ClientStates       map[string]*SystemState // Isolated map holding states per workspace
	stateMu            sync.RWMutex            // RWMutex guards the map during high-concurrency multi-client lookups
	WorkerCount        int                     // Number of concurrent back-office workers
	TaskQueue          chan InternalTicket     // Channel for incoming operational tasks
}

// --- 2. INITIALIZATION ---

// NewManager initializes the internal resource tracker and the autonomous worker pool with multi-tenant maps
func NewManager(defaultMaxCapacity int, workerCount int) *Manager {
	fmt.Println("🏢 [BACK-OFF-MULTITENANT] Initializing Operational Workflow Manager for Production...")
	m := &Manager{
		DefaultMaxCapacity: defaultMaxCapacity,
		ClientStates:       make(map[string]*SystemState),
		WorkerCount:        workerCount,
		TaskQueue:          make(chan InternalTicket, 100),
	}

	// Start the autonomous worker pool
	m.StartWorkers()

	return m
}

// getOrCreateState retrieves or provisions a specific operational state for a workspace safely
func (m *Manager) getOrCreateState(workspaceID string) *SystemState {
	m.stateMu.RLock()
	state, exists := m.ClientStates[workspaceID]
	m.stateMu.RUnlock()

	if exists {
		return state
	}

	// Double-checked locking pattern for safe lazy initialization
	m.stateMu.Lock()
	state, exists = m.ClientStates[workspaceID]
	if !exists {
		state = &SystemState{
			ActivePipelines: 0,
			MaxCapacity:     m.DefaultMaxCapacity,
		}
		m.ClientStates[workspaceID] = state
		log.Printf("🏢 [BACK-OFFICE] Provisioned isolated operational pipeline state for Workspace [%s]", workspaceID)
	}
	m.stateMu.Unlock()

	return state
}

// --- 3. CORE CAPACITY LOGIC (ENTERPRISE ISOLATED) ---

// CheckCapacity evaluates if a specific business workspace can handle a new lead or internal task
func (m *Manager) CheckCapacity(workspaceID string) bool {
	state := m.getOrCreateState(workspaceID)

	state.mu.Lock()
	defer state.mu.Unlock()

	return state.ActivePipelines < state.MaxCapacity
}

// RegisterPipeline locks in the resource once a lead or task is processed for a specific workspace
func (m *Manager) RegisterPipeline(workspaceID string, target string) {
	state := m.getOrCreateState(workspaceID)

	state.mu.Lock()
	defer state.mu.Unlock()
	
	state.ActivePipelines++
	fmt.Printf("🏢 [BACK-OFFICE] Pipeline capacity reserved for Workspace [%s] Target [%s]. Active Load: %d/%d\n", 
		workspaceID, target, state.ActivePipelines, state.MaxCapacity)
}

// 👉 NEW: ReleasePipeline safely frees up the capacity slot when a workflow completes or crashes
func (m *Manager) ReleasePipeline(workspaceID string, targetID string) {
	state := m.getOrCreateState(workspaceID)

	state.mu.Lock()
	defer state.mu.Unlock()

	if state.ActivePipelines > 0 {
		state.ActivePipelines--
		fmt.Printf("♻️ [BACK-OFFICE] Pipeline slot released for Workspace [%s] Target [%s]. Active Load: %d/%d\n", 
			workspaceID, targetID, state.ActivePipelines, state.MaxCapacity)
	}
}

// --- 4. AUTONOMOUS INGESTION & WORKER POOL ---

// StartWorkers listens for incoming internal tickets
func (m *Manager) StartWorkers() {
	for i := 0; i < m.WorkerCount; i++ {
		go func(workerID int) {
			for ticket := range m.TaskQueue {
				m.processTicket(workerID, ticket)
			}
		}(i)
	}
}

// Ingest acts as the triage router for incoming multi-tenant data
func (m *Manager) Ingest(workspaceID string, source string, rawPayload string) {
	// Pass the raw text to the Triage Brain (Requires triage.go)
	matrix := AnalyzePayload(rawPayload)

	ticket := InternalTicket{
		WorkspaceID: workspaceID,
		ID:          fmt.Sprintf("ZENO-OP-%d", time.Now().Unix()),
		Source:      source,
		Payload:     rawPayload,
		Category:    matrix.Category,
		Urgency:     matrix.Urgency,
		Status:      "OPEN",
		Timestamp:   time.Now(),
	}

	log.Printf("🏢 [BACK-OFFICE] Triage Alert for [%s]: %s payload classified as [%s | %s]. Action Required: %s", 
		workspaceID, source, matrix.Category, matrix.Urgency, matrix.Action)
		
	// Check capacity for this specific client workspace before queuing
	if m.CheckCapacity(workspaceID) {
		m.RegisterPipeline(workspaceID, ticket.ID)
		m.TaskQueue <- ticket
	} else {
		log.Printf("⚠️ [BACK-OFFICE] OVERLOAD for [%s]: Cannot route ticket %s. Max capacity reached.", workspaceID, ticket.ID)
		// In a production system, you would push this to a Redis dead-letter queue here
	}
}

// processTicket executes the required operational workflow and isolates metrics by tenant
func (m *Manager) processTicket(workerID int, ticket InternalTicket) {
	log.Printf("[COO-Worker-%d] Executing Task for Workspace [%s]: %s | Priority: %s", 
		workerID, ticket.WorkspaceID, ticket.ID, ticket.Urgency)
	
	time.Sleep(2 * time.Second) // Simulate database write or API execution
	
	// TODO: Broadcast ticket creation back to Next.js UI via WebSockets (Ensure client channel filtering)
	log.Printf("[COO-Worker-%d] Task %s autonomously routed and archived for Workspace [%s].", 
		workerID, ticket.ID, ticket.WorkspaceID)

	// 👉 UPDATED: Use the new ReleasePipeline function to clean up
	m.ReleasePipeline(ticket.WorkspaceID, ticket.ID)
}