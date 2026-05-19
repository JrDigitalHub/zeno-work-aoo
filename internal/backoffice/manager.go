package backoffice

import (
	"fmt"
	"sync"
)

// SystemState holds the real-time operational capacity of your business
type SystemState struct {
	ActivePipelines int
	MaxCapacity     int
	mu              sync.Mutex // Mutex ensures accurate counting during highly concurrent crawls
}

type Manager struct {
	State *SystemState
}

// NewManager initializes the internal resource tracker
func NewManager(maxCapacity int) *Manager {
	fmt.Println("🏢 [BACK-OFFICE] Initializing Operational Workflow Manager...")
	return &Manager{
		State: &SystemState{
			ActivePipelines: 0,
			MaxCapacity:     maxCapacity,
		},
	}
}

// CheckCapacity evaluates if the business can handle a new lead
func (m *Manager) CheckCapacity() bool {
	m.State.mu.Lock()
	defer m.State.mu.Unlock()

	return m.State.ActivePipelines < m.State.MaxCapacity
}

// RegisterPipeline locks in the resource once a lead is processed
func (m *Manager) RegisterPipeline(target string) {
	m.State.mu.Lock()
	defer m.State.mu.Unlock()
	
	m.State.ActivePipelines++
	fmt.Printf("🏢 [BACK-OFFICE] Pipeline capacity reserved for [%s]. Active Load: %d/%d\n", target, m.State.ActivePipelines, m.State.MaxCapacity)
}