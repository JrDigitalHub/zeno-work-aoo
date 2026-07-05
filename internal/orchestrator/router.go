package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/JrDigitalHub/zeno-work-aoo/pkg/protocol"
	"github.com/redis/go-redis/v9"
)

// ==========================================
// 1. THE INTERNAL NEURAL BUS (REDIS)
// ==========================================

type EventHandler func(protocol.Event)

type EventRouter struct {
	client      *redis.Client
	subscribers []EventHandler
	ctx         context.Context
}

func NewEventRouter() *EventRouter {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "localhost:6379" // Default local Docker port
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     redisURL,
		Password: "", 
		DB:       0,
		PoolSize: 100,
	})

	return &EventRouter{
		client:      rdb,
		subscribers: make([]EventHandler, 0),
		ctx:         context.Background(),
	}
}

func (r *EventRouter) Subscribe(handler EventHandler) {
	r.subscribers = append(r.subscribers, handler)
}

func (r *EventRouter) Start() {
	fmt.Println("🛡️ [ROUTER] Redis Protocol Engine online. Awaiting telemetry...")

	go func() {
		pubsub := r.client.Subscribe(r.ctx, "zeno_neural_bus")
		defer pubsub.Close()

		channel := pubsub.Channel()
		for msg := range channel {
			var incomingEvent protocol.Event
			if err := json.Unmarshal([]byte(msg.Payload), &incomingEvent); err != nil {
				log.Printf("❌ [ROUTER] Failed to decode telemetry: %v", err)
				continue
			}
			for _, handler := range r.subscribers {
				go handler(incomingEvent)
			}
		}
	}()
}

func (r *EventRouter) Publish(e protocol.Event) {
	payload, err := json.Marshal(e)
	if err != nil {
		return
	}
	r.client.Publish(r.ctx, "zeno_neural_bus", payload)
}

// ==========================================
// 2. THE EXTERNAL HTTP API (NEXT.JS FACING)
// ==========================================

// SetupHTTPRouter creates the REST endpoints that Vercel connects to
func SetupHTTPRouter(eventBus *EventRouter) *http.ServeMux {
	mux := http.NewServeMux()

	// 1. Global State: Wallet Hydration
	mux.HandleFunc("/api/v1/wallet", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		
		// NOTE: In production, extract WorkspaceID from the JWT context here!
		
		json.NewEncoder(w).Encode(map[string]interface{}{
			"token_balance":     10000,
			"subscription_tier": "Trial",
			"workspace_id":      "auto-provisioned-uuid-here",
			"user": map[string]string{
				"name":     "Zeno Administrator",
				"email":    "admin@jrdigitalhubltd.com",
				"initials": "ZA",
			},
		})
	})

	// 2. Oracle: Lead Generation Scan
	mux.HandleFunc("/api/v1/oracle/scan", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Example response matching the Oracle frontend UI structure
		json.NewEncoder(w).Encode([]map[string]interface{}{
			{
				"id":      1,
				"company": "Apex Dynamics",
				"contact": "Jane Doe",
				"role":    "VP of Strategy",
				"domain":  "apexdynamics.io",
				"status":  "Verified",
				"score":   94,
			},
		})
	})

	// 3. Sentinel: Operations / Tasks (Formerly COO)
	mux.HandleFunc("/api/v1/sentinel/tasks", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Returns the Kanban Board structure the frontend expects
		json.NewEncoder(w).Encode(map[string]interface{}{
			"backlog": []map[string]interface{}{
				{"id": 1, "title": "Audit Q3 Vendor Contracts", "priority": "High", "assignee": "Sentinel", "createdAt": time.Now().Format("Jan 02")},
			},
			"in-progress": []map[string]interface{}{
				{"id": 2, "title": "Scale Redis Cluster", "priority": "Critical", "assignee": "DevOps", "createdAt": time.Now().Format("Jan 02")},
			},
			"completed": []interface{}{},
		})
	})

	// 4. Modeler: Financial Ledger (Formerly CFO)
	mux.HandleFunc("/api/v1/modeler/ledger", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		json.NewEncoder(w).Encode(map[string]interface{}{
			"balance": 284950.0,
			"inflow":  128400.0,
			"outflow": 51200.0,
			"transactions": []map[string]interface{}{
				{"id": "TXN-001", "date": "Jul 05, 2026", "description": "Enterprise SaaS License", "category": "Revenue", "amount": 18000, "type": "incoming"},
				{"id": "TXN-002", "date": "Jul 04, 2026", "description": "AWS Infrastructure", "category": "Ops", "amount": -2400, "type": "outgoing"},
			},
		})
	})

	return mux
}
