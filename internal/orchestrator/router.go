package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/JrDigitalHub/zeno-work-aoo/pkg/protocol"
	"github.com/redis/go-redis/v9"
)

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

	// 👉 ENTERPRISE UPGRADE: Connect to Redis Broker
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisURL,
		Password: "", 
		DB:       0,
		PoolSize: 100, // Massive concurrency limit for the event bus
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

		// Listen for messages from Redis and unmarshal them back into protocol.Event
		for msg := range channel {
			var incomingEvent protocol.Event
			if err := json.Unmarshal([]byte(msg.Payload), &incomingEvent); err != nil {
				log.Printf("❌ [ROUTER] Failed to decode telemetry: %v", err)
				continue
			}

			// Distribute to all observing Agents
			for _, handler := range r.subscribers {
				go handler(incomingEvent)
			}
		}
	}()
}

func (r *EventRouter) Publish(e protocol.Event) {
	payload, err := json.Marshal(e)
	if err != nil {
		log.Printf("❌ [ROUTER] Failed to marshal event: %v", err)
		return
	}

	// Broadcast the payload out to the Redis stream
	err = r.client.Publish(r.ctx, "zeno_neural_bus", payload).Err()
	if err != nil {
		log.Printf("❌ [ROUTER] Redis publish failed: %v", err)
	}
}