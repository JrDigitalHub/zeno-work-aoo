package orchestrator

import "fmt"

type Event struct {
	ID      string
	Source  string
	Payload string
}

// 1. Define a "Handler" - any function that takes an Event as a parameter
type EventHandler func(Event)

type EventRouter struct {
	eventBus   chan Event
	subscribers []EventHandler // A list of agents listening to the bus
}

func NewEventRouter() *EventRouter {
	return &EventRouter{
		eventBus:    make(chan Event),
		subscribers: make([]EventHandler, 0),
	}
}

// 2. Subscribe allows agents to plug their ears into the Router
func (r *EventRouter) Subscribe(handler EventHandler) {
	r.subscribers = append(r.subscribers, handler)
}

func (r *EventRouter) Start() {
	fmt.Println("🛡️ [ROUTER] Core routing engine online. Awaiting telemetry...")
	
	go func() {
		for {
			incomingEvent := <-r.eventBus
			
			// 3. When an event hits the bus, loop through all subscribers and hand it to them
			for _, handler := range r.subscribers {
				// We run the handler as a goroutine so slow agents don't block the router!
				go handler(incomingEvent) 
			}
		}
	}()
}

func (r *EventRouter) Publish(e Event) {
	r.eventBus <- e
}