package orchestrator

import (
	"fmt"
	"github.com/JrDigitalHub/zeno-work-aoo/pkg/protocol"
)

// 1. The router now strictly uses the protocol.Event
type EventHandler func(protocol.Event)

type EventRouter struct {
	eventBus    chan protocol.Event
	subscribers []EventHandler
}

func NewEventRouter() *EventRouter {
	return &EventRouter{
		eventBus:    make(chan protocol.Event),
		subscribers: make([]EventHandler, 0),
	}
}

func (r *EventRouter) Subscribe(handler EventHandler) {
	r.subscribers = append(r.subscribers, handler)
}

func (r *EventRouter) Start() {
	fmt.Println("🛡️ [ROUTER] Protocol Engine online. Awaiting telemetry...")
	
	go func() {
		for {
			incomingEvent := <-r.eventBus
			for _, handler := range r.subscribers {
				go handler(incomingEvent)
			}
		}
	}()
}

func (r *EventRouter) Publish(e protocol.Event) {
	r.eventBus <- e
}