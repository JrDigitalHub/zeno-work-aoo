package comms

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/JrDigitalHub/zeno-work-aoo/pkg/protocol"
	"github.com/gorilla/websocket"
)

// SystemStatePayload defines the structured message format broadcasted to the Next.js dashboard.
type SystemStatePayload struct {
	EventName string      `json:"event_name"` // e.g., "LEAD_DISCOVERED", "EMAIL_DISPATCHED", "SYSTEM_ALERT"
	Timestamp int64       `json:"timestamp"`
	Data      interface{} `json:"data"`
}

// Client represents a single active frontend dashboard connection.
type Client struct {
	Hub  *WebSocketEngine
	Conn *websocket.Conn
	Send chan []byte
}

// WebSocketEngine manages active client states and acts as a reactive node on the system router.
type WebSocketEngine struct {
	Clients    map[*Client]bool
	Broadcast  chan SystemStatePayload
	Register   chan *Client
	Unregister chan *Client
	mu         sync.RWMutex
}

// Upgrader configures the HTTP to WebSocket protocol elevation parameters.
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		// Allows cross-origin requests from your local Next.js frontend dashboard environment
		return true
	},
}

// NewWebSocketEngine initializes the real-time state router.
func NewWebSocketEngine() *WebSocketEngine {
	return &WebSocketEngine{
		Clients:    make(map[*Client]bool),
		Broadcast:  make(chan SystemStatePayload, 256),
		Register:   make(chan *Client),
		Unregister: make(chan *Client),
	}
}

// Run activates the background state-machine lifecycle loops for connection tracking.
func (w *WebSocketEngine) Run() {
	fmt.Println("⚡ [WS Engine] Operational state machine initialized.")
	for {
		select {
		case client := <-w.Register:
			w.mu.Lock()
			w.Clients[client] = true
			w.mu.Unlock()
			fmt.Println("🔌 [WS Engine] Next.js Dashboard client connected securely.")

		case client := <-w.Unregister:
			w.mu.Lock()
			if _, ok := w.Clients[client]; ok {
				delete(w.Clients, client)
				close(client.Send)
			}
			w.mu.Unlock()
			fmt.Println("🔌 [WS Engine] Dashboard client connection severed.")

		case payload := <-w.Broadcast:
			// Marshal the state payload to crisp JSON bytes
			jsonBytes, err := json.Marshal(payload)
			if err != nil {
				fmt.Printf("❌ [WS Engine] Failed to serialize system state message: %v\n", err)
				continue
			}

			w.mu.RLock()
			for client := range w.Clients {
				select {
				case client.Send <- jsonBytes:
				default:
					close(client.Send)
					delete(w.Clients, client)
				}
			}
			w.mu.RUnlock()
		}
	}
}

// ServeHTTP handles incoming Next.js frontend upgrade handshake requests (e.g., ws://localhost:8080/ws)
func (w *WebSocketEngine) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(rw, r, nil)
	if err != nil {
		fmt.Printf("❌ [WS Engine] Upgrade connection protocol failure: %v\n", err)
		return
	}

	client := &Client{Hub: w, Conn: conn, Send: make(chan []byte, 256)}
	w.Register <- client

	// Start background synchronization routines per-client connection pool
	go client.writePump()
	go client.readPump()
}

// React intercepts live system events from the central EventRouter and translates them to WebSocket broadcasts.
func (w *WebSocketEngine) React(event protocol.Event) {
	switch event.Source {
	case "DISCOVERY":
		w.Broadcast <- SystemStatePayload{
			EventName: "LEAD_DISCOVERED",
			Timestamp: event.Timestamp,
			Data:      event.Payload,
		}
	case "SENTINEL_TEXT_OUTPUT":
		w.Broadcast <- SystemStatePayload{
			EventName: "SENTINEL_TEXT_OUTPUT",
			Timestamp: event.Timestamp,
			Data:      event.Payload,
		}
	default:
		return
	}
}

// writePump drains outbound messages from the client's internal pipeline and pushes them to the socket.
func (c *Client) writePump() {
	defer func() {
		c.Conn.Close()
	}()
	for {
		message, ok := <-c.Send
		if !ok {
			c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
			return
		}

		w, err := c.Conn.NextWriter(websocket.TextMessage)
		if err != nil {
			return
		}
		w.Write(message)

		if err := w.Close(); err != nil {
			return
		}
	}
}

// readPump maintains connection heartbeat integrity and processes incoming dashboard control messages.
func (c *Client) readPump() {
	defer func() {
		c.Hub.Unregister <- c
		c.Conn.Close()
	}()

	// Configure baseline reading constraints
	c.Conn.SetReadLimit(512)

	for {
		_, _, err := c.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				fmt.Printf("⚠️ [WS Engine] Unexpected client socket drop-off: %v\n", err)
			}
			break
		}
	}
}
