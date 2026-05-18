package agent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/JrDigitalHub/zeno-work-aoo/internal/memory"
	"github.com/JrDigitalHub/zeno-work-aoo/pkg/protocol"
)

// The Sentinel now requires a connection to the Sovereign Memory
type Sentinel struct {
	store *memory.SovereignStore
}

// Update the constructor to accept the Neo4j brain
func NewSentinel(store *memory.SovereignStore) *Sentinel {
	return &Sentinel{
		store: store,
	}
}

// Define the Ollama structures
type OllamaRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
	Stream bool   `json:"stream"`
}

type OllamaResponse struct {
	Response string `json:"response"`
}

// React now accepts the official protocol.Event
func (s *Sentinel) React(e protocol.Event) {
	if e.Source == "PREDATOR" {
		// 1. MEMORY CHECK: Ask the Neo4j Graph if we have already targeted this URL
		if _, exists := s.store.Recall(e.ID); exists {
			fmt.Printf("🛡️ [SENTINEL] Graph memory confirms target [%s] was already processed. Aborting duplicate operation.\n", e.ID)
			return
		}

		fmt.Printf("\n⚙️ [SENTINEL] New Intel received! Target ID: %s\n", e.ID)
		fmt.Println("⚙️ [SENTINEL] Engaging Local Neural Core (Qwen)...")

		// 2. Engineer the prompt
		prompt := fmt.Sprintf("You are a ruthless technical business strategist. Write a 2-sentence cold email to the owner of this website based on their website data. Be direct. Website Data: %s", e.Payload)

		reqBody, _ := json.Marshal(OllamaRequest{
			Model:  "qwen2.5:0.5b",
			Prompt: prompt,
			Stream: false,
		})

		// 3. Fire the request to local Ollama
		resp, err := http.Post("http://localhost:11434/api/generate", "application/json", bytes.NewBuffer(reqBody))
		if err != nil {
			fmt.Println("❌ [SENTINEL] Neural Core offline. Is Ollama running?")
			return
		}
		defer resp.Body.Close()

		// 4. Decode the response
		body, _ := io.ReadAll(resp.Body)
		var ollamaResp OllamaResponse
		json.Unmarshal(body, &ollamaResp)

		fmt.Printf("\n✅ [SENTINEL] Sovereign Intelligence Generated:\n\n%s\n\n", ollamaResp.Response)

		// 5. MEMORY ANCHOR: Save this success into the Graph so we never email them again
		s.store.Save(protocol.MemoryNode{
			EntityID:   e.ID,
			EntityType: "PROSPECT",
			Context:    "Cold email drafted successfully.",
		})
	}
}