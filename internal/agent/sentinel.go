package agent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/JrDigitalHub/zeno-work-aoo/internal/memory"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/orchestrator"
	"github.com/JrDigitalHub/zeno-work-aoo/pkg/protocol"
)

type Sentinel struct {
	store  *memory.SovereignStore
	router *orchestrator.EventRouter // Added pipeline link to broadcast output
}

func NewSentinel(store *memory.SovereignStore, router *orchestrator.EventRouter) *Sentinel {
	return &Sentinel{
		store:  store,
		router: router,
	}
}

type OllamaRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
	Stream bool   `json:"stream"`
}

type OllamaResponse struct {
	Response string `json:"response"`
}

func (s *Sentinel) React(e protocol.Event) {
	if e.Source == "PREDATOR" {
		if _, exists := s.store.Recall(e.ID); exists {
			fmt.Printf("🛡️ [SENTINEL] Graph memory confirms target [%s] was already processed. Aborting duplicate operation.\n", e.ID)
			return
		}

		fmt.Printf("\n⚙️ [SENTINEL] New Intel received! Target ID: %s\n", e.ID)
		fmt.Println("⚙️ [SENTINEL] Engaging Local Neural Core (Qwen)...")

		prompt := fmt.Sprintf("You are a technical business strategist. Write a 2-sentence cold message to the owner of this website. Be direct. Website Data: %s", e.Payload)

		reqBody, _ := json.Marshal(OllamaRequest{
			Model:  "qwen2.5:0.5b",
			Prompt: prompt,
			Stream: false,
		})

		resp, err := http.Post("http://localhost:11434/api/generate", "application/json", bytes.NewBuffer(reqBody))
		if err != nil {
			fmt.Println("❌ [SENTINEL] Neural Core offline. Is Ollama running?")
			return
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		var ollamaResp OllamaResponse
		json.Unmarshal(body, &ollamaResp)

		fmt.Printf("\n✅ [SENTINEL] Sovereign Intelligence Generated:\n\n%s\n\n", ollamaResp.Response)

		s.store.Save(protocol.MemoryNode{
			EntityID:   e.ID,
			EntityType: "PROSPECT",
			Context:    "Cold strategy drafted successfully.",
		})

		// 🔥 NEW: Publish the text generation event so the Sovereign Voice Engine can synthesize it!
		s.router.Publish(protocol.Event{
			ID:        e.ID,
			Source:    "SENTINEL_TEXT_OUTPUT",
			Payload:   ollamaResp.Response,
			Timestamp: time.Now().Unix(),
		})
	}
}