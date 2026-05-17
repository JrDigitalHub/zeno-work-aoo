package agent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/JrDigitalHub/zeno-work-aoo/internal/orchestrator"
)

type Sentinel struct{}

func NewSentinel() *Sentinel {
	return &Sentinel{}
}

// 1. Define the exact JSON structure Ollama expects
type OllamaRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
	Stream bool   `json:"stream"`
}

// 2. Define the exact JSON structure Ollama returns
type OllamaResponse struct {
	Response string `json:"response"`
}

func (s *Sentinel) React(e orchestrator.Event) {
	if e.Source == "PREDATOR" {
		fmt.Printf("\n⚙️ [SENTINEL] Intel received! Target ID: %s\n", e.ID)
		fmt.Println("⚙️ [SENTINEL] Engaging Local Neural Core (Qwen)...")

		// 3. Engineer the prompt using the live scraped data
		prompt := fmt.Sprintf("You are a ruthless technical business strategist. Write a 2-sentence cold email to the owner of this website based on their website data. Be direct. Website Data: %s", e.Payload)

		// 4. Package the payload for the local LLM
		reqBody, _ := json.Marshal(OllamaRequest{
			Model:  "qwen2.5:0.5b", // The model we just pulled
			Prompt: prompt,
			Stream: false,        // We want the whole response at once, not streamed
		})

		// 5. Fire the request directly to the local Ollama port (11434)
		resp, err := http.Post("http://localhost:11434/api/generate", "application/json", bytes.NewBuffer(reqBody))
		if err != nil {
			fmt.Println("❌ [SENTINEL] Neural Core offline. Is Ollama running?")
			return
		}
		defer resp.Body.Close()

		// 6. Decode the AI's response
		body, _ := io.ReadAll(resp.Body)
		var ollamaResp OllamaResponse
		json.Unmarshal(body, &ollamaResp)

		fmt.Printf("\n✅ [SENTINEL] Sovereign Intelligence Generated:\n\n%s\n\n", ollamaResp.Response)
	}
}