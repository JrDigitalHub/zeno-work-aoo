package agent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/JrDigitalHub/zeno-work-aoo/internal/backoffice"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/memory"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/orchestrator"
	"github.com/JrDigitalHub/zeno-work-aoo/pkg/protocol"
)

type Sentinel struct {
	graphStore  *memory.SovereignStore
	vectorStore *memory.VectorStore
	backOffice  *backoffice.Manager
	router      *orchestrator.EventRouter
}

func NewSentinel(gs *memory.SovereignStore, vs *memory.VectorStore, bo *backoffice.Manager, r *orchestrator.EventRouter) *Sentinel {
	return &Sentinel{
		graphStore:  gs,
		vectorStore: vs,
		backOffice:  bo,
		router:      r,
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

// Structures for generating embeddings locally
type EmbeddingRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
}

type EmbeddingResponse struct {
	Embedding []float32 `json:"embedding"`
}

func (s *Sentinel) React(e protocol.Event) {
	if e.Source == "PREDATOR" {
		// 1. Relational sanity check using Neo4j
		if _, exists := s.graphStore.Recall(e.ID); exists {
			fmt.Printf("🛡️ [SENTINEL] Graph memory confirms target [%s] was already processed. Aborting duplicate operation.\n", e.ID)
			return
		}

		// 2. NEW: Back-Office Capacity Check
		if !s.backOffice.CheckCapacity() {
			fmt.Printf("⛔ [SENTINEL] Back-Office rejected workflow for [%s]: Internal capacity maxed out.\n", e.ID)
			return
		}

		// 3. NEW: Immediately reserve the pipeline slot so concurrent subpages don't flood the system
		s.backOffice.RegisterPipeline(e.ID)

		fmt.Printf("\n⚙️ [SENTINEL] Processing New Context! Target ID: %s\n", e.ID)

		// 4. Local Embedding Generation Loop via Ollama
		fmt.Println("⚙️ [SENTINEL] Generating high-dimensional vector embeddings...")
		vector, err := s.getEmbedding(e.Payload)
		if err != nil {
			fmt.Printf("❌ [SENTINEL] Embedding generation failed: %v\n", err)
			return
		}

		// 5. Anchoring semantic truth in Qdrant Vector database
		metadata := map[string]any{
			"url":       e.ID,
			"timestamp": e.Timestamp,
		}
		err = s.vectorStore.UpsertVector(e.ID, vector, metadata)
		if err != nil {
			fmt.Printf("⚠️ [SENTINEL] Vector upsert failure: %v\n", err)
		}

		// 6. Strategic reasoning loop using local inference text completion
		fmt.Println("⚙️ [SENTINEL] Engaging Local Neural Core for strategic writing...")

		// 👉 ADD THIS SAFETY SCISSORS BLOCK:
		safePayload := fmt.Sprintf("%v", e.Payload)
		if len(safePayload) > 3000 {
			safePayload = safePayload[:3000] // Keep only the first 3000 characters to prevent LLM memory crashes
		}

		// UPGRADED PROMPT FOR HIGH-CONVERTING SDR OUTREACH
		prompt := fmt.Sprintf(`You are an elite, outcome-oriented B2B Sales Director. Read the following website data to understand what the company does. Then, write a ruthless, 2-sentence cold email to their founder. 
		
		RULES:
		1. Sentence 1 must state that our autonomous AI pipeline can scale their specific offering. 
		2. Sentence 2 must ask for a quick meeting. 
		3. Do not use polite greetings like 'Dear' or 'Hope you are well'.
		4. OUTPUT ONLY THE EMAIL TEXT. NO PREAMBLE. NO EXPLANATIONS. NO NOTES.

		Website Data: %s`, safePayload)

		reqBody, _ := json.Marshal(OllamaRequest{
			Model:  "llama3.2",
			Prompt: prompt,
			Stream: false,
		})

		resp, err := http.Post("http://localhost:11434/api/generate", "application/json", bytes.NewBuffer(reqBody))
		if err != nil {
			fmt.Println("❌ [SENTINEL] Neural Core offline.")
			return
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		var ollamaResp OllamaResponse
		json.Unmarshal(body, &ollamaResp)

		fmt.Printf("\n✅ [SENTINEL] Sovereign Intelligence Generated:\n\n%s\n\n", ollamaResp.Response)

		// 7. Anchor relationship data in Neo4j Graph
		s.graphStore.Save(protocol.MemoryNode{
			EntityID:   e.ID,
			EntityType: "PROSPECT",
			Context:    "Strategic summary processed.",
		})

		// 8. Broadcast output event onto bus
		s.router.Publish(protocol.Event{
			ID:        e.ID,
			Source:    "SENTINEL_TEXT_OUTPUT",
			Payload:   ollamaResp.Response,
			Timestamp: time.Now().Unix(),
		})
	}
}

// getEmbedding calls local Ollama endpoint to return vector matrices
func (s *Sentinel) getEmbedding(text string) ([]float32, error) {
	embReq, _ := json.Marshal(EmbeddingRequest{
		Model:  "nomic-embed-text", // Qwen natively supports feature extraction embeddings
		Prompt: text,
	})

	resp, err := http.Post("http://localhost:11434/api/embeddings", "application/json", bytes.NewBuffer(embReq))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var embResp EmbeddingResponse
	err = json.Unmarshal(body, &embResp)
	if err != nil {
		return nil, err
	}

	return embResp.Embedding, nil
}
