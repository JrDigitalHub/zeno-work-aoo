package agent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
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
	apiKey      string
}

func NewSentinel(gs *memory.SovereignStore, vs *memory.VectorStore, bo *backoffice.Manager, r *orchestrator.EventRouter) *Sentinel {
	return &Sentinel{
		graphStore:  gs,
		vectorStore: vs,
		backOffice:  bo,
		router:      r,
		apiKey:      os.Getenv("GEMINI_API_KEY"),
	}
}

// Gemini API Payload Structures
type GeminiContent struct {
	Parts []GeminiPart `json:"parts"`
}

type GeminiPart struct {
	Text string `json:"text"`
}

type GeminiGenerateRequest struct {
	Contents []GeminiContent `json:"contents"`
}

type GeminiGenerateResponse struct {
	Candidates []struct {
		Content struct {
			Parts []GeminiPart `json:"parts"`
		} `json:"content"`
	} `json:"candidates"`
}

type GeminiEmbeddingRequest struct {
	Content              GeminiContent `json:"content"`
	OutputDimensionality int           `json:"outputDimensionality"`
}

type GeminiEmbeddingResponse struct {
	Embedding struct {
		Values []float32 `json:"values"`
	} `json:"embedding"`
}

func (s *Sentinel) React(e protocol.Event) {
	if e.Source == "PREDATOR" {
		// 1. Relational sanity check using Neo4j
		// Note: In a fully scaled DB, you will eventually want to index this Recall by WorkspaceID + ID
		if _, exists := s.graphStore.Recall(e.ID); exists {
			fmt.Printf("🛡️ [SENTINEL] Graph memory confirms target [%s] was already processed. Aborting duplicate operation.\n", e.ID)
			return
		}

		// 2. Back-Office Capacity Check (👉 NOW ISOLATED BY WORKSPACE)
		if !s.backOffice.CheckCapacity(e.WorkspaceID) {
			fmt.Printf("⛔ [SENTINEL] Back-Office rejected workflow for Workspace [%s] Target [%s]: Internal capacity maxed out.\n", e.WorkspaceID, e.ID)
			return
		}

		// 3. Immediately reserve the pipeline slot so concurrent subpages don't flood the system (👉 NOW ISOLATED)
		s.backOffice.RegisterPipeline(e.WorkspaceID, e.ID)

		fmt.Printf("\n⚙️ [SENTINEL] Processing New Context! Workspace: [%s] Target ID: %s\n", e.WorkspaceID, e.ID)

		// Check for API Key presence
		if s.apiKey == "" {
			fmt.Println("❌ [SENTINEL] Critical configuration failure: GEMINI_API_KEY environment variable is empty.")
			return
		}

		// 4. Cloud Embedding Generation Loop via Gemini API
		fmt.Println("⚙️ [SENTINEL] Generating high-dimensional vector embeddings...")
		vector, err := s.getEmbedding(e.Payload)
		if err != nil {
			fmt.Printf("❌ [SENTINEL] Embedding generation failed: %v\n", err)
			return
		}

		// 5. Anchoring semantic truth in Qdrant Vector database
		metadata := map[string]any{
			"workspace_id": e.WorkspaceID, // 👉 Enterprise isolation tag
			"url":          e.ID,
			"timestamp":    e.Timestamp,
		}
		err = s.vectorStore.UpsertVector(e.ID, vector, metadata)
		if err != nil {
			fmt.Printf("⚠️ [SENTINEL] Vector upsert failure: %v\n", err)
		}

		// 6. Strategic reasoning loop using cloud inference text completion
		fmt.Println("⚙️ [SENTINEL] Engaging Cloud Neural Core for strategic writing...")

		safePayload := fmt.Sprintf("%v", e.Payload)
		if len(safePayload) > 6000 {
			safePayload = safePayload[:6000] // Upgraded memory ceiling for robust cloud context matching
		}

		prompt := fmt.Sprintf(`You are an elite, outcome-oriented B2B Sales Director. Read the following website data to understand what the company does. Then, write a ruthless, 2-sentence cold email to their founder. 
        
        RULES:
        1. Sentence 1 must state that our autonomous AI pipeline can scale their specific offering. 
        2. Sentence 2 must ask for a quick meeting. 
        3. Do not use polite greetings like 'Dear' or 'Hope you are well'.
        4. OUTPUT ONLY THE EMAIL TEXT. NO PREAMBLE. NO EXPLANATIONS. NO NOTES.

        Website Data: %s`, safePayload)

		reqBody, _ := json.Marshal(GeminiGenerateRequest{
			Contents: []GeminiContent{
				{Parts: []GeminiPart{{Text: prompt}}},
			},
		})

		url := fmt.Sprintf("https://generativelanguage.googleapis.com/v1beta/models/gemini-3.5-flash:generateContent?key=%s", s.apiKey)
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(reqBody))
		if err != nil {
			fmt.Printf("❌ [SENTINEL] Neural Core connection error: %v\n", err)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			fmt.Printf("❌ [SENTINEL] Neural Core execution error status %d: %s\n", resp.StatusCode, string(body))
			return
		}

		body, _ := io.ReadAll(resp.Body)
		var geminiResp GeminiGenerateResponse
		json.Unmarshal(body, &geminiResp)

		responseText := ""
		if len(geminiResp.Candidates) > 0 && len(geminiResp.Candidates[0].Content.Parts) > 0 {
			responseText = geminiResp.Candidates[0].Content.Parts[0].Text
		} else {
			fmt.Println("❌ [SENTINEL] Neural Core returned empty inference matrix response.")
			return
		}

		fmt.Printf("\n✅ [SENTINEL] Sovereign Intelligence Generated for [%s]:\n\n%s\n\n", e.WorkspaceID, responseText)

		// 7. Anchor relationship data in Neo4j Graph
		s.graphStore.Save(protocol.MemoryNode{
			EntityID:   e.ID,
			EntityType: "PROSPECT",
			Context:    fmt.Sprintf("[Workspace: %s] Strategic summary processed.", e.WorkspaceID), // 👉 Tagged context
		})

		// 8. Broadcast output event onto bus
		s.router.Publish(protocol.Event{
			WorkspaceID: e.WorkspaceID, // 👉 Critical: Passes ownership to the outbound Email engine
			ID:          e.ID,
			Source:      "SENTINEL_TEXT_OUTPUT",
			Payload:     responseText,
			Timestamp:   time.Now().Unix(),
		})
	}
}

// getEmbedding calls the Gemini API endpoint to return 768-dimensional vector matrices
func (s *Sentinel) getEmbedding(text string) ([]float32, error) {
	embReq, _ := json.Marshal(GeminiEmbeddingRequest{
		Content: GeminiContent{
			Parts: []GeminiPart{{Text: text}},
		},
		OutputDimensionality: 768, // 👉 Force compression to 768 dimensions for Qdrant compatibility
	})

	// 👉 Updated to the new gemini-embedding-2 model
	url := fmt.Sprintf("https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-2:embedContent?key=%s", s.apiKey)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(embReq))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API endpoint returned error status %d: %s", resp.StatusCode, string(body))
	}

	body, _ := io.ReadAll(resp.Body)
	var embResp GeminiEmbeddingResponse
	err = json.Unmarshal(body, &embResp)
	if err != nil {
		return nil, err
	}

	if len(embResp.Embedding.Values) == 0 {
		return nil, fmt.Errorf("empty embeddings returned from cloud provider matrix")
	}

	return embResp.Embedding.Values, nil
}
