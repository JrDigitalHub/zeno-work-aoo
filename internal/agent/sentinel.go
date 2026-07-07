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
	backOffice  *backoffice.PipelineManager
	router      *orchestrator.EventRouter
	apiKey      string
}

func NewSentinel(gs *memory.SovereignStore, vs *memory.VectorStore, bo *backoffice.PipelineManager, r *orchestrator.EventRouter) *Sentinel {
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

func (s *Sentinel) React(ctx context.Context, e protocol.Event) error {
	if e.Source == "PREDATOR" {
		// 1. Relational sanity check using Neo4j
		if _, exists := s.graphStore.Recall(e.ID); exists {
			fmt.Printf("🛡️ [SENTINEL] Graph memory confirms target [%s] was already processed. Aborting duplicate operation.\n", e.ID)
			return nil
		}

		// 2. Back-Office Capacity Check
		if !s.backOffice.CheckCapacity(ctx, e.WorkspaceID) {
			fmt.Printf("⛔ [SENTINEL] Back-Office rejected workflow for Workspace [%s] Target [%s]: Internal capacity maxed out.\n", e.WorkspaceID, e.ID)
			return fmt.Errorf("back-office rejected workflow: capacity maxed out")
		}

		// 3. Reserve pipeline slot
		s.backOffice.RegisterPipeline(ctx, e.WorkspaceID, e.ID)

		defer s.backOffice.ReleasePipeline(ctx, e.WorkspaceID, e.ID)

		fmt.Printf("\n⚙️ [SENTINEL] Processing New Context! Workspace: [%s] Target ID: %s\n", e.WorkspaceID, e.ID)

		if s.apiKey == "" {
			fmt.Println("❌ [SENTINEL] Critical configuration failure: GEMINI_API_KEY environment variable is empty.")
			return fmt.Errorf("GEMINI_API_KEY env is empty")
		}

		// 4. Cloud Embedding Generation
		fmt.Println("⚙️ [SENTINEL] Generating high-dimensional vector embeddings...")
		vector, err := s.getEmbedding(ctx, e.Payload)
		if err != nil {
			fmt.Printf("❌ [SENTINEL] Embedding generation failed: %v\n", err)
			return err
		}

		// 5. Anchoring semantic truth in Qdrant
		metadata := map[string]any{
			"workspace_id": e.WorkspaceID,
			"url":          e.ID,
			"timestamp":    e.Timestamp,
		}
		err = s.vectorStore.UpsertVector(e.ID, vector, metadata)
		if err != nil {
			fmt.Printf("⚠️ [SENTINEL] Vector upsert failure: %v\n", err)
		}

		// 6. Strategic reasoning loop
		fmt.Println("⚙️ [SENTINEL] Engaging Cloud Neural Core for strategic writing...")

		safePayload := fmt.Sprintf("%v", e.Payload)
		if len(safePayload) > 6000 {
			safePayload = safePayload[:6000]
		}

		prompt := fmt.Sprintf(`You are an elite, outcome-oriented B2B Sales Director. Read the following business data and write a ruthless, 2-sentence cold email.
        
        RULES:
        1. Sentence 1: Hook them with specific insight from the data provided.
        2. Sentence 2: Call to action for a meeting.
        3. Do not use generic pleasantries.
        4. OUTPUT ONLY THE EMAIL TEXT.
        
        Website Data: %s`, safePayload)

		reqBody, _ := json.Marshal(GeminiGenerateRequest{
			Contents: []GeminiContent{
				{Parts: []GeminiPart{{Text: prompt}}},
			},
		})

		url := fmt.Sprintf("https://generativelanguage.googleapis.com/v1beta/models/gemini-3.5-flash:generateContent?key=%s", s.apiKey)
		req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(reqBody))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("❌ [SENTINEL] Neural Core connection error: %v\n", err)
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			fmt.Printf("❌ [SENTINEL] Neural Core error %d: %s\n", resp.StatusCode, string(body))
			return fmt.Errorf("neural core error: status %d", resp.StatusCode)
		}

		body, _ := io.ReadAll(resp.Body)
		var geminiResp GeminiGenerateResponse
		json.Unmarshal(body, &geminiResp)

		responseText := ""
		if len(geminiResp.Candidates) > 0 && len(geminiResp.Candidates[0].Content.Parts) > 0 {
			responseText = geminiResp.Candidates[0].Content.Parts[0].Text
		} else {
			fmt.Println("❌ [SENTINEL] Neural Core returned empty matrix response.")
			return fmt.Errorf("neural core returned empty response")
		}

		fmt.Printf("\n✅ [SENTINEL] Intelligence Generated for [%s]:\n\n%s\n\n", e.WorkspaceID, responseText)

		// 7. Anchor relationship data
		s.graphStore.Save(protocol.MemoryNode{
			EntityID:   e.ID,
			EntityType: "PROSPECT",
			Context:    fmt.Sprintf("[Workspace: %s] Strategic summary processed.", e.WorkspaceID),
		})

		// 8. Broadcast to Zoho EmailEngine via SENTINEL_TEXT_OUTPUT
		err = s.router.Publish(ctx, protocol.Event{
			WorkspaceID: e.WorkspaceID,
			ID:          e.ID, // The email recipient
			Source:      "SENTINEL_TEXT_OUTPUT",
			Payload:     responseText,
			Timestamp:   time.Now().Unix(),
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Sentinel) getEmbedding(ctx context.Context, text string) ([]float32, error) {
	embReq, _ := json.Marshal(GeminiEmbeddingRequest{
		Content: GeminiContent{
			Parts: []GeminiPart{{Text: text}},
		},
		OutputDimensionality: 768,
	})

	url := fmt.Sprintf("https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-001:embedContent?key=%s", s.apiKey)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(embReq))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API error %d: %s", resp.StatusCode, string(body))
	}

	body, _ := io.ReadAll(resp.Body)
	var embResp GeminiEmbeddingResponse
	err = json.Unmarshal(body, &embResp)
	if err != nil {
		return nil, err
	}

	return embResp.Embedding.Values, nil
}
