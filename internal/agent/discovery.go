package agent

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/JrDigitalHub/zeno-work-aoo/internal/memory"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/orchestrator"
	"github.com/JrDigitalHub/zeno-work-aoo/pkg/protocol"
)

type DiscoveryAgent struct {
	router *orchestrator.EventRouter
	DB     *memory.RelationalStore // 👉 Injected the database so it matches main.go
}

// Updated to accept the database connection during boot
func NewDiscoveryAgent(r *orchestrator.EventRouter, db *memory.RelationalStore) *DiscoveryAgent {
	return &DiscoveryAgent{
		router: r,
		DB:     db,
	}
}

// AlgoliaResponse maps the JSON structure returned by the Hacker News API
type AlgoliaResponse struct {
	Hits []struct {
		URL string `json:"url"`
	} `json:"hits"`
}

// ExtractLeads initiates a live API scan bypassing HTML bot-blockers, completely isolated by WorkspaceID
func (d *DiscoveryAgent) ExtractLeads(workspaceID string, query string) {
	fmt.Printf("🔍 [DISCOVERY] Live API Hunt Initiated for Workspace [%s]. Target Sector: \"%s\"\n", workspaceID, query)
	fmt.Println("🔍 [DISCOVERY] Bypassing HTML walls. Tapping into Hacker News Algolia JSON interface...")

	// Encode the query and hit the API
	encodedQuery := url.QueryEscape(query)
	apiURL := fmt.Sprintf("https://hn.algolia.com/api/v1/search?query=%s&tags=story", encodedQuery)

	resp, err := http.Get(apiURL)
	if err != nil {
		fmt.Printf("❌ [DISCOVERY] API grid offline for [%s]: %v\n", workspaceID, err)
		return
	}
	defer resp.Body.Close()

	var result AlgoliaResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		fmt.Printf("❌ [DISCOVERY] Failed to decode telemetry for [%s]: %v\n", workspaceID, err)
		return
	}

	uniqueTargets := make(map[string]bool)
	var finalTargets []string

	// Filter and validate the raw URLs
	for _, hit := range result.Hits {
		link := hit.URL
		if link != "" &&
			strings.HasPrefix(link, "http") &&
			!strings.Contains(link, "ycombinator.com") &&
			!strings.Contains(link, "github.com") &&
			!strings.Contains(link, "medium.com") &&
			!strings.Contains(link, "nytimes.com") &&
			!strings.Contains(link, "youtube.com") {

			if !uniqueTargets[link] {
				// Note: Since this agent only handles URLs right now, we rely on the
				// downstream EmailEngine to check if the target's EMAIL exists in the DB before sending!
				uniqueTargets[link] = true
				finalTargets = append(finalTargets, link)
			}
		}

		if len(finalTargets) >= 4 {
			break
		}
	}

	fmt.Printf("✅ [DISCOVERY] Live API scan complete for Workspace [%s]. Acquired %d fresh target coordinates.\n", workspaceID, len(finalTargets))

	// Stream the live targets into the ZENO Neural Bus
	for _, target := range finalTargets {
		fmt.Printf("📡 [DISCOVERY] Streaming live target to router for [%s]: %s\n", workspaceID, target)

		d.router.Publish(protocol.Event{
			WorkspaceID: workspaceID, // 👉 CRITICAL: This ensures downstream agents know who owns this lead
			ID:          target,
			Source:      "DISCOVERY",
			Payload:     target,
			Timestamp:   time.Now().Unix(),
		})

		time.Sleep(1 * time.Second)
	}
}