package agent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/JrDigitalHub/zeno-work-aoo/internal/memory"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/orchestrator"
	"github.com/JrDigitalHub/zeno-work-aoo/pkg/protocol"
)

type DiscoveryAgent struct {
	router *orchestrator.EventRouter
	DB     *memory.RelationalStore
	apiKey string
}

func NewDiscoveryAgent(r *orchestrator.EventRouter, db *memory.RelationalStore) *DiscoveryAgent {
	return &DiscoveryAgent{
		router: r,
		DB:     db,
		apiKey: os.Getenv("SERPER_API_KEY"), // 👉 Now securely loading a real Web Search API key
	}
}

// SerperResponse maps the JSON structure returned by the Google Search API wrapper
type SerperResponse struct {
	Organic []struct {
		Title string `json:"title"`
		Link  string `json:"link"`
	} `json:"organic"`
}

// ExtractLeads initiates a global web search, completely isolated by WorkspaceID
func (d *DiscoveryAgent) ExtractLeads(workspaceID string, query string) {
	fmt.Printf("🔍 [DISCOVERY] Global Web Hunt Initiated for Workspace [%s]. Target: \"%s\"\n", workspaceID, query)

	if d.apiKey == "" {
		fmt.Printf("❌ [DISCOVERY] API grid offline for [%s]: SERPER_API_KEY environment variable is empty.\n", workspaceID)
		return
	}

	// 1. Configure the Google Search Payload
	payload := map[string]interface{}{
		"q":   query,
		"num": 20, // Fetch top 20 organic results to filter down
	}
	jsonPayload, _ := json.Marshal(payload)

	req, _ := http.NewRequest("POST", "https://google.serper.dev/search", bytes.NewBuffer(jsonPayload))
	req.Header.Add("X-API-KEY", d.apiKey)
	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("❌ [DISCOVERY] Search grid offline for [%s]: %v\n", workspaceID, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("❌ [DISCOVERY] Search engine rejected query for [%s]. Status: %d, Response: %s\n", workspaceID, resp.StatusCode, string(body))
		return
	}

	var result SerperResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		fmt.Printf("❌ [DISCOVERY] Failed to decode telemetry for [%s]: %v\n", workspaceID, err)
		return
	}

	uniqueTargets := make(map[string]bool)
	var finalTargets []string

	// 2. Enterprise Domain Filtering
	for _, hit := range result.Organic {
		link := hit.Link
		if link != "" && strings.HasPrefix(link, "http") {

			// 👉 Filter out massive aggregators, social media, and junk directories
			lowerLink := strings.ToLower(link)
			if !strings.Contains(lowerLink, "ycombinator.com") &&
				!strings.Contains(lowerLink, "github.com") &&
				!strings.Contains(lowerLink, "medium.com") &&
				!strings.Contains(lowerLink, "nytimes.com") &&
				!strings.Contains(lowerLink, "youtube.com") &&
				!strings.Contains(lowerLink, "linkedin.com") &&
				!strings.Contains(lowerLink, "facebook.com") &&
				!strings.Contains(lowerLink, "twitter.com") &&
				!strings.Contains(lowerLink, "yelp.com") &&
				!strings.Contains(lowerLink, "clutch.co") {

				if !uniqueTargets[link] {
					uniqueTargets[link] = true
					finalTargets = append(finalTargets, link)
				}
			}
		}

		// Cap the pipeline at 5 high-quality, direct company domains per sweep to protect resources
		if len(finalTargets) >= 5 {
			break
		}
	}

	fmt.Printf("✅ [DISCOVERY] Global scan complete for Workspace [%s]. Acquired %d fresh target coordinates.\n", workspaceID, len(finalTargets))

	// 3. Stream the live targets into the ZENO Neural Bus
	for _, target := range finalTargets {
		fmt.Printf("📡 [DISCOVERY] Streaming target to Predator routing for [%s]: %s\n", workspaceID, target)

		d.router.Publish(protocol.Event{
			WorkspaceID: workspaceID,
			ID:          target,
			Source:      "DISCOVERY",
			Payload:     target,
			Timestamp:   time.Now().Unix(),
		})

		time.Sleep(1 * time.Second)
	}
}
