package agent

import (
	"bytes"
	"encoding/json"
	"fmt"
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
		apiKey: os.Getenv("SERPER_API_KEY"),
	}
}

// Structs for parsing different Serper API responses
type SerperPlacesResponse struct {
	Places []struct {
		Title       string `json:"title"`
		Address     string `json:"address"`
		PhoneNumber string `json:"phoneNumber"`
		Website     string `json:"website"`
	} `json:"places"`
}

type SerperOrganicResponse struct {
	Organic []struct {
		Title   string `json:"title"`
		Link    string `json:"link"`
		Snippet string `json:"snippet"`
	} `json:"organic"`
}

// 👉 The Master Routing Hub
// We now pass a 'mode' string to dictate the hunting strategy
func (d *DiscoveryAgent) ExtractLeads(workspaceID string, query string, mode string) {
	if d.apiKey == "" {
		fmt.Printf("❌ [DISCOVERY] API grid offline for [%s]: SERPER_API_KEY missing.\n", workspaceID)
		return
	}

	switch mode {
	case "SOCIAL_HUNTER":
		d.runSocialHunter(workspaceID, query)
	case "DEEP_CRAWLER":
		d.runDeepCrawler(workspaceID, query)
	case "LOCAL_SCANNER":
		fallthrough
	default:
		d.runLocalScanner(workspaceID, query)
	}
}

// ---------------------------------------------------------
// VECTOR 1: LOCAL GRID SCANNER (Physical SMEs)
// ---------------------------------------------------------
func (d *DiscoveryAgent) runLocalScanner(workspaceID, query string) {
	fmt.Printf("📍 [DISCOVERY] Local Grid Scanner Engaged. Target: \"%s\"\n", query)

	payload := map[string]interface{}{"q": query}
	jsonPayload, _ := json.Marshal(payload)

	req, _ := http.NewRequest("POST", "https://google.serper.dev/places", bytes.NewBuffer(jsonPayload))
	req.Header.Add("X-API-KEY", d.apiKey)
	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		fmt.Printf("❌ [DISCOVERY] Local Scanner failed: %v\n", err)
		return
	}
	defer resp.Body.Close()

	var result SerperPlacesResponse
	json.NewDecoder(resp.Body).Decode(&result)

	count := 0
	for _, hit := range result.Places {
		if hit.Website != "" && strings.HasPrefix(hit.Website, "http") {
			enrichedPayload := fmt.Sprintf("COMPANY: %s | WEBSITE: %s | PHONE: %s", hit.Title, hit.Website, hit.PhoneNumber)
			
			d.router.Publish(protocol.Event{
				WorkspaceID: workspaceID,
				ID:          hit.Website,
				Source:      "DISCOVERY",
				Payload:     enrichedPayload,
				Timestamp:   time.Now().Unix(),
			})
			count++
			time.Sleep(500 * time.Millisecond) // Throttled transmission
		}
		if count >= 10 { break }
	}
	fmt.Printf("✅ [DISCOVERY] Local scan complete. Acquired %d targets.\n", count)
}

// ---------------------------------------------------------
// VECTOR 2: X-RAY SOCIAL HUNTER (Individuals / LinkedIn)
// ---------------------------------------------------------
func (d *DiscoveryAgent) runSocialHunter(workspaceID, query string) {
	// 👉 Advanced Google Dorking: Force Serper to only return LinkedIn profiles of founders
	dorkQuery := fmt.Sprintf("site:linkedin.com/in/ \"Founder\" OR \"CEO\" %s", query)
	fmt.Printf("👤 [DISCOVERY] X-Ray Social Hunter Engaged. Dorking: \"%s\"\n", dorkQuery)

	payload := map[string]interface{}{
		"q":   dorkQuery,
		"num": 10,
	}
	jsonPayload, _ := json.Marshal(payload)

	req, _ := http.NewRequest("POST", "https://google.serper.dev/search", bytes.NewBuffer(jsonPayload))
	req.Header.Add("X-API-KEY", d.apiKey)
	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		fmt.Printf("❌ [DISCOVERY] Social Hunter failed: %v\n", err)
		return
	}
	defer resp.Body.Close()

	var result SerperOrganicResponse
	json.NewDecoder(resp.Body).Decode(&result)

	count := 0
	for _, hit := range result.Organic {
		if strings.Contains(hit.Link, "linkedin.com/in/") {
			// Clean the title (usually looks like "John Doe - Founder - Acme Corp | LinkedIn")
			cleanTitle := strings.Split(hit.Title, " | ")[0]
			enrichedPayload := fmt.Sprintf("PERSON: %s | URL: %s | BIO: %s", cleanTitle, hit.Link, hit.Snippet)

			d.router.Publish(protocol.Event{
				WorkspaceID: workspaceID,
				ID:          hit.Link,
				Source:      "DISCOVERY",
				Payload:     enrichedPayload,
				Timestamp:   time.Now().Unix(),
			})
			count++
			time.Sleep(500 * time.Millisecond)
		}
	}
	fmt.Printf("✅ [DISCOVERY] X-Ray scan complete. Acquired %d key decision makers.\n", count)
}

// ---------------------------------------------------------
// VECTOR 3: DEEP-WATER CRAWLER (Struggling B2B Companies)
// ---------------------------------------------------------
func (d *DiscoveryAgent) runDeepCrawler(workspaceID, query string) {
	fmt.Printf("🌊 [DISCOVERY] Deep-Water Crawler Engaged. Target: \"%s\"\n", query)

	// 👉 The Pagination Trick: We skip the top 30 results (Pages 1-3) to bypass SEO giants
	payload := map[string]interface{}{
		"q":    query,
		"page": 4,  // Start on page 4
		"num":  20, // Grab 20 results
	}
	jsonPayload, _ := json.Marshal(payload)

	req, _ := http.NewRequest("POST", "https://google.serper.dev/search", bytes.NewBuffer(jsonPayload))
	req.Header.Add("X-API-KEY", d.apiKey)
	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		fmt.Printf("❌ [DISCOVERY] Deep Crawler failed: %v\n", err)
		return
	}
	defer resp.Body.Close()

	var result SerperOrganicResponse
	json.NewDecoder(resp.Body).Decode(&result)

	uniqueTargets := make(map[string]bool)
	count := 0

	for _, hit := range result.Organic {
		link := hit.Link
		if link != "" && strings.HasPrefix(link, "http") {
			lowerLink := strings.ToLower(link)
			
			// Aggressive filtering: We only want independent company domains
			if !strings.Contains(lowerLink, "ycombinator.com") &&
				!strings.Contains(lowerLink, "linkedin.com") &&
				!strings.Contains(lowerLink, "facebook.com") &&
				!strings.Contains(lowerLink, "clutch.co") &&
				!strings.Contains(lowerLink, "yelp.com") &&
				!strings.Contains(lowerLink, "medium.com") {

				if !uniqueTargets[link] {
					uniqueTargets[link] = true
					
					enrichedPayload := fmt.Sprintf("COMPANY: %s | WEBSITE: %s | CONTEXT: %s", hit.Title, link, hit.Snippet)
					
					d.router.Publish(protocol.Event{
						WorkspaceID: workspaceID,
						ID:          link,
						Source:      "DISCOVERY",
						Payload:     enrichedPayload,
						Timestamp:   time.Now().Unix(),
					})
					count++
					time.Sleep(500 * time.Millisecond)
				}
			}
		}
		if count >= 10 { break }
	}
	fmt.Printf("✅ [DISCOVERY] Deep-Water crawl complete. Acquired %d low-visibility SME domains.\n", count)
}