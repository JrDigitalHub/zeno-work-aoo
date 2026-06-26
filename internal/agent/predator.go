package agent

import (
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/JrDigitalHub/zeno-work-aoo/internal/orchestrator"
	"github.com/JrDigitalHub/zeno-work-aoo/pkg/protocol"
)

type Predator struct {
	router *orchestrator.EventRouter
}

func NewPredator(router *orchestrator.EventRouter) *Predator {
	return &Predator{
		router: router,
	}
}

// React listens to the Event Bus for targets identified by the Discovery Agent
func (p *Predator) React(event protocol.Event) {
	// Predator only acts on raw leads found by Discovery
	if event.Source != "DISCOVERY" {
		return
	}

	fmt.Printf("🦅 [PREDATOR] Target locked for Workspace [%s]. Parsing enriched payload...\n", event.WorkspaceID)

	// 1. Extract the URL from the enriched payload
	// Discovery sends payloads like: "COMPANY: Acme | WEBSITE: https://acme.com | PHONE: 123"
	url := p.extractURL(event.Payload)
	if url == "" {
		fmt.Printf("⚠️ [PREDATOR] No valid URL found in payload for Workspace [%s]. Aborting strike.\n", event.WorkspaceID)
		return
	}

	fmt.Printf("🦅 [PREDATOR] Initiating Deep-Crawl on %s...\n", url)

	// 2. Autonomously fetch the HTML DOM of the target
	client := &http.Client{Timeout: 15 * time.Second}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		fmt.Printf("❌ [PREDATOR] Failed to build HTTP request for %s: %v\n", url, err)
		return
	}

	// Disguise the agent as a standard Chrome browser to bypass basic anti-bot firewalls
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36")

	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("❌ [PREDATOR] Connection refused by %s: %v\n", url, err)
		return
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("❌ [PREDATOR] Failed to read HTML DOM from %s: %v\n", url, err)
		return
	}
	htmlContent := string(bodyBytes)

	// 3. Extract the exact Email Address using Regex
	email := p.scrapeEmail(htmlContent)
	if email == "" {
		fmt.Printf("⚠️ [PREDATOR] Crawl completed on %s. No public email address exposed in DOM.\n", url)
		return
	}

	fmt.Printf("🎯 [PREDATOR] SUCCESS! Email extracted from %s: %s\n", url, email)

	// 4. Repackage the data and stream it to Sentinel for email drafting
	// We combine the original context with the newly discovered email
	finalPayload := fmt.Sprintf("%s | EMAIL: %s", event.Payload, email)

	p.router.Publish(protocol.Event{
		WorkspaceID: event.WorkspaceID,
		ID:          email, // The email is now the primary ID for the rest of the pipeline
		Source:      "PREDATOR",
		Target:      "SENTINEL",
		Payload:     finalPayload,
		Timestamp:   time.Now().Unix(),
	})
}

// extractURL parses the Discovery payload to find the http link
func (p *Predator) extractURL(payload string) string {
	parts := strings.Split(payload, " | ")
	for _, part := range parts {
		if strings.HasPrefix(part, "WEBSITE: ") {
			return strings.TrimPrefix(part, "WEBSITE: ")
		} else if strings.HasPrefix(part, "URL: ") {
			return strings.TrimPrefix(part, "URL: ")
		}
	}
	
	// Fallback regex if formatting was missed
	re := regexp.MustCompile(`https?://[^\s]+`)
	return re.FindString(payload)
}

// scrapeEmail runs a standard RFC 5322 regex against the raw HTML DOM
func (p *Predator) scrapeEmail(html string) string {
	re := regexp.MustCompile(`[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}`)
	matches := re.FindAllString(html, -1)

	for _, match := range matches {
		// Filter out common junk emails found in web dev templates
		lowerMatch := strings.ToLower(match)
		if !strings.Contains(lowerMatch, "sentry.io") &&
			!strings.Contains(lowerMatch, "example.com") &&
			!strings.Contains(lowerMatch, "wixpress") &&
			!strings.HasSuffix(lowerMatch, ".png") {
			return match // Return the first valid business email found
		}
	}
	return ""
}