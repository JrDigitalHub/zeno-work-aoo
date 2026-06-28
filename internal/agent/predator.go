package agent

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
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
	if event.Source != "DISCOVERY" {
		return
	}

	fmt.Printf("🦅 [PREDATOR] Target locked for Workspace [%s]. Parsing enriched payload...\n", event.WorkspaceID)

	urlTarget := p.extractURL(event.Payload)
	if urlTarget == "" {
		fmt.Printf("⚠️ [PREDATOR] No valid URL found in payload for Workspace [%s]. Aborting strike.\n", event.WorkspaceID)
		return
	}

	fmt.Printf("🦅 [PREDATOR] Initiating Deep-Crawl on %s...\n", urlTarget)

	// 👉 ENTERPRISE UPGRADE: Proxy Rotation
	proxyStr := os.Getenv("PREDATOR_PROXY_URL")
	var transport *http.Transport
	if proxyStr != "" {
		proxyURL, _ := url.Parse(proxyStr)
		transport = &http.Transport{Proxy: http.ProxyURL(proxyURL)}
		fmt.Printf("🦅 [PREDATOR] Stealth Mode Engaged: Routing via Proxy for [%s]\n", event.WorkspaceID)
	} else {
		transport = &http.Transport{} // Fallback to local server IP if no proxy is set
	}

	client := &http.Client{
		Timeout:   15 * time.Second,
		Transport: transport,
	}

	req, err := http.NewRequest("GET", urlTarget, nil)
	if err != nil {
		fmt.Printf("❌ [PREDATOR] Failed to build HTTP request for %s: %v\n", urlTarget, err)
		return
	}

	// Disguise the agent as a standard Chrome browser
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36")

	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("❌ [PREDATOR] Connection refused by %s: %v\n", urlTarget, err)
		return
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("❌ [PREDATOR] Failed to read HTML DOM from %s: %v\n", urlTarget, err)
		return
	}
	htmlContent := string(bodyBytes)

	// Extract the exact Email Address using Regex
	email := p.scrapeEmail(htmlContent)
	if email == "" {
		fmt.Printf("⚠️ [PREDATOR] Crawl completed on %s. No public email address exposed in DOM.\n", urlTarget)
		return
	}

	fmt.Printf("🎯 [PREDATOR] SUCCESS! Email extracted from %s: %s\n", urlTarget, email)

	finalPayload := fmt.Sprintf("%s | EMAIL: %s", event.Payload, email)

	p.router.Publish(protocol.Event{
		WorkspaceID: event.WorkspaceID,
		ID:          email,
		Source:      "PREDATOR",
		Target:      "SENTINEL",
		Payload:     finalPayload,
		Timestamp:   time.Now().Unix(),
	})
}

func (p *Predator) extractURL(payload string) string {
	parts := strings.Split(payload, " | ")
	for _, part := range parts {
		if strings.HasPrefix(part, "WEBSITE: ") {
			return strings.TrimPrefix(part, "WEBSITE: ")
		} else if strings.HasPrefix(part, "URL: ") {
			return strings.TrimPrefix(part, "URL: ")
		}
	}
	re := regexp.MustCompile(`https?://[^\s]+`)
	return re.FindString(payload)
}

func (p *Predator) scrapeEmail(html string) string {
	re := regexp.MustCompile(`[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}`)
	matches := re.FindAllString(html, -1)

	for _, match := range matches {
		lowerMatch := strings.ToLower(match)
		if !strings.Contains(lowerMatch, "sentry.io") &&
			!strings.Contains(lowerMatch, "example.com") &&
			!strings.Contains(lowerMatch, "wixpress") &&
			!strings.HasSuffix(lowerMatch, ".png") {
			return match
		}
	}
	return ""
}