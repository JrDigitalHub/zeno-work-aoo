package agent

import (
	"fmt"
	"strings"

	"github.com/gocolly/colly/v2"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/orchestrator"
)

type Predator struct {
	router *orchestrator.EventRouter
}

func NewPredator(r *orchestrator.EventRouter) *Predator {
	return &Predator{
		router: r,
	}
}

// Hunt now accepts a live URL parameter
func (p *Predator) Hunt(targetURL string) {
	fmt.Printf("🦅 [PREDATOR] Sovereign extraction initiated. Breaching: %s\n", targetURL)

	// Initialize the Colly scraper
	c := colly.NewCollector()

	var extractedData []string

	// 1. Instruct the scraper what to look for (HTML elements)
	c.OnHTML("title", func(e *colly.HTMLElement) {
		extractedData = append(extractedData, "Title: "+e.Text)
	})

	c.OnHTML("h1", func(e *colly.HTMLElement) {
		extractedData = append(extractedData, "Header: "+e.Text)
	})

	// 2. What to do when the scrape is finished
	c.OnScraped(func(r *colly.Response) {
		fmt.Println("🦅 [PREDATOR] Extraction complete. Formatting payload...")
		
		// Join the scraped strings together
		finalPayload := strings.Join(extractedData, " | ")

		// Fire the LIVE data into the Event Router
		p.router.Publish(orchestrator.Event{
			ID:      "TGT_LIVE_01",
			Source:  "PREDATOR",
			Payload: finalPayload,
		})
	})

	// Handle network errors gracefully
	c.OnError(func(r *colly.Response, err error) {
		fmt.Printf("❌ [PREDATOR] Breach failed: %s\n", err)
	})

	// 3. Pull the trigger
	c.Visit(targetURL)
}