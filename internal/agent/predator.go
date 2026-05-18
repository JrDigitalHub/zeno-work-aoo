package agent

import (
	"fmt"
	"strings"
	"time"

	"github.com/gocolly/colly/v2"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/orchestrator"
	"github.com/JrDigitalHub/zeno-work-aoo/pkg/protocol"
)

type Predator struct {
	router *orchestrator.EventRouter
}

func NewPredator(r *orchestrator.EventRouter) *Predator {
	return &Predator{
		router: r,
	}
}

func (p *Predator) Hunt(targetURL string) {
	fmt.Printf("🦅 [PREDATOR] Sovereign extraction initiated. Breaching: %s\n", targetURL)

	c := colly.NewCollector()
	var extractedData []string

	c.OnHTML("title", func(e *colly.HTMLElement) {
		extractedData = append(extractedData, "Title: "+e.Text)
	})

	c.OnScraped(func(r *colly.Response) {
		fmt.Println("🦅 [PREDATOR] Extraction complete. Formatting payload...")
		
		finalPayload := strings.Join(extractedData, " | ")

		// Fire the data using the official protocol
		p.router.Publish(protocol.Event{
			ID:        targetURL, // We use the URL as the unique Graph ID
			Source:    "PREDATOR",
			Payload:   finalPayload,
			Timestamp: time.Now().Unix(),
		})
	})

	c.OnError(func(r *colly.Response, err error) {
		fmt.Printf("❌ [PREDATOR] Breach failed: %s\n", err)
	})

	c.Visit(targetURL)
}