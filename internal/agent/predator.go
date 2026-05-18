package agent

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/JrDigitalHub/zeno-work-aoo/internal/orchestrator"
	"github.com/JrDigitalHub/zeno-work-aoo/pkg/protocol"
	"github.com/gocolly/colly/v2"
)

type Predator struct {
	router *orchestrator.EventRouter
}

func NewPredator(r *orchestrator.EventRouter) *Predator {
	return &Predator{router: r}
}

// React allows the Predator to listen to the Event Router autonomously
func (p *Predator) React(e protocol.Event) {
	// If the event source is DISCOVERY, automatically hunt the URL in the payload
	if e.Source == "DISCOVERY" {
		fmt.Printf("🦅 [PREDATOR] Intercepted Discovery telemetry. Arming systems for: %s\n", e.Payload)
		go p.Hunt(e.Payload)
	}
}

func (p *Predator) Hunt(targetURL string) {
	fmt.Printf("🦅 [PREDATOR] Deep crawling initialized for domain: %s\n", targetURL)

	parsedURL, err := url.Parse(targetURL)
	if err != nil {
		fmt.Printf("❌ [PREDATOR] Malformed seed URL: %v\n", err)
		return
	}
	allowedDomain := parsedURL.Host

	c := colly.NewCollector(
		colly.AllowedDomains(allowedDomain),
		colly.MaxDepth(2),
	)

	c.Limit(&colly.LimitRule{
		DomainGlob:  "*",
		Parallelism: 2,
		Delay:       1 * time.Second,
	})

	pageContentMap := make(map[string][]string)

	c.OnHTML("title, h1, h2, p", func(e *colly.HTMLElement) {
		currentURL := e.Request.URL.String()
		text := strings.TrimSpace(e.Text)
		if text != "" {
			pageContentMap[currentURL] = append(pageContentMap[currentURL], text)
		}
	})

	c.OnHTML("a[href]", func(e *colly.HTMLElement) {
		link := e.Attr("href")
		absoluteURL := e.Request.AbsoluteURL(link)
		if absoluteURL != "" {
			e.Request.Visit(absoluteURL)
		}
	})

	c.OnScraped(func(r *colly.Response) {
		currentSubpageURL := r.Request.URL.String()
		texts := pageContentMap[currentSubpageURL]
		fullCorpus := strings.Join(texts, " | ")
		
		if len(fullCorpus) > 2000 {
			fullCorpus = fullCorpus[:2000]
		}

		fmt.Printf("🦅 [PREDATOR] Subpage indexing complete: [%s]\n", currentSubpageURL)

		p.router.Publish(protocol.Event{
			ID:        currentSubpageURL, 
			Source:    "PREDATOR",
			Payload:   fullCorpus,
			Timestamp: time.Now().Unix(),
		})
	})

	c.OnError(func(r *colly.Response, err error) {
		fmt.Printf("⚠️ [PREDATOR] Resource skipped at %s: %v\n", r.Request.URL, err)
	})

	c.Visit(targetURL)
}