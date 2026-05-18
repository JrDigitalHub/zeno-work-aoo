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

	// Use a map to isolate text extraction to its specific subpage URL
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

	// Fires per individual subpage completion
	c.OnScraped(func(r *colly.Response) {
		currentSubpageURL := r.Request.URL.String()
		
		// Grab only the text collected for THIS specific subpage
		texts := pageContentMap[currentSubpageURL]
		fullCorpus := strings.Join(texts, " | ")
		
		if len(fullCorpus) > 2000 {
			fullCorpus = fullCorpus[:2000]
		}

		fmt.Printf("🦅 [PREDATOR] Subpage indexing complete: [%s]\n", currentSubpageURL)

		// Publish the specific subpage URL as the unique Event ID
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