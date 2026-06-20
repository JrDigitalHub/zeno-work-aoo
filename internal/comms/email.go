package comms

import (
	"crypto/tls"
	"fmt"
	"net/smtp"
	"strings"

	"github.com/JrDigitalHub/zeno-work-aoo/internal/memory"
)

type TargetCompany struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Domain      string   `json:"domain"`
	Email       string   `json:"email"`
	CompanySize int      `json:"company_size"`
	TechStack   []string `json:"tech_stack"`
}

type QualificationResult struct {
	IsQualified bool     `json:"is_qualified"`
	Score       int      `json:"score"`
	Reasons     []string `json:"reasons"`
}

type OutboundEmailPayload struct {
	Target  TargetCompany `json:"target"`
	Subject string        `json:"subject"`
	Body    string        `json:"body"`
}

type LeadDiscoveredEvent struct {
	Timestamp int64         `json:"timestamp"`
	Data      TargetCompany `json:"data"`
}

type EmailEngine struct {
	SMTPServer string
	SMTPPort   string
	Username   string
	Password   string
	SenderName string
	DB         *memory.RelationalStore // The Supabase connection!
}

// Updated initialization to require the database memory
func NewEmailEngine(server, port, username, password, senderName string, db *memory.RelationalStore) *EmailEngine {
	return &EmailEngine{
		SMTPServer: server,
		SMTPPort:   port,
		Username:   username,
		Password:   password,
		SenderName: senderName,
		DB:         db,
	}
}

func (e *EmailEngine) QualifyTarget(company TargetCompany) QualificationResult {
	score := 0
	var reasons []string

	if company.Email == "" || !strings.Contains(company.Email, "@") {
		return QualificationResult{IsQualified: false, Score: 0, Reasons: []string{"Invalid or missing email address"}}
	}

	if company.CompanySize >= 5 && company.CompanySize <= 100 {
		score += 40
		reasons = append(reasons, "Optimal SME sizing bracket")
	} else if company.CompanySize > 100 {
		score += 20
		reasons = append(reasons, "Enterprise scale tier")
	} else {
		reasons = append(reasons, "Sub-optimal scale (<5)")
	}

	for _, tech := range company.TechStack {
		t := strings.ToLower(tech)
		if t == "react" || t == "typescript" || t == "node.js" || t == "go" {
			score += 20
			reasons = append(reasons, "Matches priority tech infrastructure: "+tech)
		}
	}

	isQualified := score >= 40

	return QualificationResult{
		IsQualified: isQualified,
		Score:       score,
		Reasons:     reasons,
	}
}

func (e *EmailEngine) SendOutbound(to, subject, htmlBody string) error {
	headers := make(map[string]string)
	headers["From"] = fmt.Sprintf("\"%s\" <%s>", e.SenderName, e.Username)
	headers["To"] = to
	headers["Subject"] = subject
	headers["MIME-Version"] = "1.0"
	headers["Content-Type"] = "text/html; charset=\"utf-8\""

	message := ""
	for k, v := range headers {
		message += fmt.Sprintf("%s: %s\r\n", k, v)
	}
	message += "\r\n" + htmlBody

	auth := smtp.PlainAuth("", e.Username, e.Password, e.SMTPServer)
	addr := fmt.Sprintf("%s:%s", e.SMTPServer, e.SMTPPort)

	tlsConfig := &tls.Config{InsecureSkipVerify: false, ServerName: e.SMTPServer}

	conn, err := tls.Dial("tcp", addr, tlsConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to TLS server: %v", err)
	}
	defer conn.Close()

	client, err := smtp.NewClient(conn, e.SMTPServer)
	if err != nil {
		return fmt.Errorf("failed to create SMTP client: %v", err)
	}
	defer client.Quit()

	if err = client.Auth(auth); err != nil {
		return fmt.Errorf("SMTP authentication failed: %v", err)
	}
	if err = client.Mail(e.Username); err != nil {
		return fmt.Errorf("failed setting sender envelope: %v", err)
	}
	if err = client.Rcpt(to); err != nil {
		return fmt.Errorf("failed setting recipient envelope: %v", err)
	}

	w, err := client.Data()
	if err != nil {
		return fmt.Errorf("failed creating data writer stream: %v", err)
	}
	_, err = w.Write([]byte(message))
	if err != nil {
		return fmt.Errorf("failed writing body payload: %v", err)
	}
	err = w.Close()
	if err != nil {
		return fmt.Errorf("failed closing message data writer: %v", err)
	}

	return nil
}

func (e *EmailEngine) React(event interface{}) {
	// Inspect incoming broad structural events passing through the hub using a type switch
	switch ev := event.(type) {

	case LeadDiscoveredEvent:
		fmt.Printf("🔍 [EmailEngine] Intercepted LeadDiscoveredEvent for company: %s\n", ev.Data.Name)

		// Run evaluation logic check before touching outbound pipes
		assessment := e.QualifyTarget(ev.Data)
		if !assessment.IsQualified {
			fmt.Printf("⚠️ [EmailEngine] Lead %s failed qualification criteria. Score: %d. Reasons: %v. Execution halted.\n",
				ev.Data.Name, assessment.Score, assessment.Reasons)
			return
		}

		fmt.Printf("🎯 [EmailEngine] Lead %s PASSED qualification setup (Score: %d). Awaiting system composition event...\n",
			ev.Data.Name, assessment.Score)

	case OutboundEmailPayload:
		fmt.Printf("✉️ [EmailEngine] Intercepted outbound execution task targeting: %s\n", ev.Target.Email)

		// Run a secondary qualification check immediately before actual delivery to verify state safety
		assessment := e.QualifyTarget(ev.Target)
		if !assessment.IsQualified {
			fmt.Printf("🛑 [EmailEngine] Pre-flight security block: Target %s fails safety parameters. Aborting transmission.\n", ev.Target.Email)
			return
		}

		// Direct handshake execution to Zoho SMTP
		err := e.SendOutbound(ev.Target.Email, ev.Subject, ev.Body)
		if err != nil {
			fmt.Printf("❌ [EmailEngine] Failed to dispatch outbound transmission to %s: %v\n", ev.Target.Email, err)
		} else {
			fmt.Printf("🚀 [EmailEngine] Success! Outbound message cleanly delivered to %s from system node.\n", ev.Target.Email)
		}

	default:
		// Ignore unhandled event structures floating inside the runtime bus
	}
}
