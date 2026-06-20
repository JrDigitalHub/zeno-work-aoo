package models

// TargetCompany holds the raw or scraped data of a business.
type TargetCompany struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Domain      string   `json:"domain"`
	Industry    string   `json:"industry"`
	CompanySize int      `json:"company_size"`
	TechStack   []string `json:"tech_stack"`
}

// QualificationResult holds the outcome of running a company through our ruleset.
type QualificationResult struct {
	TargetID    string   `json:"target_id"`
	IsQualified bool     `json:"is_qualified"`
	Score       int      `json:"score"`
	Reasons     []string `json:"reasons"`
}