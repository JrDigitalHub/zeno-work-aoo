package qualification

import "github.com/JrDigitalHub/zeno-work-aoo/pkg/models"

// EvaluateTarget applies your proprietary business rules to grade a lead.
func EvaluateTarget(company models.TargetCompany) models.QualificationResult {
	score := 0
	var reasons []string

	// Rule 1: Target Ideal Customer Profile (ICP) Size
	if company.CompanySize >= 10 && company.CompanySize <= 100 {
		score += 40
		reasons = append(reasons, "Optimal SME size bracket (10-100)")
	} else if company.CompanySize > 100 {
		score += 20
		reasons = append(reasons, "Enterprise scale target")
	} else {
		reasons = append(reasons, "Sub-optimal scale (<10)")
	}

	// Rule 2: Technical Synergy Alignment
	for _, tech := range company.TechStack {
		if tech == "React" || tech == "Node.js" || tech == "TypeScript" || tech == "Go" {
			score += 15
			reasons = append(reasons, "Uses target developer stack: "+tech)
		}
	}

	// Qualification threshold barrier
	isQualified := score >= 50

	return models.QualificationResult{
		TargetID:    company.ID,
		IsQualified: isQualified,
		Score:       score,
		Reasons:     reasons,
	}
}
