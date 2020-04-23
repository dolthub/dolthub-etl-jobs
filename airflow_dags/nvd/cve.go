package main

type CVE struct {
	id                  string
	description         string
	problemType         string
	exploitabilityScore string
	impactScore         string
	datePublished       string
	dateLastModified    string
}

func (cve *CVE) toStringSlice() []string {
	return []string{
		cve.id,
		cve.description,
		cve.problemType,
		cve.exploitabilityScore,
		cve.impactScore,
		cve.datePublished,
		cve.dateLastModified,
	}
}

func (cve CVE) headerLine() []string {
	return []string{
		"cve_id",
		"description",
		"problem_type",
		"exploitability_score",
		"impact_score",
		"date_published",
		"date_last_modified",
	}
}
