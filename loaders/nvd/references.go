package main

import "fmt"

type CVEReference struct {
	refID     int
	cveID     string
	Name      string
	Refsource string
	URL       string
}

func (r CVEReference) headerLine() []string {
	return []string{
		"reference_id",
		"cve_id",
		"name",
		"refsource",
		"url",
	}
}

func (r *CVEReference) toStringSlice() []string {
	return []string{
		fmt.Sprintf("%d", r.refID),
		r.cveID,
		r.Name,
		r.Refsource,
		r.URL,
	}
}

type CVEReferenceTag struct {
	refID int
	tag   string
}

func (t CVEReferenceTag) headerLine() []string {
	return []string{
		"reference_id",
		"tag",
	}
}

func (t *CVEReferenceTag) toStringSlice() []string {
	return []string{
		fmt.Sprintf("%d", t.refID),
		t.tag,
	}
}
