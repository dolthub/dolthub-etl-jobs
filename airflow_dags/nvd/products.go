package main

type AffectedProduct struct {
	cveID           string
	vendor          string
	product         string
	versionAffected string
	versionValue    string
}

func (ap AffectedProduct) headerLine() []string {
	return []string{
		"cve_id",
		"vendor",
		"product",
		"version",
	}
}

func (ap *AffectedProduct) toStringSlice() []string {
	var version string
	if ap.versionAffected == "=" {
		version = ap.versionValue
	} else {
		version = ap.versionAffected + " " + ap.versionValue
	}
	return []string{
		ap.cveID,
		ap.vendor,
		ap.product,
		version,
	}
}
