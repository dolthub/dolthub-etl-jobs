package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/facebookincubator/nvdtools/cvefeed/nvd/schema"
	"github.com/facebookincubator/nvdtools/providers/nvd"
)

const (
	downloadDir = "/tmp/nvd"

	languageEnglish = "en"

	tableCVE = "CVE"
	tableCVSS2 = "CVSS2"
	tableCVSS3 = "CVSS3"
	tableReferences = "references"
	tableReferenceTags = "reference_tags"
	tableProducts = "affected_products"
)

func main() {

	var (
		timeout   time.Duration = 5*time.Minute
		cvefeed   nvd.CVE = 4
		cpefeed   nvd.CPE = 0
	)

	dfs := nvd.Sync{
		Feeds:    []nvd.Syncer{cvefeed, cpefeed},
		Source:   nvd.NewSourceConfig(),
		LocalDir: downloadDir,
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	err := dfs.Do(ctx)

	if err != nil {
		fmt.Println(err)
		return
	}

	files, err := ioutil.ReadDir(downloadDir)

	if err != nil {
		fmt.Println(err)
		return
	}

	var feeds []*schema.NVDCVEFeedJSON10
	for _, f := range files {
		fmt.Println(f.Name())
		if !strings.Contains(f.Name(), "json") {
			continue
		}
		feed, err := ParseJSONFeed(fmt.Sprintf("%s/%s", downloadDir, f.Name()))
		if err != nil {
			fmt.Println(err)
			return
		}
		feeds = append(feeds, feed)
		//break
	}

	CveFile, err := os.Create(fmt.Sprintf("%s.csv", tableCVE))
	if err != nil {
		fmt.Println(err); return
	}
	defer CveFile.Close()
	Cvss2File, err := os.Create(fmt.Sprintf("%s.csv", tableCVSS2))
	if err != nil {
		fmt.Println(err); return
	}
	defer Cvss2File.Close()
	Cvss3File, err := os.Create(fmt.Sprintf("%s.csv", tableCVSS3))
	if err != nil {
		fmt.Println(err); return
	}
	defer Cvss3File.Close()
	RefFile, err := os.Create(fmt.Sprintf("%s.csv", tableReferences))
	if err != nil {
		fmt.Println(err); return
	}
	defer RefFile.Close()
	TagFile, err := os.Create(fmt.Sprintf("%s.csv", tableReferenceTags))
	if err != nil {
		fmt.Println(err); return
	}
	defer TagFile.Close()
	ProductFile, err := os.Create(fmt.Sprintf("%s.csv", tableProducts))
	if err != nil {
		fmt.Println(err); return
	}
	defer ProductFile.Close()

	tw := TableWriter{
		CveWr:   csv.NewWriter(CveFile),
		Cvss2Wr: csv.NewWriter(Cvss2File),
		Cvss3Wr: csv.NewWriter(Cvss3File),
		RefWr:   csv.NewWriter(RefFile),
		TagWr:   csv.NewWriter(TagFile),
		ProdWr:  csv.NewWriter(ProductFile),
	}

	err = NormalizeCVEs(languageEnglish, &tw, feeds...)

	if err != nil {
		fmt.Println(err)
		return
	}

	tw.CveWr.Flush()
	tw.RefWr.Flush()
	tw.TagWr.Flush()
	tw.Cvss2Wr.Flush()
	tw.Cvss3Wr.Flush()
	tw.ProdWr.Flush()
}

// ParseJSON parses JSON dictionary from NVD vulnerability feed
func ParseJSONFeed(path string) (*schema.NVDCVEFeedJSON10, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("dictionary: failed to load feed %s: \n %v", path, err)
	}
	defer f.Close()

	reader, err := setupReader(f)
	if err != nil {
		return nil, fmt.Errorf("can't setup reader: %v", err)
	}
	defer reader.Close()

	var feed schema.NVDCVEFeedJSON10
	if err := json.NewDecoder(reader).Decode(&feed); err != nil {
		return nil, err
	}

	return &feed, nil
}

func setupReader(in io.Reader) (src io.ReadCloser, err error) {
	r := bufio.NewReader(in)
	header, err := r.Peek(2)
	if err != nil {
		return nil, err
	}
	// assume plain text first
	src = ioutil.NopCloser(r)
	// replace with gzip.Reader if gzip'ed
	if header[0] == 0x1f && header[1] == 0x8b { // file is gzip'ed
		zr, err := gzip.NewReader(r)
		if err != nil {
			return nil, err
		}
		src = zr
	}
	// TODO: maybe support .zip
	return src, nil
}

type TableWriter struct {
	CveWr   *csv.Writer
	Cvss2Wr *csv.Writer
	Cvss3Wr *csv.Writer
	RefWr   *csv.Writer
	TagWr   *csv.Writer
	ProdWr  *csv.Writer
}

func (tw *TableWriter) writeHeaders() (err error) {
	err = tw.CveWr.Write(CVE{}.headerLine())
	if err != nil {
		return err
	}
	err = tw.Cvss2Wr.Write(CVSS2{}.headerLine())
	if err != nil {
		return err
	}
	err = tw.Cvss3Wr.Write(CVSS3{}.headerLine())
	if err != nil {
		return err
	}
	err = tw.RefWr.Write(CVEReference{}.headerLine())
	if err != nil {
		return err
	}
	err = tw.TagWr.Write(CVEReferenceTag{}.headerLine())
	if err != nil {
		return err
	}
	err = tw.ProdWr.Write(AffectedProduct{}.headerLine())
	if err != nil {
		return err
	}
	return nil
}

func NormalizeCVEs(lang string, tw *TableWriter, feeds ...*schema.NVDCVEFeedJSON10) (err error) {
	referenceID := 0

	err = tw.writeHeaders()
	if err != nil {
		return err
	}

	for idx, feed := range feeds {
		fmt.Println(fmt.Sprintf("parsing feed %d", idx))
		for idx, item := range feed.CVEItems {
			if item != nil && item.CVE != nil && item.CVE.CVEDataMeta != nil {

				dp, err := time.Parse("2006-01-02T15:04Z", item.PublishedDate)
				if err != nil {
					return err
				}
				dlm, err := time.Parse("2006-01-02T15:04Z", item.LastModifiedDate)
				if err != nil {
					return err
				}

				cve := CVE{
					id:               item.CVE.CVEDataMeta.ID,
					datePublished:    dp.Format("2006-01-02 15:04:05"),
					dateLastModified: dlm.Format("2006-01-02 15:04:05"),
				}

				// description
				var dd []string
				if item.CVE.Description != nil {
					for _, d := range item.CVE.Description.DescriptionData {
						if d.Lang == lang {
							dd = append(dd, d.Value)
						}
					}
				}
				cve.description = strings.Join(dd, ". ")

				// problem type
				var pp []string
				if item.CVE.Problemtype != nil && item.CVE.Problemtype.ProblemtypeData != nil {
					for _, p := range item.CVE.Problemtype.ProblemtypeData {
						for _, pd := range p.Description {
							if pd.Lang == lang {
								pp = append(pp, pd.Value)
							}
						}
					}
				}
				cve.problemType = strings.Join(pp, ". ")

				// references and tag
				if item.CVE.References != nil {
					for _, ref := range item.CVE.References.ReferenceData {
						r := CVEReference{
							refID:     referenceID,
							cveID:     item.CVE.CVEDataMeta.ID,
							Name:      ref.Name,
							Refsource: ref.Refsource,
							URL:       ref.URL,
						}
						err = tw.RefWr.Write(r.toStringSlice())
						if err != nil {
							return err
						}


						for _, t := range ref.Tags {
							rt := CVEReferenceTag{refID: referenceID, tag: t}
							err = tw.TagWr.Write(rt.toStringSlice())
							if err != nil {
								return err
							}
						}

						referenceID++
					}
				}

				// TODO: partial data?
				if item.Impact != nil && item.Impact.BaseMetricV2 != nil {
					cve.impactScore = fmt.Sprintf("%.2f", item.Impact.BaseMetricV2.ImpactScore)
					cve.exploitabilityScore = fmt.Sprintf("%.2f", item.Impact.BaseMetricV2.ExploitabilityScore)

					cvss2 := CVSS2{
						cveID:                      item.CVE.CVEDataMeta.ID,
						AcInsufInfo:                item.Impact.BaseMetricV2.AcInsufInfo,
						ObtainAllPrivilege:         item.Impact.BaseMetricV2.ObtainAllPrivilege,
						ObtainOtherPrivilege:       item.Impact.BaseMetricV2.ObtainOtherPrivilege,
						ObtainUserPrivilege:        item.Impact.BaseMetricV2.ObtainUserPrivilege,
						Severity:                   item.Impact.BaseMetricV2.Severity,
						UserInteractionRequired:    item.Impact.BaseMetricV2.UserInteractionRequired,
						AccessComplexity:           item.Impact.BaseMetricV2.CVSSV2.AccessComplexity,
					}

					if item.Impact.BaseMetricV2.CVSSV2 != nil {
						cvss2.AccessVector = item.Impact.BaseMetricV2.CVSSV2.AccessVector
						cvss2.Authentication = item.Impact.BaseMetricV2.CVSSV2.Authentication
						cvss2.AvailabilityImpact = item.Impact.BaseMetricV2.CVSSV2.AvailabilityImpact
						cvss2.AvailabilityRequirement = item.Impact.BaseMetricV2.CVSSV2.AvailabilityRequirement
						cvss2.BaseScore = fmt.Sprintf("%.2f", item.Impact.BaseMetricV2.CVSSV2.BaseScore)
						cvss2.CollateralDamagePotential = item.Impact.BaseMetricV2.CVSSV2.CollateralDamagePotential
						cvss2.ConfidentialityImpact = item.Impact.BaseMetricV2.CVSSV2.ConfidentialityImpact
						cvss2.ConfidentialityRequirement = item.Impact.BaseMetricV2.CVSSV2.ConfidentialityRequirement
						cvss2.EnvironmentalScore = fmt.Sprintf("%.2f", item.Impact.BaseMetricV2.CVSSV2.EnvironmentalScore)
						cvss2.Exploitability = item.Impact.BaseMetricV2.CVSSV2.Exploitability
						cvss2.IntegrityImpact = item.Impact.BaseMetricV2.CVSSV2.IntegrityImpact
						cvss2.IntegrityRequirement = item.Impact.BaseMetricV2.CVSSV2.IntegrityRequirement
						cvss2.RemediationLevel = item.Impact.BaseMetricV2.CVSSV2.RemediationLevel
						cvss2.ReportConfidence = item.Impact.BaseMetricV2.CVSSV2.ReportConfidence
						cvss2.TargetDistribution = item.Impact.BaseMetricV2.CVSSV2.TargetDistribution
						cvss2.TemporalScore = fmt.Sprintf("%.2f", item.Impact.BaseMetricV2.CVSSV2.TemporalScore)
						cvss2.VectorString = item.Impact.BaseMetricV2.CVSSV2.VectorString
						cvss2.Version = item.Impact.BaseMetricV2.CVSSV2.Version
					}

					err = tw.Cvss2Wr.Write(cvss2.toStringSlice())
					if err != nil {
						return err
					}
				}

				// TODO: partial data?
				if item.Impact != nil && item.Impact.BaseMetricV3 != nil && item.Impact.BaseMetricV3.CVSSV3 != nil {
					cve.impactScore = fmt.Sprintf("%.2f", item.Impact.BaseMetricV3.ImpactScore)
					cve.exploitabilityScore = fmt.Sprintf("%.2f", item.Impact.BaseMetricV3.ExploitabilityScore)

					if item.Impact.BaseMetricV3.CVSSV3 != nil {

						cvss3 := CVSS3{
							cveID:                         item.CVE.CVEDataMeta.ID,
							AttackComplexity:              item.Impact.BaseMetricV3.CVSSV3.AttackComplexity,
							AttackVector:                   item.Impact.BaseMetricV3.CVSSV3.AttackVector,
							AvailabilityImpact:             item.Impact.BaseMetricV3.CVSSV3.AvailabilityImpact,
							AvailabilityRequirement:        item.Impact.BaseMetricV3.CVSSV3.AvailabilityRequirement,
							BaseScore:                      fmt.Sprintf("%.2f", item.Impact.BaseMetricV3.CVSSV3.BaseScore),
							BaseSeverity:                   item.Impact.BaseMetricV3.CVSSV3.BaseSeverity,
							ConfidentialityImpact:          item.Impact.BaseMetricV3.CVSSV3.ConfidentialityImpact,
							ConfidentialityRequirement:     item.Impact.BaseMetricV3.CVSSV3.ConfidentialityRequirement,
							EnvironmentalScore:             fmt.Sprintf("%.2f", item.Impact.BaseMetricV3.CVSSV3.EnvironmentalScore),
							EnvironmentalSeverity:          item.Impact.BaseMetricV3.CVSSV3.EnvironmentalSeverity,
							ExploitCodeMaturity:            item.Impact.BaseMetricV3.CVSSV3.ExploitCodeMaturity,
							IntegrityImpact:                item.Impact.BaseMetricV3.CVSSV3.IntegrityImpact,
							IntegrityRequirement:           item.Impact.BaseMetricV3.CVSSV3.IntegrityRequirement,
							ModifiedAttackComplexity:       item.Impact.BaseMetricV3.CVSSV3.ModifiedAttackComplexity,
							ModifiedAttackVector:           item.Impact.BaseMetricV3.CVSSV3.ModifiedAttackVector,
							ModifiedAvailabilityImpact:     item.Impact.BaseMetricV3.CVSSV3.ModifiedAvailabilityImpact,
							ModifiedConfidentialityImpact:  item.Impact.BaseMetricV3.CVSSV3.ModifiedConfidentialityImpact,
							ModifiedIntegrityImpact:        item.Impact.BaseMetricV3.CVSSV3.ModifiedIntegrityImpact,
							ModifiedPrivilegesRequired:     item.Impact.BaseMetricV3.CVSSV3.ModifiedPrivilegesRequired,
							ModifiedScope:                  item.Impact.BaseMetricV3.CVSSV3.ModifiedScope,
							ModifiedUserInteraction:        item.Impact.BaseMetricV3.CVSSV3.ModifiedUserInteraction,
							PrivilegesRequired:             item.Impact.BaseMetricV3.CVSSV3.PrivilegesRequired,
							RemediationLevel:               item.Impact.BaseMetricV3.CVSSV3.RemediationLevel,
							ReportConfidence:               item.Impact.BaseMetricV3.CVSSV3.ReportConfidence,
							Scope:                          item.Impact.BaseMetricV3.CVSSV3.Scope,
							TemporalScore:                  fmt.Sprintf("%.2f", item.Impact.BaseMetricV3.CVSSV3.TemporalScore),
							TemporalSeverity:               item.Impact.BaseMetricV3.CVSSV3.TemporalSeverity,
							UserInteraction:                item.Impact.BaseMetricV3.CVSSV3.UserInteraction,
							VectorString:                   item.Impact.BaseMetricV3.CVSSV3.VectorString,
							Version:                        item.Impact.BaseMetricV3.CVSSV3.Version,
						}
						err = tw.Cvss3Wr.Write(cvss3.toStringSlice())
						if err != nil {
							return err
						}
					}
				}

				// Affected Products
				if item.CVE.Affects != nil && item.CVE.Affects.Vendor != nil {
					for _, v := range item.CVE.Affects.Vendor.VendorData {
						for _, p := range v.Product.ProductData {
							for _, ver := range p.Version.VersionData {
								va := AffectedProduct{
									cveID:           item.CVE.CVEDataMeta.ID,
									vendor:          v.VendorName,
									product:         p.ProductName,
									versionAffected: ver.VersionAffected,
									versionValue:    ver.VersionValue,
								}
								err = tw.ProdWr.Write(va.toStringSlice())
								if err != nil {
									return err
								}
							}
						}
					}
				}


				err = tw.CveWr.Write(cve.toStringSlice())
				if err != nil {
					return err
				}
			} else {
				return fmt.Errorf("error for item %d", idx)
			}
		}
	}
	return nil
}

type CVE struct {
	id 					string
	description 		string
	problemType 		string
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

type AffectedProduct struct {
	cveID    		 string
	vendor   		 string
	product  		 string
	versionAffected  string
	versionValue     string
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


type CVSS2 struct {
	cveID					   string

	AcInsufInfo                bool
	ObtainAllPrivilege         bool
	ObtainOtherPrivilege       bool
	ObtainUserPrivilege        bool
	Severity                   string
	UserInteractionRequired    bool
	AccessComplexity           string
	AccessVector               string
	Authentication             string
	AvailabilityImpact         string
	AvailabilityRequirement    string
	BaseScore                  string
	CollateralDamagePotential  string
	ConfidentialityImpact      string
	ConfidentialityRequirement string
	EnvironmentalScore         string
	Exploitability             string
	IntegrityImpact            string
	IntegrityRequirement       string
	RemediationLevel           string
	ReportConfidence           string
	TargetDistribution         string
	TemporalScore              string
	VectorString               string
	Version                    string
}

type CVSS3 struct {
	cveID						  string

	AttackComplexity              string
	AttackVector                  string
	AvailabilityImpact            string
	AvailabilityRequirement       string
	BaseScore                     string
	BaseSeverity                  string
	ConfidentialityImpact         string
	ConfidentialityRequirement    string
	EnvironmentalScore            string
	EnvironmentalSeverity         string
	ExploitCodeMaturity           string
	IntegrityImpact               string
	IntegrityRequirement          string
	ModifiedAttackComplexity      string
	ModifiedAttackVector          string
	ModifiedAvailabilityImpact    string
	ModifiedConfidentialityImpact string
	ModifiedIntegrityImpact       string
	ModifiedPrivilegesRequired    string
	ModifiedScope                 string
	ModifiedUserInteraction       string
	PrivilegesRequired            string
	RemediationLevel              string
	ReportConfidence              string
	Scope                         string
	TemporalScore                 string
	TemporalSeverity              string
	UserInteraction               string
	VectorString                  string
	Version                       string
}

func (cvss2 CVSS2) headerLine() []string {
	return []string{
		"cve_id",
		"ac_insuf_info",
		"obtain_all_privilege",
		"obtain_other_privilege",
		"obtain_user_privilege",
		"severity",
		"user_interaction_required",
		"access_complexity",
		"access_vector",
		"authentication",
		"availability_impact",
		"availability_requirement",
		"base_score",
		"collateral_damage_potential",
		"confidentiality_impact",
		"confidentiality_requirement",
		"environmental_score",
		"exploitability",
		"integrity_impact",
		"integrity_requirement",
		"remediation_level",
		"report_confidence",
		"target_distribution",
		"temporal_score",
		"vector_string",
		"version",
	}
}

func (cvss2 *CVSS2) toStringSlice() []string {

	b2s := func(b bool) string {
		if b {
			return "True"
		}
		return "False"
	}

	return []string{
		cvss2.cveID,
		b2s(cvss2.AcInsufInfo),
		b2s(cvss2.ObtainAllPrivilege),
		b2s(cvss2.ObtainOtherPrivilege),
		b2s(cvss2.ObtainUserPrivilege),
		cvss2.Severity,
		b2s(cvss2.UserInteractionRequired),
		cvss2.AccessComplexity,
		cvss2.AccessVector,
		cvss2.Authentication,
		cvss2.AvailabilityImpact,
		cvss2.AvailabilityRequirement,
		cvss2.BaseScore,
		cvss2.CollateralDamagePotential,
		cvss2.ConfidentialityImpact,
		cvss2.ConfidentialityRequirement,
		cvss2.EnvironmentalScore,
		cvss2.Exploitability,
		cvss2.IntegrityImpact,
		cvss2.IntegrityRequirement,
		cvss2.RemediationLevel,
		cvss2.ReportConfidence,
		cvss2.TargetDistribution,
		cvss2.TemporalScore,
		cvss2.VectorString,
		cvss2.Version,
	}
}

func (CVSS3 CVSS3) headerLine() []string {
	return []string{
		"cve_id",
		"attack_complexity",
		"attack_vector",
		"availability_impact",
		"availability_requirement",
		"base_score",
		"base_severity",
		"confidentiality_impact",
		"confidentiality_requirement",
		"environmental_score",
		"environmental_severity",
		"exploit_code_maturity",
		"integrity_impact",
		"integrity_requirement",
		"modified_attack_complexity",
		"modifie_attack_vector",
		"modified_availability_impact",
		"modified_confidentiality_impact",
		"modified_integrity_impact",
		"modified_privileges_required",
		"modified_scope",
		"modified_user_interaction",
		"privileges_required",
		"remediation_level",
		"report_confidence",
		"scope",
		"temporal_score",
		"temporal_severity",
		"user_interaction",
		"vector_string",
		"version",
	}
}

func (cvss3 *CVSS3) toStringSlice() []string {
	return []string{
		cvss3.cveID,
		cvss3.AttackComplexity,
		cvss3.AttackVector,
		cvss3.AvailabilityImpact,
		cvss3.AvailabilityRequirement,
		cvss3.BaseScore,
		cvss3.BaseSeverity,
		cvss3.ConfidentialityImpact,
		cvss3.ConfidentialityRequirement,
		cvss3.EnvironmentalScore,
		cvss3.EnvironmentalSeverity,
		cvss3.ExploitCodeMaturity,
		cvss3.IntegrityImpact,
		cvss3.IntegrityRequirement,
		cvss3.ModifiedAttackComplexity,
		cvss3.ModifiedAttackVector,
		cvss3.ModifiedAvailabilityImpact,
		cvss3.ModifiedConfidentialityImpact,
		cvss3.ModifiedIntegrityImpact,
		cvss3.ModifiedPrivilegesRequired,
		cvss3.ModifiedScope,
		cvss3.ModifiedUserInteraction,
		cvss3.PrivilegesRequired,
		cvss3.RemediationLevel,
		cvss3.ReportConfidence,
		cvss3.Scope,
		cvss3.TemporalScore,
		cvss3.TemporalSeverity,
		cvss3.UserInteraction,
		cvss3.VectorString,
		cvss3.Version,
	}
}