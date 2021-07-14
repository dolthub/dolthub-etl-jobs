package main

import (
	"fmt"

	"github.com/facebookincubator/nvdtools/cvefeed/nvd/schema"
)

type CVSS2 struct {
	cveID string

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
	cveID string

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

func NewCVSS2FromCVEItem(item *schema.NVDCVEFeedJSON10DefCVEItem) CVSS2 {
	cvss2 := CVSS2{
		cveID:                   item.CVE.CVEDataMeta.ID,
		AcInsufInfo:             item.Impact.BaseMetricV2.AcInsufInfo,
		ObtainAllPrivilege:      item.Impact.BaseMetricV2.ObtainAllPrivilege,
		ObtainOtherPrivilege:    item.Impact.BaseMetricV2.ObtainOtherPrivilege,
		ObtainUserPrivilege:     item.Impact.BaseMetricV2.ObtainUserPrivilege,
		Severity:                item.Impact.BaseMetricV2.Severity,
		UserInteractionRequired: item.Impact.BaseMetricV2.UserInteractionRequired,
		AccessComplexity:        item.Impact.BaseMetricV2.CVSSV2.AccessComplexity,
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
	return cvss2
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

// Pointer Validation should happen upstream
func NewCVSS3FromCVEItem(item *schema.NVDCVEFeedJSON10DefCVEItem) CVSS3 {
	return CVSS3{
		cveID:                         item.CVE.CVEDataMeta.ID,
		AttackComplexity:              item.Impact.BaseMetricV3.CVSSV3.AttackComplexity,
		AttackVector:                  item.Impact.BaseMetricV3.CVSSV3.AttackVector,
		AvailabilityImpact:            item.Impact.BaseMetricV3.CVSSV3.AvailabilityImpact,
		AvailabilityRequirement:       item.Impact.BaseMetricV3.CVSSV3.AvailabilityRequirement,
		BaseScore:                     fmt.Sprintf("%.2f", item.Impact.BaseMetricV3.CVSSV3.BaseScore),
		BaseSeverity:                  item.Impact.BaseMetricV3.CVSSV3.BaseSeverity,
		ConfidentialityImpact:         item.Impact.BaseMetricV3.CVSSV3.ConfidentialityImpact,
		ConfidentialityRequirement:    item.Impact.BaseMetricV3.CVSSV3.ConfidentialityRequirement,
		EnvironmentalScore:            fmt.Sprintf("%.2f", item.Impact.BaseMetricV3.CVSSV3.EnvironmentalScore),
		EnvironmentalSeverity:         item.Impact.BaseMetricV3.CVSSV3.EnvironmentalSeverity,
		ExploitCodeMaturity:           item.Impact.BaseMetricV3.CVSSV3.ExploitCodeMaturity,
		IntegrityImpact:               item.Impact.BaseMetricV3.CVSSV3.IntegrityImpact,
		IntegrityRequirement:          item.Impact.BaseMetricV3.CVSSV3.IntegrityRequirement,
		ModifiedAttackComplexity:      item.Impact.BaseMetricV3.CVSSV3.ModifiedAttackComplexity,
		ModifiedAttackVector:          item.Impact.BaseMetricV3.CVSSV3.ModifiedAttackVector,
		ModifiedAvailabilityImpact:    item.Impact.BaseMetricV3.CVSSV3.ModifiedAvailabilityImpact,
		ModifiedConfidentialityImpact: item.Impact.BaseMetricV3.CVSSV3.ModifiedConfidentialityImpact,
		ModifiedIntegrityImpact:       item.Impact.BaseMetricV3.CVSSV3.ModifiedIntegrityImpact,
		ModifiedPrivilegesRequired:    item.Impact.BaseMetricV3.CVSSV3.ModifiedPrivilegesRequired,
		ModifiedScope:                 item.Impact.BaseMetricV3.CVSSV3.ModifiedScope,
		ModifiedUserInteraction:       item.Impact.BaseMetricV3.CVSSV3.ModifiedUserInteraction,
		PrivilegesRequired:            item.Impact.BaseMetricV3.CVSSV3.PrivilegesRequired,
		RemediationLevel:              item.Impact.BaseMetricV3.CVSSV3.RemediationLevel,
		ReportConfidence:              item.Impact.BaseMetricV3.CVSSV3.ReportConfidence,
		Scope:                         item.Impact.BaseMetricV3.CVSSV3.Scope,
		TemporalScore:                 fmt.Sprintf("%.2f", item.Impact.BaseMetricV3.CVSSV3.TemporalScore),
		TemporalSeverity:              item.Impact.BaseMetricV3.CVSSV3.TemporalSeverity,
		UserInteraction:               item.Impact.BaseMetricV3.CVSSV3.UserInteraction,
		VectorString:                  item.Impact.BaseMetricV3.CVSSV3.VectorString,
		Version:                       item.Impact.BaseMetricV3.CVSSV3.Version,
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
