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

	tableCVE           = "CVE"
	tableCVSS2         = "CVSS2"
	tableCVSS3         = "CVSS3"
	tableReferences    = "references"
	tableReferenceTags = "reference_tags"
	tableProducts      = "affected_products"
)

func main() {

	if len(os.Args) != 2 {
		fmt.Println("usage: go run main.go <workdir>")
		return
	}
	workdir := os.Args[1]

	const (
		timeout         = 5 * time.Minute
		cvefeed nvd.CVE = 4
		cpefeed nvd.CPE = 0
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

	CveFile, err := os.Create(fmt.Sprintf("%s/%s.csv", workdir, tableCVE))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer CveFile.Close()
	Cvss2File, err := os.Create(fmt.Sprintf("%s/%s.csv", workdir, tableCVSS2))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer Cvss2File.Close()
	Cvss3File, err := os.Create(fmt.Sprintf("%s/%s.csv", workdir, tableCVSS3))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer Cvss3File.Close()
	RefFile, err := os.Create(fmt.Sprintf("%s/%s.csv", workdir, tableReferences))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer RefFile.Close()
	TagFile, err := os.Create(fmt.Sprintf("%s/%s.csv", workdir, tableReferenceTags))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer TagFile.Close()
	ProductFile, err := os.Create(fmt.Sprintf("%s/%s.csv", workdir, tableProducts))
	if err != nil {
		fmt.Println(err)
		return
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

	err = NormalizeCVEItems(languageEnglish, &tw, feeds...)

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

func NormalizeCVEItems(lang string, tw *TableWriter, feeds ...*schema.NVDCVEFeedJSON10) (err error) {
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

				if item.Impact != nil && item.Impact.BaseMetricV2 != nil {
					cve.impactScore = fmt.Sprintf("%.2f", item.Impact.BaseMetricV2.ImpactScore)
					cve.exploitabilityScore = fmt.Sprintf("%.2f", item.Impact.BaseMetricV2.ExploitabilityScore)

					cvss2 := NewCVSS2FromCVEItem(item)

					err = tw.Cvss2Wr.Write(cvss2.toStringSlice())
					if err != nil {
						return err
					}
				}

				if item.Impact != nil && item.Impact.BaseMetricV3 != nil && item.Impact.BaseMetricV3.CVSSV3 != nil {
					cve.impactScore = fmt.Sprintf("%.2f", item.Impact.BaseMetricV3.ImpactScore)
					cve.exploitabilityScore = fmt.Sprintf("%.2f", item.Impact.BaseMetricV3.ExploitabilityScore)

					if item.Impact.BaseMetricV3.CVSSV3 != nil {

						cvss3 := NewCVSS3FromCVEItem(item)

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
