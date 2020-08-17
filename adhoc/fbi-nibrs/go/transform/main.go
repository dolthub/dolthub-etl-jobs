package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/google/uuid"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

var migrateLegacyHeaders = flag.Bool("migrateLegacyHeaders", false, "Migrate legacy headers to uppercase")
var transformForDolt = flag.Bool("transformForDolt", false, "Transform timestamps and dates to dolt parsable format, add incremented primary keys")
var removeOldFiles = flag.Bool("removeOldFiles", false, "Removes all files except for whats in /[state]/[year]/[abbr]/transformed")
var dataDir = flag.String("dataDir", "", "Directory of nibrs data")

type state struct {
	Abbr string
	ID   int
}

// sourced from alabama 1991 and 2018 ref_state.csv
var states = map[string]state{
	"alabama": {"AL", 2},
	"alaska": {"AK", 1},
	"arizona": {"AZ", 5},
	"arkansas": {"AR", 3},
	"california": {"CA", 6},
	"colorado": {"CO", 7},
	"connecticut": {"CT", 8},
	"district_of_columbia": {"DC", 10},
	"delaware": {"DE", 11},
	"florida": {"FL", 12},
	"georgia": {"GA", 13},
	"hawaii": {"HI", 15},
	"idaho": {"ID", 17},
	"illinois": {"IL", 18},
	"indiana": {"IN", 19},
	"iowa": {"IA", 16},
	"kansas": {"KS", 20},
	"kentucky": {"KY", 21},
	"louisiana": {"LA", 22},
	"maine": {"ME", 25},
	"maryland": {"MD", 24},
	"massachusetts": {"MA", 23},
	"michigan": {"MI", 26},
	"minnesota": {"MN", 27},
	"mississippi": {"MS", 29},
	"missouri": {"MO", 28},
	"montana": {"MT", 30},
	"nebraska": {"NE", 31},
	"nevada": {"NV", 37},
	"new_hampshire": {"NH", 34},
	"new_jersey": {"NJ", 35},
	"new_mexico": {"NM", 36},
	"new_york": {"NY", 38},
	"north_carolina": {"NC", 32},
	"north_dakota": {"ND", 33},
	"ohio": {"OH", 39},
	"oklahoma": {"OK", 40},
	"oregon": {"OR", 41},
	"pennsylvania": {"PA", 42},
	"rhode_island": {"RI", 44},
	"south_carolina": {"SC", 45},
	"south_dakota": {"SD", 46},
	"tennessee": {"TN", 47},
	"texas": {"TX", 48},
	"utah": {"UT", 49},
	"vermont": {"VT", 52},
	"virginia": {"VA", 51},
	"washington": {"WA", 53},
	"west_virginia": {"WV", 55},
	"wisconsin": {"WI", 54},
	"wyoming": {"WY", 56},
}

var mainTables = map[string]int{
	"nibrs_month":               1,
	"nibrs_incident":            1,
	"nibrs_offense":             1,
	"nibrs_offender":            1,
	"nibrs_victim":              1,
	"nibrs_victim_offense":      1,
	"nibrs_victim_offender_rel": 1,
	"nibrs_victim_circumstances":1,
	"nibrs_weapon":              1,
	"nibrs_property":            1,
	"nibrs_property_desc":       1,
	"nibrs_suspected_drug":      1,
	"nibrs_suspect_using":       1,
	"nibrs_victim_injury":       1,
	"nibrs_criminal_act":        1,
	"nibrs_bias_motivation":     1,
	"nibrs_arrestee":            1,
	"nibrs_arrestee_weapon":     1,
	"agency_participation":      1,
	"cde_agencies":              1,
	"agencies":                  1,
}

// if column is not empty, try to split by - for format "01-JAN-06"
var timeDateTablesMap = map[string][]string{
	"nibrs_month":         {"REPORT_DATE", "PREPARED_DATE"},
	"nibrs_incident":      {"SUBMISSION_DATE", "INCIDENT_DATE", "CLEARED_EXCEPT_DATE"},
	"nibrs_arrestee":      {"ARREST_DATE"},
	"nibrs_property_desc": {"DATE_RECOVERED"},
	"agencies": {
		"NIBRS_CERT_DATE",
		"NIBRS_START_DATE",
		"NIBRS_LEOKA_START_DATE",
		"NIBRS_CT_START_DATE",
		"NIBRS_MULTI_BIAS_START_DATE",
		"NIBRS_OFF_ETH_START_DATE",
	},
}

const (
	dataYearCol                   = "DATA_YEAR"
	stateIDCol                    = "STATE_ID"
	agencyTableCol                = "AGENCY_TABLE_TYPE_ID"
	nibrsBiasMotivationCol        = "NIBRS_BIAS_MOTIVATION_ID"
	nibrsPropDescIDCol            = "NIBRS_PROP_DESC_ID"
	nibrsSuspectUsingIDCol        = "NIBRS_SUSPECT_USING_ID"
	nibrsVictimInjuryIDCol        = "NIBRS_VICTIM_INJURY_ID"
	nibrsVictimOffenseIDCol       = "NIBRS_VICTIM_OFFENSE_ID"
	nibrsVictimCircumstancesIDCol = "NIBRS_VICTIM_CIRCUMSTANCES_ID"
	nibrsCriminalActIDCol = "NIBRS_CRIMINAL_ACT_ID"
	agencyParticipationIDCol = "AGENCY_PARTICIPATION_ID"
	agencyIDCol              = "NIBRS_AGENCY_ID"
)

var addColumnsTableMap = map[string][]string{
	"cde_agencies":               {dataYearCol},
	"agencies":                   {agencyIDCol},
	"agency_participation":       {dataYearCol, stateIDCol, agencyParticipationIDCol},
	"nibrs_month":                {stateIDCol, agencyTableCol},
	"nibrs_incident":             {dataYearCol, stateIDCol, agencyTableCol},
	"nibrs_arrestee":             {dataYearCol, stateIDCol},
	"nibrs_arrestee_weapon":      {dataYearCol, stateIDCol},
	"nibrs_offense":              {dataYearCol, stateIDCol},
	"nibrs_bias_motivation":      {dataYearCol, stateIDCol, "NIBRS_BIAS_MOTIVATION_ID"},
	"nibrs_offender":             {dataYearCol, stateIDCol},
	"nibrs_property":             {dataYearCol, stateIDCol},
	"nibrs_property_desc":        {dataYearCol, stateIDCol, "NIBRS_PROP_DESC_ID"},
	"nibrs_suspect_using":        {dataYearCol, stateIDCol, "NIBRS_SUSPECT_USING_ID"},
	"nibrs_suspected_drug":       {dataYearCol, stateIDCol},
	"nibrs_victim":               {dataYearCol, stateIDCol},
	"nibrs_victim_injury":        {dataYearCol, stateIDCol, "NIBRS_VICTIM_INJURY_ID"},
	"nibrs_victim_offender_rel":  {dataYearCol, stateIDCol},
	"nibrs_victim_offense":       {dataYearCol, stateIDCol, "NIBRS_VICTIM_OFFENSE_ID"},
	"nibrs_criminal_act":         {dataYearCol, stateIDCol, nibrsCriminalActIDCol},
	"nibrs_weapon":               {dataYearCol, stateIDCol},
	"nibrs_victim_circumstances": {dataYearCol, stateIDCol, "NIBRS_VICTIM_CIRCUMSTANCES_ID"},
}

func main() {
	flag.Parse()

	if *migrateLegacyHeaders && !*transformForDolt {
		migrate(*dataDir)
		fmt.Println("Finished migrating legacy headers")
		os.Exit(0)
	} else if *transformForDolt && !*migrateLegacyHeaders {
		transform(*dataDir)
		fmt.Println("Finished transforming data")
		os.Exit(0)
	} else if *removeOldFiles && !*transformForDolt && !*migrateLegacyHeaders {
		removeFiles(*dataDir)
		fmt.Println("Finished removing old files")
		os.Exit(0)
	}

	log.Fatal("Must supply flag")
}

func removeFiles (dataDir string) {
	for stateName, s := range states {
		for year := 1991; year < 2019; year++ {
			dirNoAbbr := filepath.Join(dataDir, stateName, fmt.Sprintf("%d", year))
			dirWithAbbr := filepath.Join(dataDir, stateName, fmt.Sprintf("%d", year), s.Abbr)
			for _, dir := range []string{dirNoAbbr, dirWithAbbr} {
				if _, err := os.Stat(dir); os.IsNotExist(err) {
					continue
				}
				files, err := ioutil.ReadDir(dir)
				if err != nil {
					log.Fatal(err)
				}
				for _, f := range files {
					if strings.HasSuffix(f.Name(), ".csv") ||
						strings.HasSuffix(f.Name(), ".zip") ||
						strings.HasSuffix(f.Name(), ".md") ||
						strings.HasSuffix(f.Name(), ".html") ||
						strings.HasSuffix(f.Name(), ".pdf") ||
						strings.HasSuffix(f.Name(), ".sql") {
						err := os.Remove(filepath.Join(dir, f.Name()))
						if err != nil {
							log.Fatal(err)
						}
						fmt.Printf("removing file: %s \n", filepath.Join(dir, f.Name()))
					}
				}
			}
		}
	}
}

func transform(dataDir string) {
	for stateName, s := range states {
		for year := 1991; year < 2019; year++ {
			directory := filepath.Join(dataDir, stateName, fmt.Sprintf("%d", year), s.Abbr)
			if _, err := os.Stat(directory); os.IsNotExist(err) {
				continue
			}
			fmt.Printf("working in %s \n", directory)
			files, err := ioutil.ReadDir(directory)
			if err != nil {
				log.Fatal(err)
			}

			transformDir := filepath.Join(directory, "transformed")
			err = os.Mkdir(transformDir, os.ModePerm)
			if err != nil {
				log.Fatal(err)
			}

			for _, f := range files {
				if strings.HasSuffix(f.Name(), ".csv") {
					trimmed := strings.TrimSuffix(f.Name(), ".csv")

					_, ok := mainTables[strings.ToLower(trimmed)]
					if !ok {
						continue
					}

					lower := strings.ToLower(trimmed)

					var transformColumnTimeDate bool
					var addRequiredData bool

					// check if we need to transform date/time
					timeDateCols, ok := timeDateTablesMap[lower]
					if ok {
						transformColumnTimeDate = true
					}

					// check if we need to add columns
					dataCols, ok := addColumnsTableMap[lower]
					if ok {
						addRequiredData = true
					}

					// skip irrelavant table
					if !transformColumnTimeDate && !addRequiredData {
						continue
					}

					// open the csvs
					csvIn, err := os.Open(filepath.Join(directory, f.Name()))
					if err != nil {
						log.Fatal(err)
					}

					r := csv.NewReader(csvIn)

					// setup writer
					csvOut, err := os.Create(filepath.Join(transformDir, fmt.Sprintf("%s.csv", strings.ToUpper(trimmed))))
					if err != nil {
						csvIn.Close()
						log.Fatal(err)
					}
					w := csv.NewWriter(csvOut)

					// handle header
					rec, err := r.Read()
					if err != nil {
						csvIn.Close()
						csvOut.Close()
						log.Fatal(err)
					}

					// if we are adding columns, do this for header
					// TODO: fix duplicate columns
					var alteredHeaderStartIdx int
					var headers []string
					if addRequiredData {
						alteredHeaderStartIdx = len(rec)
						// append all data cols to header
						for _, col := range dataCols {
							rec = append(rec, col)
						}
						headers = make([]string, len(rec))
						copy(headers, rec)
					}

					if transformColumnTimeDate && headers == nil {
						headers = make([]string, len(rec))
						copy(headers, rec)
					}

					if headers == nil {
						csvIn.Close()
						csvOut.Close()
						log.Fatal("something went wrong copying headers")
					}

					// collect indexes of date columns to modify at the row level
					headerIdxList := make([]int, 0)
					if transformColumnTimeDate {
						for idx, header := range headers {
							for _, col := range timeDateCols {
								if col == header {
									headerIdxList = append(headerIdxList, idx)
								}
							}
						}
					}

					fmt.Printf("State: %s Year: %d, File: %s \n", stateName, year, trimmed)
					if err = w.Write(rec); err != nil {
						csvIn.Close()
						csvOut.Close()
						log.Fatal(err)
					}
					w.Flush()

					// handle all other rows
					for {
						rec, err = r.Read()
						if err != nil {
							if err == io.EOF {
								break
							}
							csvIn.Close()
							csvOut.Close()
							log.Fatal(err)
						}

						// do the append data work
						if addRequiredData {
							for i := alteredHeaderStartIdx; i < len(headers); i++ {
								col := headers[i]
								valueFunc := fromColumnGetValueFunc(col, year, s.ID)
								rec = append(rec, valueFunc())
							}
						}

						// do the time/data transform work
						if transformColumnTimeDate {
							for _, colIdx := range headerIdxList {
								timeDateVal := rec[colIdx]
								if timeDateVal == "" {
									continue
								}
								colonTest := strings.Split(timeDateVal, ":")
								if len(colonTest) != 1 {
									continue
								}
								timeDateParts := strings.Split(colonTest[0], "-")
								if len(timeDateParts) != 3 {
									continue
								}
								day := timeDateParts[0]
								month := fromTimeDatePartMonth(timeDateParts[1])
								year := fromTimeDatePartYear(timeDateParts[2])
								newVal := fmt.Sprintf("%04d-%02d-%s", year, month, day)
								rec[colIdx] = newVal
							}
						}

						if err = w.Write(rec); err != nil {
							csvIn.Close()
							csvOut.Close()
							log.Fatal(err)
						}
						w.Flush()
					}
				}
			}
		}
	}
}

func fromColumnGetValueFunc(columnName string, year, stateID int) func() string {
	switch columnName {
	case dataYearCol:
		return func() string {
			return fmt.Sprintf("%04d", year)
		}
	case stateIDCol:
		return func() string {
			return fmt.Sprintf("%d", stateID)
		}
	case agencyTableCol:
		return func() string {
			return fmt.Sprintf("%d", getAgencyTableType(year))
		}
	case agencyParticipationIDCol, nibrsBiasMotivationCol, nibrsPropDescIDCol,
		nibrsSuspectUsingIDCol, nibrsVictimOffenseIDCol, nibrsVictimInjuryIDCol,
		nibrsVictimCircumstancesIDCol, nibrsCriminalActIDCol, agencyIDCol:
		return func() string {
			id, err := uuid.NewRandom()
			if err != nil {
				panic(err)
			}
			return id.String()
		}
	default:
		panic("unknown column name")
	}
}

// does not handle nebraska 2015
func getAgencyTableType(year int) int {
	if year >= 1991 && year <= 2015 {
		return 1
	} else if year > 2015 {
		return 2
	}
	panic("year is out of range")
}

func fromTimeDatePartYear(year string) int {
	twoDigitYear, err := strconv.Atoi(year)
	if err != nil {
		log.Fatal(err)
	}

	// handle years from 2000 to 2030
	if twoDigitYear >= 0 && twoDigitYear <= 30 {
		return 2000 + twoDigitYear

		// handle years from 1930 up
	} else if twoDigitYear > 30 {
		return 1900 + twoDigitYear
	}

	panic(fmt.Sprintf("unable to get year from %d \n", twoDigitYear))
}

func fromTimeDatePartMonth(month string) int {
	switch strings.ToLower(month) {
	case "jan":
		return 1
	case "feb":
		return 2
	case "mar":
		return 3
	case "apr":
		return 4
	case "may":
		return 5
	case "jun":
		return 6
	case "jul":
		return 7
	case "aug":
		return 8
	case "sep":
		return 9
	case "oct":
		return 10
	case "nov":
		return 11
	case "dec":
		return 12
	default:
		panic("unknown month")
	}
}

func migrate(dataDir string) {
	for stateName, s := range states {
		for year := 1991; year < 2016; year++ {

			directory := filepath.Join(dataDir, stateName, fmt.Sprintf("%d", year))
			if _, err := os.Stat(directory); os.IsNotExist(err) {
				continue
			}
			if stateName == "nebraska" && year == 2015 {
				continue
			}

			err := migrateHeaders(directory, stateName, s.Abbr, year)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

func migrateHeaders(directory, state, abbr string, year int) error {
	files, err := ioutil.ReadDir(directory)
	if err != nil {
		return err
	}

	transformDir := filepath.Join(directory, abbr)
	err = os.Mkdir(transformDir, os.ModePerm)
	if err != nil {
		return err
	}

	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".csv") {
			trimmed := strings.TrimSuffix(f.Name(), ".csv")

			_, ok := mainTables[strings.ToLower(trimmed)]
			if !ok {
				fmt.Printf("Skipping ref table: %s \n", trimmed)
				continue
			}

			csvIn, err := os.Open(filepath.Join(directory, f.Name()))
			if err != nil {
				return err
			}

			r := csv.NewReader(csvIn)

			// setup writer
			csvOut, err := os.Create(filepath.Join(transformDir, fmt.Sprintf("%s.csv", strings.ToUpper(trimmed))))
			if err != nil {
				inErr := csvIn.Close()
				if inErr != nil {
					fmt.Printf("csvIn close error: %+v \n", inErr)
				}
				return err
			}
			w := csv.NewWriter(csvOut)

			// handle header
			rec, err := r.Read()
			if err != nil {
				inErr := csvIn.Close()
				if inErr != nil {
					fmt.Printf("csvIn close error: %+v \n", inErr)
				}
				outErr := csvOut.Close()
				if outErr != nil {
					fmt.Printf("csvOut close error: %+v \n", outErr)
				}
				return err
			}

			upperHeaders := make([]string, 0)
			for _, lowerHeader := range rec {
				upperHeaders = append(upperHeaders, strings.ToUpper(lowerHeader))
			}

			fmt.Printf("State: %s Year: %d, File: %s \n", state, year, trimmed)
			if err = w.Write(upperHeaders); err != nil {
				inErr := csvIn.Close()
				if inErr != nil {
					fmt.Printf("csvIn close error: %+v \n", inErr)
				}
				outErr := csvOut.Close()
				if outErr != nil {
					fmt.Printf("csvOut close error: %+v \n", outErr)
				}
				return err
			}
			w.Flush()
			for {
				rec, err = r.Read()
				if err != nil {
					if err == io.EOF {
						break
					}
					inErr := csvIn.Close()
					if inErr != nil {
						fmt.Printf("csvIn close error: %+v \n", inErr)
					}
					outErr := csvOut.Close()
					if outErr != nil {
						fmt.Printf("csvOut close error: %+v \n", outErr)
					}
					return err
				}
				if err = w.Write(rec); err != nil {
					inErr := csvIn.Close()
					if inErr != nil {
						fmt.Printf("csvIn close error: %+v \n", inErr)
					}
					outErr := csvIn.Close()
					if outErr != nil {
						fmt.Printf("csvOut close error: %+v \n", outErr)
					}
					return err
				}
				w.Flush()
			}
			err = csvIn.Close()
			if err != nil {
				outErr := csvOut.Close()
				if outErr != nil {
					fmt.Printf("csvOut close error: %+v \n", outErr)
				}
				return err
			}
			err = csvOut.Close()
			if err != nil {
				return err
			}
		}
	}
	return nil
}
