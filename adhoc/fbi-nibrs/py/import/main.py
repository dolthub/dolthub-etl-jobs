import os
import argparse

states = {
  "alabama": "AL",
  "alaska": "AK",
  "arizona": "AZ",
  "arkansas": "AR",
  "california": "CA",
  "colorado": "CO",
  "connecticut": "CT",
  "district_of_columbia": "DC",
  "delaware": "DE",
  "florida": "FL",
  "georgia": "GA",
  "hawaii": "HI",
  "idaho": "ID",
  "illinois": "IL",
  "indiana": "IN",
  "iowa": "IA",
  "kansas": "KS",
  "kentucky": "KY",
  "louisiana": "LA",
  "maine": "ME",
  "maryland": "MD",
  "massachusetts": "MA",
  "michigan": "MI",
  "minnesota": "MN",
  "mississippi": "MS",
  "missouri": "MO",
  "montana": "MT",
  "nebraska": "NE",
  "nevada": "NV",
  "new_hampshire": "NH",
  "new_jersey": "NJ",
  "new_mexico": "NM",
  "new_york": "NY",
  "north_carolina": "NC",
  "north_dakota": "ND",
  "ohio": "OH",
  "oklahoma": "OK",
  "oregon": "OR",
  "pennsylvania": "PA",
  "rhode_island": "RI",
  "south_carolina": "SC",
  "south_dakota": "SD",
  "tennessee": "TN",
  "texas": "TX",
  "utah": "UT",
  "vermont": "VT",
  "virginia": "VA",
  "washington": "WA",
  "west_virginia": "WV",
  "wisconsin": "WI",
  "wyoming": "WY",
}

tableFiles = {
    "CDE_AGENCIES.csv": "cde_agencies",
    "AGENCIES.csv": "agencies",
    "AGENCY_PARTICIPATION.csv": "agency_participation",
    "NIBRS_MONTH.csv": "nibrs_month",
    "NIBRS_INCIDENT.csv": "nibrs_incident",
    "NIBRS_ARRESTEE.csv": "nibrs_arrestee",
    "NIBRS_ARRESTEE_WEAPON.csv": "nibrs_arrestee_weapon",
    "NIBRS_OFFENSE.csv": "nibrs_offense",
    "NIBRS_BIAS_MOTIVATION.csv": "nibrs_bias_motivation",
    "NIBRS_OFFENDER.csv": "nibrs_offender",
    "NIBRS_PROPERTY.csv": "nibrs_property",
    "NIBRS_PROPERTY_DESC.csv": "nibrs_property_desc",
    "NIBRS_SUSPECT_USING.csv": "nibrs_suspect_using",
    "NIBRS_SUSPECTED_DRUG.csv": "nibrs_suspected_drug",
    "NIBRS_VICTIM.csv": "nibrs_victim",
    "NIBRS_VICTIM_INJURY.csv": "nibrs_victim_injury",
    "NIBRS_VICTIM_OFFENDER_REL.csv": "nibrs_victim_offender_rel",
    "NIBRS_VICTIM_OFFENSE.csv": "nibrs_victim_offense",
    "NIBRS_CRIMINAL_ACT.csv": "nibrs_criminal_act",
    "NIBRS_WEAPON.csv": "nibrs_weapon",
    "NIBRS_VICTIM_CIRCUMSTANCES.csv": "nibrs_victim_circumstances",
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('importScriptPath', metavar='importScriptPath', type=str, nargs='+',help='path where import scripts should be written')
    parser.add_argument('dataPath', metavar='dataPath', type=str, nargs='+', help='path to downloaded nibrs data')
    args = parser.parse_args()
    importDir = args.importScriptPath[0]
    dataPath = args.dataPath[0]

    legacyDir = os.path.join(importDir, "legacy")
    os.makedirs(legacyDir)
    latestDir = os.path.join(importDir, "latest")
    os.makedirs(latestDir)

    dirs = {"legacy": legacyDir, "latest": latestDir}

    for key in dirs:
        writeDir = dirs[key]
        for state in states:
            stateFile = os.path.join(writeDir, "import-{0}.sh".format(state))
            f = open(stateFile, "a")
            f.write('#!/bin/bash\n')
            f.write('set -eo pipefail\n')

            for tableFile in tableFiles:
                tableName = tableFiles[tableFile]
                abbr = states[state]
                if key == "legacy" and tableName == "agencies":
                    continue
                if key == "latest" and (tableName == "cde_agencies" or tableName == "agency_participation"):
                    continue

                for year in range(1991, 2019):
                    csv_file = "{0}/{1}/{2}/{3}/transformed/{4}".format(dataPath, state, year, abbr, tableFile)
                    if not os.path.exists(csv_file):
                        continue

                    f.write('echo \"{0}\"\n'.format(csv_file))
                    f.write('dolt table import -u {} {}\n'.format(tableName, csv_file))
                    f.write('\n')

            f.close()
