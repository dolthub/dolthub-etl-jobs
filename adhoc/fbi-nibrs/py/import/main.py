import os

states = {
  # "alabama": "AL",
  # "alaska": "AK",
  # "arizona": "AZ",
  # "arkansas": "AR",
  # "california": "CA",
  # "colorado": "CO",
#   "connecticut": "CT",
  # "district_of_columbia": "DC",
  # "delaware": "DE",
  # "florida": "FL",
  "georgia": "GA",
  # "hawaii": "HI",
  # "idaho": "ID",
  # "illinois": "IL",
  # "indiana": "IN",
  # "iowa": "IA",
  # "kansas": "KS",
  # "kentucky": "KY",
  # "louisiana": "LA",
  # "maine": "ME",
  # "maryland": "MD",
  # "massachusetts": "MA",
  # "michigan": "MI",
  # "minnesota": "MN",
  # "mississippi": "MS",
  # "missouri": "MO",
  # "montana": "MT",
  # "nebraska": "NE",
  # "nevada": "NV",
  # "new_hampshire": "NH",
  # "new_jersey": "NJ",
  # "new_mexico": "NM",
  # "new_york": "NY",
  # "north_carolina": "NC",
  # "north_dakota": "ND",
  # "ohio": "OH",
  # "oklahoma": "OK",
  # "oregon": "OR",
  # "pennsylvania": "PA",
  # "rhode_island": "RI",
  # "south_carolina": "SC",
  # "south_dakota": "SD",
  # "tennessee": "TN",
  # "texas": "TX",
  # "utah": "UT",
  # "vermont": "VT",
  # "virginia": "VA",
  # "washington": "WA",
  # "west_virginia": "WV",
  # "wisconsin": "WI",
  # "wyoming": "WY",
}

tableFiles = {
    "CDE_AGENCIES.csv": "cde_agencies",
#     "AGENCIES.csv": "agencies",
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

doltRepo="/home/ubuntu/fbi-nibrs"

if __name__ == "__main__":
    os.chdir(doltRepo)
    # status = os.popen('dolt status')
    # print(status.read())
    print('#!/bin/bash')
    print('set -eo pipefail')
    for tableFile in tableFiles:
        tableName = tableFiles[tableFile]
        for state in states:
            abbr = states[state]
            for year in range(1991, 2019):

                csv_file = "/home/ubuntu/nibrs-data/{0}/{1}/{2}/transformed/{3}".format(state, year, abbr, tableFile)
                if not os.path.exists(csv_file):
                    continue
                # try:
#                     print("importing data for state: {0} year: {1} table: {2}".format(state, year, tableFile))
#                     update_table = os.popen(
#                         'dolt table import -u {} {}'.format(tableName, csv_file))
#                     print(update_table.read())
                    # add_table = os.popen('dolt add -A')
                print('echo \"{0}\"'.format(csv_file))
                print('dolt table import -u {} {}'.format(tableName, csv_file))
#     print('dolt add .')
                # except OSError as e:
                #     print("error importing data for state: {0} year: {1} table: {2} error: {3}".format(state, year, tableFile, e))


