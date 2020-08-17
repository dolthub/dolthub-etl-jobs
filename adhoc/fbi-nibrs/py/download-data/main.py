import requests
import zipfile
import os
import argparse

def download_zip(state, year, url, path):
    try:
        r = requests.get(url, stream=True)
        if r.ok:
            print("Downloading data from:", url)
            if not os.path.exists(os.path.dirname(path)):
                print("Creating dir:", os.path.dirname(path))
                os.makedirs(os.path.dirname(path))
            print("Writing data to:", path)
            with open(path, 'wb') as f:
                for ch in r.iter_content(chunk_size=1024):
                    if ch:
                        f.write(ch)
        else:
            return
    except Exception as e:
        print("Something went wrong downloading data for state: %s, year: %d" % (state, year))
        print("Error:", e)
        pass


def unzip_file(file_path, output_dir):
    try:
        if os.path.isfile(file_path):
            print("Unzipping:", file_path)
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(output_dir)
        else:
            return
    except Exception as e:
        print("Something went wrong unzipping file:", file_path)
        print("Error:", e)
        pass


def get_years(start, end):
    years = []
    for year in range(start, end+1):
        years.append(year)
    return years


nibrs_base_url_pre_2016 = "http://s3-us-gov-west-1.amazonaws.com/cg-d3f0433b-a53e-4934-8b94-c678aa2cbaf3"
nibrs_base_url_current = "http://s3-us-gov-west-1.amazonaws.com/cg-d4b776d0-d898-4153-90c8-8336f86bdfec"

states = {
  "Alabama": "AL",
  "Alaska": "AK",
  "Arizona": "AZ",
  "Arkansas": "AR",
  "California": "CA",
  "Colorado": "CO",
  "Connecticut": "CT",
  "Delaware": "DE",
  "Florida": "FL",
  "Georgia": "GA",
  "Hawaii": "HI",
  "Idaho": "ID",
  "Illinois": "IL",
  "Indiana": "IN",
  "Iowa": "IA",
  "Kansas": "KS",
  "Kentucky": "KY",
  "Louisiana": "LA",
  "Maine": "ME",
  "Maryland": "MD",
  "Massachusetts": "MA",
  "Michigan": "MI",
  "Minnesota": "MN",
  "Mississippi": "MS",
  "Missouri": "MO",
  "Montana": "MT",
  "Nebraska": "NE",
  "Nevada": "NV",
  "New Hampshire": "NH",
  "New Jersey": "NJ",
  "New Mexico": "NM",
  "New York": "NY",
  "North Carolina": "NC",
  "North Dakota": "ND",
  "Ohio": "OH",
  "Oklahoma": "OK",
  "Oregon": "OR",
  "Pennsylvania": "PA",
  "Rhode Island": "RI",
  "South Carolina": "SC",
  "South Dakota": "SD",
  "Tennessee": "TN",
  "Texas": "TX",
  "Utah": "UT",
  "Vermont": "VT",
  "Virginia": "VA",
  "Washington": "WA",
  "West Virginia": "WV",
  "Wisconsin": "WI",
  "Wyoming": "WY",
  "District of Columbia": "DC",
  "American Samoa": "AS",
  "Canal Zone": "CZ",
  "Guam": "GM",
  "U.S. Virgin Islands": "VI",
  "Mariana Islands": "MP",
  "Other": "OT",
  "Testy McTesterstate": "TS",
  "Federal": "FS",
  "Puerto Rico": "PR",
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('dataPath', metavar='dataPath', type=str, nargs='+', help='path where data should be downloaded')
    args = parser.parse_args()
    dataPath = args.dataPath[0]

    years = get_years(1991, 2018)
    for year in years:
        for state in states:
            abbr = states[state]
            if year < 2016:
                baseURL = nibrs_base_url_pre_2016
            else:
                baseURL = nibrs_base_url_current
            url = "{0}/{1}/{2}-{1}.zip".format(baseURL, year, abbr)
            formatted_state = state.replace(" ", "_")
            file_name = "{0}-{1}.zip".format(abbr, year)
            download_base_dir = os.path.join(dataPath, "{0}/{1}".format(formatted_state.lower(), year))
            download_path = os.path.join(download_base_dir, file_name)
            download_zip(state, year, url, download_path)
            unzip_file(download_path, download_base_dir)
