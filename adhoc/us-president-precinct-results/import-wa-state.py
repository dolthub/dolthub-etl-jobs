from doltpy.core import Dolt
from doltpy.core.write import import_list

import mysql.connector

import csv
import re

from pprint import pprint

csv_2016 = '20161108_allstateprecincts.csv'
# csv_2020 = '20201103_allstateprecincts.csv'

wa_map = {
    'state': 'WASHINGTON',
    'state_postal': 'WA',
    'state_fips': '53',
    'state_icpsr': '73',
}

county_map = {
    'AD': {
        'name': 'ADAMS COUNTY',
        'fips': '001',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'AS': {
        'name': 'ASOTIN COUNTY',
        'fips': '003',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'BE': {
        'name': 'BENTON COUNTY',
        'fips': '005',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'CH': {
        'name': 'CHELAN COUNTY',
        'fips': '007',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'CM': {
        'name': 'CLALLAM COUNTY',
        'fips': '009',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'CR': {
        'name': 'CLARK COUNTY',
        'fips': '011',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'CU': {
        'name': 'COLUMBIA COUNTY',
        'fips': '013',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'CZ': {
        'name': 'COWLITZ COUNTY',
        'fips': '015',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'DG': {
        'name': 'DOUGLAS COUNTY',
        'fips': '017',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'FE': {
        'name': 'FERRY COUNTY',
        'fips': '019',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'FR': {
        'name': 'FRANKLIN COUNTY',
        'fips': '021',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'GA': {
        'name': 'GARFIELD COUNTY',
        'fips': '023',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'GR': {
        'name': 'GRANT COUNTY',
        'fips': '025',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'GY': {
        'name': 'GRAYS HARBOR COUNTY',
        'fips': '027',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'IS': {
        'name': 'ISLAND COUNTY',
        'fips': '029',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'JE': {
        'name': 'JEFFERSON COUNTY',
        'fips': '031',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'KI': {
        'name': 'KING COUNTY',
        'fips': '033',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'KP': {
        'name': 'KITSAP COUNTY',
        'fips': '035',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'KS': {
        'name': 'KITTITAS COUNTY',
        'fips': '037',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'KT': {
        'name': 'KLICKITAT COUNTY',
        'fips': '039',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'LE': {
        'name': 'LEWIS COUNTY',
        'fips': '041',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'LI': {
        'name': 'LINCOLN COUNTY',
        'fips': '043',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'MA': {
        'name': 'MASON COUNTY',
        'fips': '045',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'OK': {
        'name': 'OKANOGAN COUNTY',
        'fips': '047',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'PA': {
        'name': 'PACIFIC COUNTY',
        'fips': '049',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'PE': {
        'name': 'PEND OREILLE COUNTY',
        'fips': '051',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'PI': {
        'name': 'PIERCE COUNTY',
        'fips': '053',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'SJ': {
        'name': 'SAN JUAN COUNTY',
        'fips': '055',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'SK': {
        'name': 'SKAGIT COUNTY',
        'fips': '057',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'SM': {
        'name': 'SKAMANIA COUNTY',
        'fips': '059',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'SN': {
        'name': 'SNOHOMISH COUNTY',
        'fips': '061',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'SP': {
        'name': 'SPOKANE COUNTY',
        'fips': '063',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'ST': {
        'name': 'STEVENS COUNTY',
        'fips': '065',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'TH': {
        'name': 'THURSTON COUNTY',
        'fips': '067',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'WK': {
        'name': 'WAHKIAKUM COUNTY',
        'fips': '069',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'WL': {
        'name': 'WALLA WALLA COUNTY',
        'fips': '071',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'WM': {
        'name': 'WHATCOM COUNTY',
        'fips': '073',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'WT': {
        'name': 'WHITMAN COUNTY',
        'fips': '075',
        'ansi': None,
        'lat': None,
        'lon': None,
    },
    'YA': {
        'name': 'YAKIMA COUNTY',
        'fips': '077',
        'ansi': None,
        'lat': None,
        'lon': None,
    }
}

candidate_map = {
    'Hillary Clinton / Tim Kaine': 'HILLARY CLINTON',
    'Donald J. Trump / Michael R. Pence': 'DONALD TRUMP',
    'Alyson Kennedy / Osborne Hart': 'ALLYSON KENNEDY',
    'Gloria Estela La Riva / Eugene Puryear': 'GLORIA LA RIVA',
    'Jill Stein / Ajamu Baraka': 'JILL STEIN',
    'Darrell L. Castle / Scott N. Bradley': 'DARRELL CASTLE',
    'Gary Johnson / Bill Weld': 'GARY JOHNSON', 
}

repo = Dolt('/Users/timse/dolthub/dolt/us-president-precinct-results')

repo.sql_server()

connection = mysql.connector.connect(user='root', 
                                     host="127.0.0.1",
                                     port=3306,
                                     database='us_president_precinct_results')
cursor = connection.cursor(prepared=True)

# Need to turn autocommit on because mysql.connector turns it off by default
cursor.execute('SET @@autocommit = 1')

with open(csv_2016, newline='') as csvhandle:
    csvreader = csv.reader(csvhandle)

    header = next(csvreader)

    count = 0
    county_unique = {}
    precinct_unique = {}
    for row in csvreader:
        count += 1
        if ( count % 100 == 0 ):
            print("Processing row: %s" % count)
 
        if row[0] != 'President/Vice President':
            continue
        else:
            # We got a row with some presidntial precinct data
            county_code   = row[1]
            candidate     = row[2]
            precinct      = row[3]
            precinct_code = row[4]
            votes         = row[5]

            election_year = 2016
            stage         = 'gen'

            county_name = county_map[county_code]['name']

            # jurisdiction by convention is just the county without COUNTY at the end
            jurisdiction = re.sub('\s+COUNTY$', '', county_map[county_code]['name'])

            # Normalize the candidates to what exists in the db.
            if ( candidate_map.get(candidate) ):
                candidate = candidate_map.get(candidate)
            else:
                print("Candidate: %s not found. Insert a record into candidates table" % candidate)
                break

            if precinct == 'Total':
                continue

            # First time we run into a county, insert it into counties table
            if ( not county_unique.get(county_code) ):
                county_unique[county_code] = 1

                if county_map.get(county_code):
                    county = {**county_map[county_code], **wa_map}

                    # Don't duplicate an insert
                    query = "select count(*) from counties where name=%s and state='WASHINGTON'"
                    cursor.execute(query, (county_map[county_code]['name'],)) 

                    results = cursor.fetchall()

                    if ( results[0][0] == 0 ):
                        query_cols = ', '.join("`" + str(x) + "`" for x in sorted(county))
                        prepareds  = ', '.join("%s" for x in sorted(county))
                        query      = "insert into counties (%s) values (%s)" % (query_cols, prepareds)
                        values     =  [value for (key, value) in sorted(county.items())]
                        cursor.execute(query, values)

            # Now add the precincts
            if ( not precinct_unique.get(county_code) ):
                precinct_unique[county_code] = {}
                if ( not precinct_unique.get(county_code).get(precinct) ):
                    precinct_unique[precinct] = 1
                    
                    # Don't duplicate an insert
                    query = """
                        select count(*) from precincts where 
                        precinct = %s and 
                        county = %s and 
                        state = %s and 
                        jurisdiction =%s
                    """
                    cursor.execute(query, (precinct, county_name, 'WASHINGTON', jurisdiction))
                    results = cursor.fetchall()

                    if results[0][0] == 0: 
                        query = """
                            insert into precincts (precinct, county, state, jurisdiction) 
                            values (%s, %s, %s, %s)
                        """
                        cursor.execute(query, (precinct, county_name, 'WASHINGTON', jurisdiction))
            
            # Finally, add vote tallies
            # I can use replace here because there are no key constraints that depend on this table
            query = """
                replace into vote_tallies (election_year, 
                                           stage, 
                                           precinct, 
                                           county, 
                                           state, 
                                           jurisdiction, 
                                           candidate, 
                                           writein, 
                                           office, 
                                           vote_mode, 
                                           votes)
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, (election_year, 
                                   stage, 
                                   precinct, 
                                   county_name, 
                                   'WASHINGTON', 
                                   jurisdiction,
                                   candidate,
                                   0,
                                   'US PRESIDENT',
                                   'ELECTION DAY',
                                    votes))

cursor.close()
connection.close()

repo.sql_server_stop()