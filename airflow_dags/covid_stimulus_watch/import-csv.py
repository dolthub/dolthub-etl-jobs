#!/usr/local/bin/python3

import csv
import datetime
import urllib.request
import io

from doltpy.core import Dolt
from doltpy.core.write import bulk_import

column_map = {
    'Recipient Company': 'recipient_company',
    'Parent Company': 'parent_company',
    'Award Date': 'award_date',
    'Award Type': 'award_type',
    'Grant Amount': 'grant_amount',
    'Face Loan Amount': 'face_loan_amount',
    'Awarding Agency': 'awarding_agency',
    'Program Name': 'program_name',
    'Award Details': 'award_details',
    'Data Source for Award': 'data_source_for_award',
    'Facility Name': 'facility_name',
    'Facility State': 'facility_state',
    'Facility City': 'facility_city',
    'Notes': 'notes',
    'Ownership Structure': 'ownership_structure',
    'Stock Ticker Symbol': 'stock_ticker_symbol',
    'Parent Headquarters State': 'parent_headquarters_state',
    'Parent Headquarters Country': 'parent_headquarters_country',
    'Parent Sector': 'parent_sector',
    'Parent Industry': 'parent_industry',
    'Parent Total Workforce at end of 2019': 'parent_total_workforce_2019',
    'Parent Latest Workforce Size': 'parent_total_workforce_current',
    'Parent Employment-related Penalties Since 2010': 'parent_employment_penalties_since_2010',
    'Parent Federal Corporate Income Tax Rate': 'parent_fed_income_tax_rate',
    'Parent Total Federal, State, and Local Subsidies Since 2010': 'parent_total_subsidies_since_2010',
    'Parent Government-contracting-related Penalties Since 2010': 'parent_gov_contract_penalties_since_2010',
    'Parent Environmental / Healthcare / Safety Penalties Since 2010': 'parent_env_health_safety_penalties_since_2010',
    'Parent Consumer Protection / Financial / Competition-related Penaties Since 2010': 'parent_consumer_protect_financial_competition_penalties_since_2010',
    'Parent Ratio of CEO Pay to that of Median Worker': 'parent_ratio_ceo_pay_avg_worker',
    'CEO Pay': 'ceo_pay',
    'Median Worker Pay': 'median_worker_pay',
    'Parent TARP Loans Received During Financial Crisis': 'parent_tarp_loans',
    'Employment Issues': 'employment_issues',
    'Tax/Subsidy Issues': 'tax_subsidy_issues',
    'Government Contracting Issues': 'government_contracting_issues',
    'Environment/Safety Issues': 'environment_safety_issues',
    'Consumer Protection Issues': 'consumer_protection_issues',
    'CEO Compensation Issues': 'ceo_compensation_issues',
    'POGO Link': 'pogo_link',
    'Just Capital Link': 'just_capital_link',
    'Follow The Money Link': 'follow_the_money_link', 
    'Violation Tracker Link': 'violation_tracker_link',
    'Subsidy Tracker Link': 'subsidy_tracker_link'
}

pks = ['recipient_company', 'parent_company', 'grant_amount', 'face_loan_amount']

url = 'https://data.covidstimuluswatch.org/prog.php?&detail=export_csv'
outcsvfile = 'dolt-import.csv'

tmpdir = '.'

org = 'Liquidata'
repo_name = 'covid-stimulus-watch'

target = f'{org}/{repo_name}'

print(f'Cloning {target}')
repo = Dolt.clone(target, '.')

print(f'Reading {url}')
with urllib.request.urlopen(url) as response, open(outcsvfile, "w") as outcsvhandle:
    csvreader = csv.reader(io.StringIO(response.read().decode('utf-8')))
    csvwriter = csv.writer(outcsvhandle)

    print('Converting to Dolt format')
    header = next(csvreader)

    header_out = []
    for col in header:
        if column_map.get(col):
            header_out.append(column_map.get(col))
        else:
            raise Exception(f'{col} not found in column map')
        
    csvwriter.writerow(header_out)

    duplicates = {}
    for row in csvreader:
        # Convert dollars into numbers 
        row = [val.replace('$', '') for val in row]
        row = [val.replace(',', '') for val in row]

        # Convert yes/no into boolean
        for index, value in enumerate(row):
            if value == 'yes':
                row[index] = 1
            elif value == 'no':
                row[index] = 0
            else:
                continue
            
        # Convert dates
        award_date = datetime.datetime.strptime(row[2], '%Y%m%d')
        row[2] = award_date.date()

        # Remove duplicate rows
        company = row[0]
        parent  = row[1]
        grant   = row[4]
        loan    = row[5]

        if duplicates.get(company, {}).get(parent, {}).get(grant, {}).get(loan):
            continue
        else:
            duplicates[company] = {}
            duplicates[company][parent]= {}
            duplicates[company][parent][grant] = {}
            duplicates[company][parent][grant][loan] = 1

        # Insert parent comapny as company if parent does not exist
        if not parent:
            row[1]=row[0]
            
        csvwriter.writerow(row)

print('Importing to Dolt')
bulk_import(repo, 'recipients', open(outcsvfile), pks, 'replace')

if repo.status().is_clean:
    print('No changes to repo. Exiting')
else:
    print('Commiting and pushing to DoltHub')
    repo.add('recipients')

    now = datetime.datetime.now()
    print(f'Latest data downloaded from {url} at {now}')
    repo.commit(f'Latest data downloaded from {url} at {now}')
    repo.push('origin', 'master')
