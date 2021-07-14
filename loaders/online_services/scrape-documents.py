#!/usr/local/bin/python3

import datetime
import shutil
import sys

from doltpy.core import Dolt
from doltpy.core.read import read_table
from doltpy.core.write import import_df

from subprocess import Popen, PIPE

# Uncomment to test locally on a Mac
# sys.path.append('/Applications/Google\ Chrome.app/Contents/MacOS/')
# CHROME = 'Google Chrome'
CHROME = '/usr/bin/google-chrome-stable'

def scrape_document(url):
    headless_chrome = [CHROME,
                       '--headless',
                       '--disable-gpu',
                       '--dump-dom',
                       '--crash-dumps-dir=/tmp',
                       url]
    
    process = Popen(headless_chrome, stdout=PIPE)
    (output, err) = process.communicate()
    exit_code = process.wait()

    return output

repo_name = 'Liquidata/online-services'
root = '.'
repo = Dolt.clone(repo_name, root)

documents_df = read_table(repo, 'documents')
documents_df['terms_raw'] = documents_df['terms_raw'].astype(str)
documents_df['privacy_raw'] = documents_df['privacy_raw'].astype(str)

for index, row in documents_df.iterrows():
    print(f'Processing {index}')
    documents_df.at[index, 'terms_raw']   = scrape_document(row['terms_url'])
    documents_df.at[index, 'privacy_raw'] = scrape_document(row['privacy_url'])

import_df(repo, 'documents', documents_df, ['product_id'])

if repo.status().is_clean:
    print('No changes to repo. Exiting')
else:
    print('Commiting and pushing to DoltHub')
    repo.add('documents')

    now = datetime.datetime.now()
    print(f'Latest documents downloaded {now}')
    repo.commit(f'Latest data downloaded {now}')
    repo.push('origin', 'master')
