#!/usr/local/bin/python3

import shutil
import sys

from doltpy.core import Dolt, clone_repo
from subprocess import Popen, PIPE

sys.path.append('/Applications/Google\ Chrome.app/Contents/MacOS/')
CHROME = 'Google Chrome'

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
repo = clone_repo(repo_name, root)

documents_df = repo.read_table('documents')
documents_df['terms_raw'] = documents_df['terms_raw'].astype(str)
documents_df['privacy_raw'] = documents_df['privacy_raw'].astype(str)

for index, row in documents_df.iterrows():
    print(f'Processing {index}')
    documents_df.at[index, 'terms_raw']   = scrape_document(row['terms_url'])
    documents_df.at[index, 'privacy_raw'] = scrape_document(row['privacy_url'])

repo.import_df('documents', documents_df, ['product_id'])

if repo.repo_is_clean:
    print('No changes to repo. Exiting')
else:
    print('Commiting and pushing to DoltHub')
    repo.add_table_to_next_commit('documents')

    now = datetime.datetime.now()
    print(f'Latest data downloaded from {url} at {now}')
    repo.commit(f'Latest data downloaded from {url} at {now}')
    repo.push('origin', 'master')
