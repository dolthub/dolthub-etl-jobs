import re
import subprocess
import time
import logging
import requests
import tempfile
from collections import defaultdict
from os import path
from doltpy.etl import get_df_table_loader, load_to_dolt
from doltpy.core.dolt import Dolt
import pandas as pd
from typing import Callable, List
from unidecode import unidecode
from nltk.stem import PorterStemmer

DoltTableLoader = Callable[[Dolt], str]

logger = logging.getLogger(__name__)

CURR_DIR = path.dirname(path.abspath(__file__))
BZ2_FILE_NAME = 'enwiki-latest-pages-articles-multistream.xml.bz2'
DUMP_URL = 'https://dumps.wikimedia.your.org/enwiki/latest/{}'.format(BZ2_FILE_NAME)
WIKIEXTRACTOR_PATH = path.join(CURR_DIR, 'wikiextractor/WikiExtractor.py')

WORD_USES = defaultdict(int)

LINE_TRANS = str.maketrans('–’', "-\'")
WORD_SPLIT = re.compile(r'[^\w\-\'\.&]|[\'\-\'\.&\/_]{2,}')

FILTERS = {
    'none': re.compile(r'^[\w\.\-\/][\w\.\'\-\/&]*[\w\.\-]*$'),
    'no_numbers': re.compile(r'.*[0-9].*'),
    'ASCII_only': re.compile(r'^[a-z0-9\-][a-z0-9\.\'\-&]*[a-z0-9\.\-]$'),
    'no_abbreviations': re.compile(r'.*[&\.].*'),
    'strict': re.compile(r'^[a-z][a-z\'\-]*[a-z\.\']$')
}

FILTER_NAMES = ['no_numbers', 'ASCII_only', 'no_abbreviations', 'strict', 'convert_to_ASCII', 'stemmed']


def fetch_data():
    logging.info('Fetching Wikipedia XML dump from URL {}'.format(DUMP_URL))
    r = requests.get(DUMP_URL, stream=True)
    with open(BZ2_FILE_NAME, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
    logging.info('Finished downloading XML dump')
    process_bz2()


def process_bz2():
    logging.info('Processing XML dump')
    start = time.time()

    with subprocess.Popen(
        'bzcat {} | {} --no_templates -o - -'.format(BZ2_FILE_NAME, WIKIEXTRACTOR_PATH),
        stdout=subprocess.PIPE,
        shell=True,
    ) as proc:
        for line in proc.stdout:
            line = line.decode('utf-8')
            line = line.translate(LINE_TRANS)
            if not is_line_tag(line):
                line = line.lower()
                for word in filter(None, WORD_SPLIT.split(line)):
                    word = remove_unwanted_punctuation(word)
                    if FILTERS.get('none').match(word):
                        WORD_USES[word] += 1

    duration = (time.time() - start)/60
    logging.info('ET completed in %.1f minutes', duration)


def is_line_tag(line: str):
    return line[0:4] == '<doc' or '</doc>' in line


def remove_unwanted_punctuation(word: str):
    punctuation = ".'&"
    while len(word) > 0 and (word[0] in punctuation or word[-1] in punctuation):
        if word[0] in punctuation:
            word = word[1:]
        elif word[-1] in punctuation:
            word = word[:-1]
    return word


def passes_filter(filter_type: str, word: str):
    if filter_type[:2] == 'no':
        return not FILTERS.get(filter_type).match(word)
    return FILTERS.get(filter_type).match(word)


def apply_filter(filter_type: str, word: str, porter: PorterStemmer):
    if filter_type == 'stemmed':
        return porter.stem(word), True
    if filter_type == 'convert_to_ASCII':
        return unidecode(word), True
    return word, passes_filter(filter_type, word)


def get_filter_dict(filter_type: str):
    filter_dict = defaultdict(int)
    porter = PorterStemmer()
    for word, frequency in WORD_USES.items():
        word, passed_filter = apply_filter(filter_type, word, porter)
        if passed_filter and word is not None and len(word) > 0:
            filter_dict[word] += frequency
    return filter_dict


def get_master_df_builder() -> Callable[[], pd.DataFrame]:
    def inner() -> pd.DataFrame:
        fetch_data()
        logging.info('Successfully processed {} words from dump'.format(len(WORD_USES.items())))
        df = pd.DataFrame([{'word': word, 'frequency': frequency}
                          for word, frequency in WORD_USES.items()])
        return df.astype({'frequency': 'int'})

    return inner


def get_filter_df_builder(filter_type: str) -> Callable[[], pd.DataFrame]:
    def inner() -> pd.DataFrame:
        filter_dict = get_filter_dict(filter_type)
        logging.info('Successfully processed {} words with {} filter'.format(len(filter_dict.items()), filter_type))
        df = pd.DataFrame([{'word': word, 'frequency': frequency}
                          for word, frequency in filter_dict.items()])
        return df.astype({'frequency': 'int'})

    return inner


def load_and_push(repo: Dolt,
                  loaders: List[DoltTableLoader],
                  branch_date: str,
                  remote: str,
                  branch: str,
                  message: str):
    commit = True
    logger.info(
        '''Commencing to load to DoltHub with the following options:
                        - dolt_dir  {dolt_dir}
                        - commit    {commit}
                        - message   {message}
                        - branch    {branch}
                        - remote    {remote}
                        - push      {push}
        '''.format(dolt_dir=repo.repo_dir,
                   commit=commit,
                   message=message,
                   branch=branch,
                   remote=remote,
                   push=True))
    load_to_dolt(repo, loaders, commit, message, branch)
    logger.info('Pushing changes to remote {} on branch {}'.format(remote, branch))
    repo.push('origin', branch)
    current_branch = repo.get_current_branch()
    if current_branch != branch_date and branch != 'master':
        repo.checkout(branch_date)


def create_and_push_branch(repo: Dolt, branch_name: str):
    logging.info('Creating branch {}'.format(branch_name))
    repo.create_branch(branch_name)
    logging.info('Pushing branch {} to origin'.format(branch_name))
    repo.push('origin', branch_name)


def load_wikipedia_branches(remote: str, branch_date: str):
    temp_dir = tempfile.mkdtemp()
    repo = Dolt(temp_dir)
    logging.info('Cloning repo from {} to {}'.format(remote, temp_dir))
    repo.clone(remote)

    # handle master
    master_loaders = [get_df_table_loader('word_frequency',
                                          get_master_df_builder(),
                                          pk_cols=['word'],
                                          import_mode='replace')]
    message = 'Update Wikipedia word frequencies for {} XML dump'.format(branch_date)
    load_and_push(repo, master_loaders, branch_date, remote, 'master', message)

    # create and push base branch
    create_and_push_branch(repo, branch_date)

    # handle filter branches
    for filter_name in FILTER_NAMES:
        filter_loaders = [get_df_table_loader('word_frequency',
                                              get_filter_df_builder(filter_name),
                                              pk_cols=['word'],
                                              import_mode='replace')]
        branch_name = '{}/filter_{}'.format(branch_date, filter_name)
        message = 'Update Wikipedia word frequencies with {} filter for {} XML dump'.format(filter_name, branch_date)
        load_and_push(repo, filter_loaders, branch_date, remote, branch_name, message)
