import re
import subprocess
import time
import logging
import requests
import tempfile
from collections import defaultdict
from os import path, remove
from doltpy.etl import get_df_table_loader
from doltpy.core.dolt import Dolt
import pandas as pd
from typing import Callable
from unidecode import unidecode
from nltk.stem import PorterStemmer

logger = logging.getLogger(__name__)

CURR_DIR = path.dirname(path.abspath(__file__))
BZ2_FILE_NAME = 'enwiki-latest-pages-articles-multistream.xml.bz2'
DUMP_URL = 'https://dumps.wikimedia.your.org/enwiki/latest/{}'.format(BZ2_FILE_NAME)
DUMP_PATH = path.join(CURR_DIR, BZ2_FILE_NAME)
WIKIEXTRACTOR_PATH = path.join(CURR_DIR, 'wikiextractor/WikiExtractor.py')

WORD_USES = defaultdict(int)
WORDS_TO_STEM = defaultdict(int)

LINE_TRANS = str.maketrans('–’', "-\'")
WORD_SPLIT = re.compile(r'[^\w\-\'\.&]|[\'\-\'\.&\/_]{2,}')

FILTERS = {
    'none': re.compile(r'^[\w\.\-\/][\w\.\'\-\/&]*[\w\.\-]*$'),
    'no_numbers': re.compile(r'.*[0-9].*'),
    'ASCII_only': re.compile(r'^[a-z0-9\-][a-z0-9\.\'\-&]*[a-z0-9\.\-]$'),
    'no_abbreviations': re.compile(r'.*[&\.].*'),
    'strict': re.compile(r'^[a-z][a-z\'\-]*[a-z\.\']$')
}


def fetch_data(filter_type: str):
    if path.exists(DUMP_PATH):
        logging.info('Using existing XML dump.')
    else:
        logging.info('Fetching Wikipedia XML dump from URL {}'.format(DUMP_URL))
        r = requests.get(DUMP_URL, stream=True)
        with open(DUMP_PATH, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
        logging.info('Finished downloading XML dump')
    process_bz2(filter_type)


def process_bz2(filter_type: str):
    logging.info('Processing dump with filter: {}'.format(filter_type))
    start = time.time()
    assert_filter_exists(filter_type)

    with subprocess.Popen(
        'bzcat {} | {} --no_templates -o - -'.format(DUMP_PATH, WIKIEXTRACTOR_PATH),
        stdout=subprocess.PIPE,
        shell=True,
    ) as proc:
        for line in proc.stdout:
            line = line.decode('utf-8')
            line = line.translate(LINE_TRANS)
            if not is_line_tag(line):
                line = line.lower()
                for word in filter(None, WORD_SPLIT.split(line)):
                    if filter_type == 'stemmed':
                        add_to_dict(word, filter_type, WORDS_TO_STEM)
                    else:
                        add_to_dict(word, filter_type, WORD_USES)

    if filter_type == 'stemmed':
        stem_words()
    duration = (time.time() - start)/60
    logging.info('ET completed in %.1f minutes', duration)


def assert_filter_exists(filter_type: str):
    valid_filters = ['stemmed', 'convert_to_ASCII']
    valid_filters.extend([wf for wf in FILTERS])
    is_valid = filter_type in valid_filters
    invalid_msg = '{} is not a valid word filter. Valid filters are {}'.format(filter_type, ', '.join(valid_filters))
    assert is_valid, invalid_msg


def is_line_tag(line: str):
    return line[0:4] == '<doc' or '</doc>' in line


def add_to_dict(word: str, filter_type: str, word_dict: defaultdict):
    word, passed_filter = filter_word(word, filter_type)
    if passed_filter and len(word) > 0 and word is not None:
        word_dict[word] += 1


def filter_word(word: str, filter_type: str):
    word = remove_unwanted_punctuation(word)
    if not FILTERS.get('none').match(word):
        return word, False
    if filter_type == 'convert_to_ASCII':
        return unidecode(word), True
    if filter_type == 'stemmed' or filter_type == 'none':
        return word, True
    return word, passes_filter(filter_type, word)


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


def stem_words():
    logging.info('Stemming {} words'.format(len(WORDS_TO_STEM.items())))
    porter = PorterStemmer()
    for word, freq in WORDS_TO_STEM.items():
        stem = porter.stem(word)
        if len(stem) > 0:
            WORD_USES[stem] += freq


def get_df_builder(filter_type: str) -> Callable[[], pd.DataFrame]:
    def inner() -> pd.DataFrame:
        fetch_data(filter_type)
        logging.info('Successfully processed {} words from dump'.format(len(WORD_USES.items())))
        df = pd.DataFrame([{'word': word, 'frequency': frequency}
                          for word, frequency in WORD_USES.items()])
        return df.astype({'frequency': 'int'})

    return inner


def get_loaders(filter_type: str):
    return [get_df_table_loader('word_frequency', get_df_builder(filter_type), pk_cols=['word'], import_mode='replace')]


# DAG helpers
def create_and_push_branch(remote: str, branch_name: str):
    temp_dir = tempfile.mkdtemp()
    repo = Dolt(temp_dir)
    logging.info('Cloning repo from {} to {}'.format(remote, temp_dir))
    repo.clone(remote)
    logging.info('Creating branch {}'.format(branch_name))
    repo.create_branch(branch_name)
    logging.info('Pushing branch {} to origin'.format(branch_name))
    repo.push('origin', branch_name)


def cleanup():
    remove(DUMP_PATH)
    logging.info('Wikipedia dump removed from current directory')