import sys
import re
import subprocess
import time
import logging
import requests
from doltpy.etl import get_df_table_loader
import pandas as pd
from collections import defaultdict
from typing import Callable
from os import path
from unidecode import unidecode
import spacy
sp = spacy.load("en_core_web_lg")

logger = logging.getLogger(__name__)

CURR_DIR = path.dirname(path.abspath(__file__))
BZ2_FILE_NAME = 'enwiki-latest-pages-articles-multistream.xml.bz2'
DUMP_URL = 'https://dumps.wikimedia.your.org/enwiki/latest/{}'.format(BZ2_FILE_NAME)
DUMP_PATH = path.join(CURR_DIR, BZ2_FILE_NAME)
WIKIEXTRACTOR_PATH = path.join(CURR_DIR, 'wikiextractor/WikiExtractor.py')

WORD_USES = defaultdict(int)

LINE_TRANS = str.maketrans('–’', "-\'")

FILTERS = {
    'word_split': re.compile(r'[^\w\-\'\.&]|[\'\-\'\.&\/_]{2,}'),
    'raw': re.compile(r'^[\w\.\-\/][\w\.\'\-\/&]*[\w\.\-]*$'),
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
    logging.info("Processing dump with filter: {}".format(sys.argv[3]))
    start = time.time()
    filter_string = 'no_filter' if filter_type not in FILTERS else FILTERS[filter_type]

    with subprocess.Popen(
        'bzcat {} | {} --no_templates -o - -'.format(DUMP_PATH, WIKIEXTRACTOR_PATH),
        stdout=subprocess.PIPE,
        shell=True,
    ) as proc:
        for line in proc.stdout:
            line = line.decode('utf-8')
            line = line.translate(LINE_TRANS)
            if is_not_line_tag(line):
                if 'stemmed' in filter_string:
                    sentence = sp(line)
                    for word in sentence:
                        word = word.lemma_
                        add_to_dict(word, filter_string)
                else:
                    line = line.lower()
                    for word in filter(None, FILTERS.get('word_split').split(line)):
                        add_to_dict(word, filter_string)

    logging.info('ET completed in {}'.format(time.time() - start))


def add_to_dict(word, filter_string):
    word, passes_filter = filter_word(word, filter_string)
    if passes_filter and len(word) > 0 and word is not None:
        WORD_USES[word] += 1


def is_not_line_tag(line: str):
    if line[0:4] == '<doc' and '>' in line:
        return False
    if '</doc>' in line:
        return False
    return True


def filter_word(word: str, filter_string: str):
    word = remove_unwanted_punctuation(word)
    if 'stemmed' in filter_string and word == '-PRON-':
        return word, True
    if not FILTERS.get('raw').match(word):
        return word, False
    if 'convert_to_ASCII' in filter_string:
        word = unidecode(word)
    if fails_filter(filter_string, 'no_numbers', word):
        return word, False
    if fails_filter(filter_string, 'ASCII_only', word):
        return word, False
    if fails_filter(filter_string, 'no_abbreviations', word):
        return word, False
    if fails_filter(filter_string, 'strict', word):
        return word, False

    return word, True


def fails_filter(filter_string: str, curr_filter: str, word: str):
    if curr_filter in filter_string:
        if curr_filter[:2] == 'no':
            return FILTERS.get(curr_filter).match(word)
        return not FILTERS.get(curr_filter).match(word)
    return False


def remove_unwanted_punctuation(word: str):
    punctuation = ".'&"
    while len(word) > 0 and (word[0] in punctuation or word[-1] in punctuation):
        if word[0] in punctuation:
            word = word[1:]
        elif word[-1] in punctuation:
            word = word[:-1]

    return word


def get_df_builder(filter_type: str) -> Callable[[], pd.DataFrame]:
    def inner() -> pd.DataFrame:
        fetch_data(filter_type)
        logging.info('Successfully added {} words'.format(len(WORD_USES.items())))
        df = pd.DataFrame([{'word': word, 'frequency': frequency}
                          for word, frequency in WORD_USES.items()])
        return df.astype({'frequency': 'int'})

    return inner


def get_loaders(filter_type: str):
    return [get_df_table_loader('word_frequency', get_df_builder(filter_type), pk_cols=['word'], import_mode='replace')]
