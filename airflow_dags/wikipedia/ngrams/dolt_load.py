import re
import subprocess
import time
import logging
import requests
import nltk
from collections import defaultdict
from os import path
from pathlib import Path
from doltpy.etl import get_df_table_writer, get_dolt_loader, get_branch_creator
import pandas as pd
from typing import Callable
from functools import reduce
from nltk.util import ngrams

logger = logging.getLogger(__name__)

date = '20190720'
CURR_DIR = path.dirname(path.abspath(__file__))
BZ2_FILE_NAME = 'enwiki-{}-pages-articles-multistream.xml.bz2'.format(date)
DUMP_URL = 'https://dumps.wikimedia.your.org/enwiki/{}/{}'.format(date, BZ2_FILE_NAME)
DUMP_PATH = path.join(CURR_DIR, BZ2_FILE_NAME)
WIKIEXTRACTOR_PATH = path.join(Path(CURR_DIR).parent, 'wikiextractor/WikiExtractor.py')


def ngram_dict_def():
    return defaultdict(int)


NGRAM_DICTS = {
    'unigram': defaultdict(ngram_dict_def),
    'bigram': defaultdict(ngram_dict_def),
    'trigram': defaultdict(ngram_dict_def),
}


NAME_MAP = {
    1: 'uni',
    2: 'bi',
    3: 'tri'
}

LINE_TRANS = str.maketrans('–’', "-\'")
NOT_PUNCTUATION = re.compile(r'.*[a-zA-Z0-9].*')


def fetch_data():
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
    return process_bz2()


def process_bz2():
    logging.info('Processing dump')
    start = time.time()
    article = ''
    article_count = 0
    word_count = 0

    with subprocess.Popen(
        'bzcat {} | {} --no_templates -o - -'.format(DUMP_PATH, WIKIEXTRACTOR_PATH),
        stdout=subprocess.PIPE,
        shell=True,
    ) as proc:
        for line in proc.stdout:
            line = line.decode('utf-8')
            line = line.translate(LINE_TRANS)
            if is_line_tag(line) == 'end':
                article_count += 1
                art_word_count = add_ngrams(article)
                word_count += art_word_count
                article = ''
            if is_line_tag(line) == 'not-tag':
                article += line
    duration = (time.time() - start) / 60
    logging.info('ET completed in %.1f minutes', duration)
    return article_count, word_count


def add_ngrams(article: str):
    word_count = 0
    for n in range(1, 4):
        art_dict = defaultdict(int)
        ngram_dict = NGRAM_DICTS.get(NAME_MAP.get(n) + 'gram')
        for sent in nltk.sent_tokenize(article):
            # this is where I'd filter things out - may be better order of things
            tokens = [token for token in nltk.word_tokenize(sent) if NOT_PUNCTUATION.match(token)]
            if n == 1:
                word_count += len(tokens)
            for ngram in ngrams(tokens,
                                n,
                                pad_left=True,
                                pad_right=True,
                                left_pad_symbol='_START_',
                                right_pad_symbol='_END_'):
                # TODO: don't want double-padded trigrams (i.e. _START_, _START_, word)
                ngram_str = ' '.join(ngram)
                art_dict[ngram_str] += 1
        for ngram, count in art_dict.items():
            ngram_dict[ngram]['total_count'] += count
            ngram_dict[ngram]['article_count'] += 1
    return word_count


def is_line_tag(line: str):
    if line[0:4] == '<doc':
        return 'start'
    if '</doc>' in line:
        return 'end'
    return 'not-tag'


# keeping a word count might be faster than this
# def get_total_word_count():
#     unigrams = NGRAM_DICTS.get('unigram')
#     values = [value['total_count'] for value in unigrams.values()]
#     return reduce(lambda x, value: x + value, values, 0)


def get_ngram_df_builder(ngram_name: str) -> Callable[[], pd.DataFrame]:
    ngram_dict = NGRAM_DICTS.get(ngram_name)

    def inner() -> pd.DataFrame:
        logging.info('Successfully processed {} {} from XML dump'.format(len(ngram_dict.items()), ngram_name+'s'))
        df = pd.DataFrame([{ngram_name: ngram,
                            'total_count': counts['total_count'],
                            'article_count': counts['article_count']}
                          for ngram, counts in ngram_dict.items()])
        return df.astype({'total_count': 'int', 'article_count': 'int'})

    return inner


def get_counts_df_builder(date_string: str, article_total: int, word_total: int):
    def inner() -> pd.DataFrame:
        df = pd.DataFrame([{'dump_date': date_string,
                            'total_word_count': word_total,
                            'total_article_count': article_total}])
        return df.astype({'total_word_count': 'int', 'total_article_count': 'int'})

    return inner


def get_writers(date_string: str):
    writers = []
    article_total, word_total = fetch_data()
    for ngram_name in NGRAM_DICTS:
        table_name = ngram_name + '_counts'
        ngram_writer = get_df_table_writer(
            table_name,
            get_ngram_df_builder(ngram_name),
            [ngram_name],
            import_mode='replace'
        )
        writers.append(ngram_writer)

    counts_writer = get_df_table_writer(
        'total_counts',
        get_counts_df_builder(date_string, article_total, word_total),
        ['dump_date'],
        import_mode='update'
    )
    writers.append(counts_writer)
    return writers


def get_dolt_datasets(date_string: str):
    date_string = '11-5-19'
    loaders = []
    writers = get_writers(date_string)
    message = 'Update 4000 articles ngram data for dump date {}'.format(date_string)
    loaders.append(get_dolt_loader(writers, True, message))
    # loaders.append(get_branch_creator(date_string))
    return loaders


