import re
import subprocess
import time
import logging
import requests
import nltk
from collections import defaultdict, Counter
from os import path
from pathlib import Path
from doltpy.etl import get_df_table_writer, get_dolt_loader, get_branch_creator, dolthub_loader
import pandas as pd
from typing import Callable
from functools import reduce, partial
import operator
import string
from itertools import islice

logger = logging.getLogger(__name__)

CURR_DIR = path.dirname(path.abspath(__file__))
WIKIEXTRACTOR_PATH = path.join(Path(CURR_DIR).parent, 'wikiextractor/WikiExtractor.py')

NGRAM_DICTS = {
    'unigram': defaultdict(lambda: defaultdict(int)),
    'bigram': defaultdict(lambda: defaultdict(int)),
    'trigram': defaultdict(lambda: defaultdict(int)),
}

LINE_TRANS = str.maketrans('–’', "-\'")
NOT_PUNCTUATION = re.compile(r'.*[a-zA-Z0-9].*')
NON_WORD_PUNCT = re.sub(r'[.\'&-]', '', string.punctuation)


def fetch_data(dump_target: str):
    bz2_file_name = 'enwiki-{}-pages-articles-multistream.xml.bz2'.format(dump_target)
    dump_url = 'https://dumps.wikimedia.your.org/enwiki/{}/{}'.format(dump_target, bz2_file_name)
    dump_path = path.join(CURR_DIR, bz2_file_name)

    if path.exists(dump_path):
        logging.info('Using existing XML dump.')
    else:
        logging.info('Fetching Wikipedia XML dump from URL {}'.format(dump_url))
        r = requests.get(dump_url, stream=True)
        with open(dump_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
        logging.info('Finished downloading XML dump')
    return process_bz2(dump_path)


def process_bz2(dump_path: str):
    logging.info('Processing dump')
    start = time.time()
    article = ''
    article_count = 0
    is_title = True

    with subprocess.Popen(
        'bzcat {} | {} --no_templates -o - -'.format(dump_path, WIKIEXTRACTOR_PATH),
        stdout=subprocess.PIPE,
        shell=True,
    ) as proc:
        for line in proc.stdout:
            line = line.decode('utf-8')
            line = line.translate(LINE_TRANS)
            if get_line_tag(line) == 'end':
                article_count += 1
                add_ngrams(article)
                article = ''
                is_title = True
            if get_line_tag(line) == 'not-tag':
                if is_title:
                    article += line.strip() + '. '  # title should be its own sentence
                    is_title = False
                else:
                    article += line.strip() + ' '  # strip newlines
    duration = (time.time() - start) / 60
    logging.info('ET completed in %.1f minutes', duration)
    return article_count


def add_ngrams(article: str):
    # Count ngrams for this article
    art_dicts = count_article_ngrams(article)
    # Add total and article counts from this article to total ngram counts
    add_article_ngram_counts(art_dicts)


def count_article_ngrams(article: str):
    art_dicts = {
        'unigram': defaultdict(int),
        'bigram': defaultdict(int),
        'trigram': defaultdict(int),
    }
    # sent_tokenize is expensive but it's the fastest of other sentence tokenizers I found and it
    # doesn't seem like there's a way around using it if I want sentence boundaries
    for sent in nltk.sent_tokenize(article):
        sent = normalize_sentence(sent)
        # Add article unigrams
        for word in sent.split(' '):
            if len(word) > 0 and NOT_PUNCTUATION.match(word):
                art_dicts['unigram'][word] += 1
        sent_padding = '_START_ ' + sent + ' _END_'
        words = [word for word in sent_padding.split(' ') if len(word) > 0 and NOT_PUNCTUATION.match(word)]
        # Add article bigrams
        for word in zip(words, islice(words, 1, None)):
            art_dicts['bigram'][word] += 1
        # Add article trigrams
        for word in zip(words, islice(words, 1, None), islice(words, 2, None)):
            art_dicts['trigram'][word] += 1

    return art_dicts


def add_article_ngram_counts(art_dicts: dict):
    for key, art_counter in art_dicts.items():
        ngram_dict = NGRAM_DICTS[key]
        for ngram, count in art_counter.items():
            if key is not 'unigram':
                ngram = ' '.join(ngram)
            if ngram is not None:
                ngram_dict[ngram]['total_count'] += count
                ngram_dict[ngram]['article_count'] += 1


def normalize_sentence(sent: str):
    # remove punctuation from end of sentence
    sent = sent[0:-1]
    # remove non-word punctuation (all punctuation except for .-&')
    sent = sent.translate(str.maketrans('', '', NON_WORD_PUNCT))
    # remove punctuation from beginning and end of words
    sent = re.sub(r'\b[-.&\']+\B|\B[-.,&\']+\b', '', sent)
    return sent


def get_line_tag(line: str):
    if line[0:4] == '<doc':
        return 'start'
    if '</doc>' in line:
        return 'end'
    return 'not-tag'


# For getting case-insensitive ngrams and not lowering sentence boundaries _START_ and _END_
def get_lowered_ngram(ngram: str):
    if '_START_' in ngram:
        ngram = ngram.split(' ')
        lowered_words = ' '.join(ngram[1:]).lower()
        return '_START_ ' + lowered_words
    if '_END_' in ngram:
        ngram = ngram.split(' ')
        lowered_words = ' '.join(ngram[:-1]).lower()
        return lowered_words + ' _END_'
    return ngram.lower()


def get_filtered_dict(ngrams):
    f_ngrams = defaultdict(lambda: defaultdict(int))
    for ngram, count in ngrams.items():
        lowered = get_lowered_ngram(ngram)
        f_ngrams[lowered]['total_count'] += count['total_count']
        f_ngrams[lowered]['article_count'] += count['article_count']
    return f_ngrams


# DF builders
def get_ngram_df_builder(ngram_dict: dict, ngram_name: str, f_kind: str) -> Callable[[], pd.DataFrame]:

    def inner() -> pd.DataFrame:
        logging.info('Successfully processed {} {} ({}) from XML dump'.format(len(ngram_dict.items()),
                                                                              ngram_name+'s',
                                                                              f_kind,))
        # Importing trigram table takes forever
        df = pd.DataFrame([{ngram_name: ngram,
                            'total_count': counts['total_count'],
                            'article_count': counts['article_count']}
                          for ngram, counts in ngram_dict.items()])
        return df.astype({'total_count': 'int', 'article_count': 'int'})

    return inner


def get_counts_df_builder(date_string: str, article_total: int):
    word_total = reduce(operator.add, [value['total_count'] for value in NGRAM_DICTS['unigram'].values()], 0)

    def inner() -> pd.DataFrame:
        df = pd.DataFrame([{'dump_date': date_string,
                            'total_word_count': word_total,
                            'total_article_count': article_total}])
        return df.astype({'total_word_count': 'int', 'total_article_count': 'int'})

    return inner


# Gets list of writers for each loader
def get_writers(date_string: str, article_count: int, f_kind: str, ngram_dicts: dict):
    writers = []

    for ngram_name, ngram_dict in ngram_dicts.items():
        table_name = ngram_name + '_counts'
        writers.append(get_df_table_writer(table_name,
                                           get_ngram_df_builder(ngram_dict, ngram_name, f_kind),
                                           [ngram_name],
                                           import_mode='replace'))

    writers.append(get_df_table_writer('total_counts',
                                       get_counts_df_builder(date_string, article_count),
                                       ['dump_date'],
                                       import_mode='update'))
    return writers


# Gets list of loaders
def get_dolt_datasets(date_string: str, dump_target: str):
    loaders = []
    articles_count = fetch_data(dump_target)

    # get case-sensitive ngrams
    writers = get_writers(date_string, articles_count, 'case-sensitive', NGRAM_DICTS)
    message = 'Update case-sensitive ngram data for dump date {}'.format(date_string)
    loaders.append(get_dolt_loader(writers, True, message))
    loaders.append(get_branch_creator('{}/case-sensitive'.format(date_string)))

    # get case-insensitive ngrams
    f_ngram_dicts = {
        'unigram': get_filtered_dict(NGRAM_DICTS['unigram']),
        'bigram': get_filtered_dict(NGRAM_DICTS['bigram']),
        'trigram': get_filtered_dict(NGRAM_DICTS['trigram']),
    }
    f_message = 'Update case-insensitive ngram data for dump date {}'.format(date_string)
    f_writers = get_writers(date_string, articles_count, 'case-insensitive', f_ngram_dicts)
    loaders.append(get_dolt_loader(f_writers, True, f_message, '{}/case-insensitive'.format(date_string)))

    return loaders


# If you want to profile you can uncomment this and run:
# liquidata-etl-jobs$ python -m cProfile -o wiki-perf.cprof airflow_dags/wikipedia/ngrams/dolt_load.py
# and then to visualize, download pyprof2calltree and run:
# liquidata-etl-jobs$ pyprof2calltree -k -i wiki-perf.cprof

# WIKIPEDIA_NGRAMS_REPO = 'Liquidata/wikipedia-ngrams'
# dolthub_loader(loader_builder=partial(get_dolt_datasets, '7-20-19', '20190720'),
#                dolt_dir='/Users/taylor/Desktop/wikipedia-ngrams',
#                clone=False,
#                push=False,
#                remote_name='origin',
#                dry_run=False,
#                remote_url=WIKIPEDIA_NGRAMS_REPO)
