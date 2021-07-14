#!/usr/local/bin/python3

import subprocess
import time
import logging
import requests
import math
import re
import csv
import nltk
import string
from collections import defaultdict
from os import path, remove
from pathlib import Path
from doltpy.etl import get_df_table_writer, get_dolt_loader, get_branch_creator, load_to_dolthub
import pandas as pd
from typing import Callable
from itertools import islice
from glob import glob
from heapq import merge
import argparse

logger = logging.getLogger(__name__)

REPO_PATH = 'Liquidata/wikipedia-word-frequency'
CURR_DIR = path.dirname(path.abspath(__file__))
WIKIEXTRACTOR_PATH = path.join(Path(CURR_DIR).parent, 'wikiextractor/WikiExtractor.py')
UNI_SHARD_LEN = 150000
BI_SHARD_LEN = 50000
TRI_SHARD_LEN = 15000

NGRAM_DICTS = {
    'unigram': defaultdict(lambda: defaultdict(int)),
    'bigram': defaultdict(lambda: defaultdict(int)),
    'trigram': defaultdict(lambda: defaultdict(int)),
}

LOWERED_NGRAM_DICTS = {
    'unigram': defaultdict(lambda: defaultdict(int)),
    'bigram': defaultdict(lambda: defaultdict(int)),
    'trigram': defaultdict(lambda: defaultdict(int)),
}

LINE_TRANS = str.maketrans('–’', "-\'")
WORD_SPLIT = re.compile(r'[^\w\-\'\.&]|[\'\-\'\.&\/_]{2,}')
NOT_PUNCTUATION = re.compile(r'.*[a-zA-Z0-9].*')
NON_WORD_PUNCT = re.sub(r'[.\'&-]', '', string.punctuation)


def fetch_data(dump_target: str):
    """
    Downloads Wikipedia XML dump for given date
    :param dump_target:
    :return:
    """
    bz2_file_name = 'enwiki-{}-pages-articles-multistream.xml.bz2'.format(dump_target)
    dump_url = 'https://dumps.wikimedia.your.org/enwiki/{}/{}'.format(dump_target, bz2_file_name)

    logging.info('Fetching Wikipedia XML dump from URL {}'.format(dump_url))
    r = requests.get(dump_url, stream=True)
    with open(bz2_file_name, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
    logging.info('Finished downloading XML dump')
    return process_bz2(bz2_file_name)


def process_bz2(bz2_file_name: str):
    """
    Goes through dump and aggregates each article, passing it to add_ngrams
    :param bz2_file_name:
    :return:
    """
    logging.info('Processing dump')

    assert path.exists(bz2_file_name), 'Dump file does not exist {}'.format(bz2_file_name)
    start = time.time()
    article = ''
    article_count = 0
    is_title = True

    with subprocess.Popen(
        'bzcat {} | {} --no_templates -o - -'.format(bz2_file_name, WIKIEXTRACTOR_PATH),
        stdout=subprocess.PIPE,
        shell=True,
    ) as proc:
        for line in proc.stdout:
            line = line.decode('utf-8')
            line = line.translate(LINE_TRANS)
            if get_line_tag(line) == 'end':
                article_count += 1
                add_ngrams(article, article_count)
                article = ''
                is_title = True
            if get_line_tag(line) == 'not-tag':
                if is_title:
                    article += line.strip() + '. '  # title should be its own sentence
                    is_title = False
                else:
                    article += line.strip() + ' '  # strip newlines
    # Create csvs for last shards
    write_to_csv(str(int(math.ceil(article_count / UNI_SHARD_LEN))), 'unigram')
    write_to_csv(str(int(math.ceil(article_count / BI_SHARD_LEN))), 'bigram')
    write_to_csv(str(int(math.ceil(article_count / TRI_SHARD_LEN))), 'trigram')
    duration = (time.time() - start) / 60
    logging.info('ET completed in %.1f minutes', duration)

    return article_count


def add_ngrams(article: str, article_count: int):
    """
    Write ngrams to csv after designated amount of articles
    Count ngrams for this article
    Add total and article counts from this article to total ngram counts
    :param article:
    :param article_count:
    :return:
    """
    if article_count % UNI_SHARD_LEN == 0:
        csv_num = int(article_count / UNI_SHARD_LEN)
        write_to_csv(str(csv_num), 'unigram')
    if article_count % BI_SHARD_LEN == 0:
        csv_num = int(article_count / BI_SHARD_LEN)
        write_to_csv(str(csv_num), 'bigram')
    if article_count % TRI_SHARD_LEN == 0:
        csv_num = int(article_count / TRI_SHARD_LEN)
        write_to_csv(str(csv_num), 'trigram')
    art_dicts = count_article_ngrams(article)
    add_article_ngram_counts(art_dicts)


def get_ngram_df_builder(ngram_name: str, lower: str) -> Callable[[], pd.DataFrame]:

    def inner() -> pd.DataFrame:
        df = pd.read_csv('all_{}s{}.csv'.format(ngram_name, lower))
        return df.astype({'total_count': 'int', 'article_count': 'int'})

    return inner


def get_counts_df_builder(date_string: str, article_total: int):
    uni_df = pd.read_csv('all_unigrams.csv')
    word_total = uni_df['total_count'].sum()

    def inner() -> pd.DataFrame:
        df = pd.DataFrame([{'dump_date': date_string,
                            'total_word_count': word_total,
                            'total_article_count': article_total}])
        return df.astype({'total_word_count': 'int', 'total_article_count': 'int'})

    return inner


def get_writers(date_string: str, article_count: int, lower=''):
    writers = []

    for ngram_name in NGRAM_DICTS.keys():
        logging.info('Starting merge for {}s'.format(ngram_name))
        merge_csvs(ngram_name, lower)
        logging.info('Successfully merged all {} {} csvs'.format(lower, ngram_name))
        table_name = ngram_name + '_counts'
        writers.append(get_df_table_writer(table_name,
                                           get_ngram_df_builder(ngram_name, lower),
                                           [ngram_name],
                                           import_mode='replace'))
    if len(lower) == 0:
        writers.append(get_df_table_writer('total_counts',
                                           get_counts_df_builder(date_string, article_count),
                                           ['dump_date'],
                                           import_mode='update'))

    return writers


def load(date_string: str, dump_target: str):
    """
    Gets case-sensitive and case-insensitive loader for each Wikipedia dump
    Each loader has a writer for each table
    :param date_string:
    :param dump_target:
    :return:
    """
    loaders = []
    article_count = fetch_data(dump_target)

    # Get case-sensitive ngrams
    writers = get_writers(date_string, article_count)
    message = 'Update case-sensitive ngrams for dump date {}'.format(date_string)
    loaders.append(get_dolt_loader(writers, True, message))
    loaders.append(get_branch_creator('{}/case-sensitive'.format(date_string)))

    # Get case-insensitive ngrams
    l_message = 'Update case-insensitive ngrams for dump date {}'.format(date_string)
    l_writers = get_writers(date_string, article_count, lower='_lower')
    loaders.append(get_dolt_loader(l_writers, True, l_message, '{}/case-insensitive'.format(date_string)))

    load_to_dolthub(loaders, clone=True, push=True, remote_name='origin', remote_url=REPO_PATH)


# ----------------------------------------------------------------------
# Load loading_helpers


def count_article_ngrams(article: str):
    """
    Counts ngrams for each tokenized article, adding sentence padding to bigrams
    :param article:
    :return:
    """
    art_dicts = {
        'unigram': defaultdict(int),
        'bigram': defaultdict(int),
        'trigram': defaultdict(int),
    }
    for sent in nltk.sent_tokenize(article):
        sent = normalize_sentence(sent)
        # Add article unigrams
        for word in filter(None, WORD_SPLIT.split(sent)):
            if len(word) > 0 and NOT_PUNCTUATION.match(word):
                art_dicts['unigram'][word] += 1
        sent_padding = '_START_ ' + sent + ' _END_'
        words = [word for word in filter(None, WORD_SPLIT.split(sent_padding)) if
                 len(word) > 0 and NOT_PUNCTUATION.match(word)]
        # Add article bigrams
        for word in zip(words, islice(words, 1, None)):
            art_dicts['bigram'][word] += 1
        # Add article trigrams
        for word in zip(words, islice(words, 1, None), islice(words, 2, None)):
            art_dicts['trigram'][word] += 1

    return art_dicts


def normalize_sentence(sent: str):
    # remove punctuation from end of sentence
    sent = sent[0:-1]
    # remove non-word punctuation (all punctuation except for .-&')
    sent = sent.translate(str.maketrans('', '', NON_WORD_PUNCT))
    # remove punctuation from beginning and end of words
    sent = re.sub(r'\b[-.&\']+\B|\B[-.,&\']+\b', '', sent)
    return sent


def add_article_ngram_counts(art_dicts: dict):
    for key, art_dict in art_dicts.items():
        ngram_dict = NGRAM_DICTS[key]
        lowered_dict = LOWERED_NGRAM_DICTS[key]
        for ngram, count in art_dict.items():
            if key != 'unigram':
                ngram = ' '.join(ngram)
            lowered_ngram = get_lowered_ngram(ngram)
            if ngram is not None:
                ngram_dict[ngram]['total_count'] += count
                ngram_dict[ngram]['article_count'] += 1
                lowered_dict[lowered_ngram]['total_count'] += count
                lowered_dict[lowered_ngram]['article_count'] += 1


def write_to_csv(csv_num: str, ngram_name: str):
    """
    Writes sorted ngrams to csv and clears ngrams dict to free memory
    :param csv_num:
    :param ngram_name:
    :return:
    """
    csv_name = '{}s_{}.csv'.format(ngram_name, csv_num)
    lowered_csv_name = '{}s_lower_{}.csv'.format(ngram_name, csv_num)
    ngrams = NGRAM_DICTS[ngram_name]
    lowered_ngrams = LOWERED_NGRAM_DICTS[ngram_name]

    logging.info('Writing {} {}s to {}'.format(len(ngrams.items()), ngram_name, csv_name))

    # Write ngrams to csv
    df = pd.DataFrame({ngram_name: ngram,
                      'total_count': ngrams[ngram]['total_count'],
                       'article_count': ngrams[ngram]['article_count']}
                      for ngram in sorted(ngrams.keys()))
    df.to_csv(csv_name, index=False)
    NGRAM_DICTS[ngram_name] = defaultdict(lambda: defaultdict(int))

    logging.info('Done writing {}. Moving on.'.format(csv_name))
    logging.info('Writing {} lowered {}s to {}'.format(len(lowered_ngrams.items()), ngram_name, lowered_csv_name))

    # Write lowered ngrams to csv
    lower_df = pd.DataFrame({ngram_name: ngram,
                       'total_count': lowered_ngrams[ngram]['total_count'],
                       'article_count': lowered_ngrams[ngram]['article_count']}
                      for ngram in sorted(lowered_ngrams.keys()))
    lower_df.to_csv(lowered_csv_name, index=False)
    LOWERED_NGRAM_DICTS[ngram_name] = defaultdict(lambda: defaultdict(int))

    logging.info('Done writing {}. Moving on.'.format(lowered_csv_name))


def merge_csvs(ngram_name: str, lower: str):
    """
    Takes sorted ngram csvs and merges them into one csv,
    aggregating counts for each ngram
    :param ngram_name:
    :param lower:
    :return:
    """
    paths = glob('{}s{}_*.csv'.format(ngram_name, lower))
    files = map(open, paths)
    output_file = open('all_{}s{}.csv'.format(ngram_name, lower), 'w')
    csv_writer = csv.writer(output_file)
    curr_ngram = ''
    line_to_write = []
    # Write header
    csv_writer.writerow([ngram_name, 'total_count', 'article_count'])
    for line in merge(*[decorated_file(f) for f in files]):
        if line[0] != curr_ngram:
            if len(curr_ngram) > 0:
                csv_writer.writerow(line_to_write)
            curr_ngram = line[0]
            line_to_write = line
        else:
            line_to_write[1] += line[1]
            line_to_write[2] += line[2]
    # Write last row
    csv_writer.writerow(line_to_write)
    # Remove csv chunks to free up storage
    for f in paths:
        remove(f)


def decorated_file(file):
    # Skip header but make sure columns are in right order
    header = next(file)
    header = header.split(',')
    assert header[1].strip() == 'total_count' and header[2].strip() == 'article_count', \
        'header columns do not match: {}, {}'.format(header[1], header[2])
    for line in file:
        line = line.split(',')
        line[1] = int(line[1])
        line[2] = int(line[2].strip())
        yield line


def get_line_tag(line: str):
    if line[0:4] == '<doc':
        return 'start'
    if '</doc>' in line:
        return 'end'
    return 'not-tag'


def get_lowered_ngram(ngram: str):
    """
    Gets case-insensitive ngrams and not lowering sentence padding _START_ and _END_
    :param ngram:
    :return:
    """
    if '_START_' in ngram:
        ngram = ngram.split(' ')
        lowered_words = ' '.join(ngram[1:]).lower()
        return '_START_ ' + lowered_words
    if '_END_' in ngram:
        ngram = ngram.split(' ')
        lowered_words = ' '.join(ngram[:-1]).lower()
        return lowered_words + ' _END_'
    return ngram.lower()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date-string', type=str, required=True)
    parser.add_argument('--dump-target', type=str, required=True)
    args = parser.parse_args()

    load(args.date_string, args.dump_target)
