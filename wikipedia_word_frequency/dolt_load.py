import sys
from doltpy_etl import get_df_table_loader
import pandas as pd
import re
from collections import defaultdict
import subprocess
from typing import Callable
from os import path
import wget
import time
from multiprocessing import Pool, cpu_count
from unidecode import unidecode
import spacy
sp = spacy.load("en_core_web_lg")

FILE_NAME = './enwiki-latest-pages-articles-multistream.xml.bz2'
URL = 'https://dumps.wikimedia.your.org/enwiki/latest/'.format(FILE_NAME)
DIR = '/Users/taylor/Desktop/doltdev/liquidata-etl-jobs/wikipedia_word_frequency'

WORD_USES = defaultdict(int)

LINE_TRANS = str.maketrans('–’', "-\'")

FILTERS = {
    'word_split': re.compile(r'[^\w\-\'\.&]|[\'\-\'\.&\/_]{2,}'),
    'raw': re.compile(r'^[\w\.\-\/][\w\.\'\-\/&]*[\w\.\-]*$'),
    'no-nums': re.compile(r'.*[0-9].*'),
    'ascii-only': re.compile(r'^[a-z0-9\-][a-z0-9\.\'\-&]*[a-z0-9\.\-]$'),
    'no-abbreviations': re.compile(r'.*[&\.].*'),
    'strict': re.compile(r'^[a-z][a-z\'\-]*[a-z\.\']$')
}


def fetch_data():
    if path.exists(FILE_NAME):
        print('Using existing bz2 file.')
    else:
        print('Fetching bz2 file from URL {}'.format(URL))
        wget.download(URL, DIR)
        print('Finished downloading bz2 file')


def process_bz2(filter_string: str):
    process_count = cpu_count()
    start = time.time()

    fn = FILE_NAME
    pool = Pool(process_count)

    with subprocess.Popen(
        'bzcat %s | wikiextractor/WikiExtractor.py --no_templates -o - -' % fn,
        stdout=subprocess.PIPE,
        shell=True,
    ) as proc:
        for line in proc.stdout:
            # TODO: this currently takes up too much memory to complete the process
            pool.apply_async(extract_words, args=(filter_string, line), callback=log_word)
        pool.close()
        pool.join()
        print('ET completed in ', time.time() - start)

        # logic for stemmed filter:
        # if 'stemmed' in filter_string:
        #     sentence = sp(line)
        #     for word in sentence:
        #         if word.lemma_ == '-PRON-':
        #             word = word.text.lower()
        #         else:
        #             word = word.lemma_
        #         add_word_to_dictionary(word.lower(), filter_string)


def extract_words(filter_string, line):
    words = []
    append = words.append
    line = line.decode('utf-8')
    line = line.translate(LINE_TRANS)
    if is_not_line_tag(line):
        line = line.lower()
        for word in filter(None, FILTERS.get('word_split').split(line)):
            word, passes_filter = filter_word(word, filter_string)
            if passes_filter and word is not None:
                append(word)
    return words


def log_word(line):
    for word in line:
        WORD_USES[word] += 1


def is_not_line_tag(line: str):
    if line[0:4] == '<doc' and '>' in line:
        return False
    if '</doc>' in line:
        return False
    return True


def filter_word(word: str, filter_string):
    word = remove_unwanted_punctuation(word)
    if not FILTERS.get('raw').match(word):
        return word, False
    if 'convert-to-ascii' in filter_string:
        word = unidecode(word)
    if fails_filter(filter_string, 'no-nums', word):
        return word, False
    if fails_filter(filter_string, 'ascii-only', word):
        return word, False
    if fails_filter(filter_string, 'no-abbreviations', word):
        return word, False
    if fails_filter(filter_string, 'strict', word):
        return word, False

    return word, True


def fails_filter(filter_string, curr_filter, word):
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


def get_df_builder() -> Callable[[], pd.DataFrame]:
    def inner() -> pd.DataFrame:
        filter_string = ""
        if '--options' in sys.argv:
            options_index = sys.argv.index('--options')
            filter_string = sys.argv[options_index+1]
        process_bz2(filter_string)
        print('len', len(WORD_USES.items()))
        df = pd.DataFrame([{'word': word, 'frequency': frequency}
                          for word, frequency in WORD_USES.items()])
        return df.astype({'frequency': 'int'})

    return inner


loaders = [get_df_table_loader('word_frequency', get_df_builder(), pk_cols=['word'], import_mode='replace')]