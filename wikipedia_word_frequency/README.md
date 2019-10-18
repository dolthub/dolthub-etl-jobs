# Pulling Wikipedia dump into Dolt

DoltHub has a mapping of the word frequencies from all the English pages and articles in
Wikipedia. This is updated whenever there is a new XML dump (at least once a
month, up to twice a month according to their website).

## Filter Options
What is unique about this ETL job is that you can provide different filtering options 
using the `--options` argument. The different filtering options include:

1. `raw`: filters out unwanted punctuation (provided by default)
2. `no-nums`: filters out words that contain numbers
3. `no-abbreviations`: filters out abbreviations (i.e. C.I.A. and R&D)
4. `ascii-only`: filters out words that contain non-ASCII characters
5. `strict`: uses all of the above filters with additional special character restrictions
6. `convert-to-ascii`: converts all characters to ASCII
7. `stemmed`: converts all words to their lemma

It's possible to use one or more of these filters as a string with the filter names separated
by commas. See example below.

## Manual Run

Install Git submodules:

```bash
git submodule init && git submodule update
```
If you would like to use the `convert-to-ascii` filter you will need to install `unidecode`:

```bash
pip install unidecode
```

If you would like to use the `stemmed` filter you will need to install `spacy`:

```bash
pip install -U spacy
python -m spacy download en_core_web_lg
```

To write to local Dolt manually run the following command:
```bash
$ dolt_load
    --commit
    --dolt-dir $HOME/word_frequency
    --message '10-10-19 update to word frequency'
    --branch filter-ascii
    --options 'ascii-only, convert-to-ascii'
    wikipedia_word_frequency.dolt_load.loaders
```
