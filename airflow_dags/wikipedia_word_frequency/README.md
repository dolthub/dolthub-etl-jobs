# Pulling Wikipedia word frequencies from XML dump into Dolt

DoltHub has a mapping of the word frequencies from all the English pages and articles in
Wikipedia. This is updated whenever there is a new XML dump (at least once or twice a
month).

## Filter Options

1. `raw`: filters out unwanted punctuation (provided by default)
2. `no-nums`: filters out words that contain numbers
3. `no-abbreviations`: filters out abbreviations (i.e. C.I.A. and R&D)
4. `ascii-only`: filters out words that contain non-ASCII characters
5. `strict`: uses all of the above filters with additional special character restrictions
6. `convert-to-ascii`: converts all characters to ASCII
7. `stemmed`: converts all words to their lemma

