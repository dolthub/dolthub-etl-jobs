common-crawl-privacy-policies
=============================

An initial pass at filtering all common crawl index entries looking for privacy
policies and terms of use across the web. The resulting output is meant for
ingesting into a Dolt database.  This is dead simple right now. 

Schema
------

```sql
CREATE TABLE `matching_index_entries` (
  `collection` TEXT NOT NULL COMMENT 'tag:10077',
  `segment` TEXT NOT NULL COMMENT 'tag:9971',
  `index_key` TEXT NOT NULL COMMENT 'tag:3478',
  `timestamp` TEXT NOT NULL COMMENT 'tag:12114',
  `url` TEXT NOT NULL COMMENT 'tag:5934',
  `mime` TEXT COMMENT 'tag:6385',
  `mime_detected` TEXT COMMENT 'tag:3234',
  `status` TEXT COMMENT 'tag:2638',
  `digest` TEXT COMMENT 'tag:14847',
  `length` TEXT COMMENT 'tag:9449',
  `offset` TEXT COMMENT 'tag:4328',
  `filename` TEXT COMMENT 'tag:11789',
  `charset` TEXT COMMENT 'tag:12773',
  `languages` TEXT COMMENT 'tag:14247',
  `redirect` TEXT COMMENT 'tag:7665',
  PRIMARY KEY (`collection`,`segment`,`index_key`,`timestamp`,`url`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
```

Example
-------

```sh
$ ./common-crawl-privacy-policies loadsegments
...
$ ls -1 *.sql.gz | sort
CC-MAIN-2020-16-00001.sql.gz
CC-MAIN-2020-16-00002.sql.gz
CC-MAIN-2020-16-00003.sql.gz
CC-MAIN-2020-16-00004.sql.gz
CC-MAIN-2020-16-00005.sql.gz
...
$ for f in `ls -1 *.sql.gz | sort`; do zcat $f | dolt sql; done
```

TODO
----

* Error handling with retries instead of `panic`s
* Check point incremental work&mdash;stage downloaded files and per-step outputs
* Better filtering and heuristics of relevant index entries
* Fetch WARC records for relevant index entries
* Support processing multiple crawls
* Support WARC revisit entries after WARC records are supported
