#!/bin/bash
set -eo pipefail
workdir=$(pwd)
root=$(dirname "$0")
cd $root
ulimit -n 16384
go run . "$workdir"/index_summary.psv
cd "$workdir"
dolt init
dolt sql -q 'CREATE TABLE `entry_stats` (
  `host` LONGTEXT NOT NULL COMMENT '\''tag:0'\'',
  `prefix` LONGTEXT NOT NULL COMMENT '\''tag:1'\'',
  `status` LONGTEXT NOT NULL COMMENT '\''tag:2'\'',
  `mime_detected` LONGTEXT NOT NULL COMMENT '\''tag:3'\'',
  `languages` LONGTEXT NOT NULL COMMENT '\''tag:4'\'',
  `count` BIGINT COMMENT '\''tag:5'\'',
  `size` BIGINT COMMENT '\''tag:6'\'',
  PRIMARY KEY (`host`,`prefix`,`status`,`mime_detected`,`languages`)
)'
dolt table import -r entry_stats index_summary.psv
