#!/bin/bash
set -eo pipefail
workdir=$(pwd)
root=$(dirname "$0")
cd $root
ulimit -n 16384
go run . "$workdir"/index_summary.psv
cd "$workdir"
dolt clone Liquidata/common-crawl-index-stats
cd common-crawl-index-stats
dolt table import -r entry_stats ../index_summary.psv
dolt add .
dolt commit -m 'Importing new data.'
dolt push
