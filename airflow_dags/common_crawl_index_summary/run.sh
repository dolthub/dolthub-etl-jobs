#!/bin/bash
root=$(dirname "$0")
cd $root
ulimit -n 16384
exec go run . "$@"
