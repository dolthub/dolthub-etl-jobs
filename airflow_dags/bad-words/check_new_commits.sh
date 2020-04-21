#!/bin/bash
set -eo pipefail
expected="62ed2560e55a8b308f02cf42dee765a22c855927"
if ! git ls-remote https://github.com/LDNOOBW/List-of-Dirty-Naughty-Obscene-and-Otherwise-Bad-Words HEAD | grep "$expected" > /dev/null; then
    exit 1
fi
