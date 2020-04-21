#!/bin/bash
set -eo pipefail
expected_master_hash="62ed2560e55a8b308f02cf42dee765a22c855927"
expefted_pr_ref="refs/pull/130"
# Check if there is a new commit on the tip of master
if ! git ls-remote https://github.com/LDNOOBW/List-of-Dirty-Naughty-Obscene-and-Otherwise-Bad-Words HEAD | grep "$expected_master_hash" > /dev/null; then
    exit 1
fi
# Check if there is a new PR
if ! git ls-remote https://github.com/LDNOOBW/List-of-Dirty-Naughty-Obscene-and-Otherwise-Bad-Words | grep "$expected_pr_ref" > /dev/null; then
    exit 1
fi
