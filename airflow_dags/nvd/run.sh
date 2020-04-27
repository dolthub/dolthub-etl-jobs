#!/bin/bash
set -eo pipefail

workdir=$(pwd)
root=$(dirname "$0")
cp "$root/main.go" "$workdir"
cp "$root/cve.go" "$workdir"
cp "$root/cvss.go" "$workdir"
cp "$root/products.go" "$workdir"
cp "$root/references.go" "$workdir"

go run .
dolt clone Liquidata/NVD
mv *.csv NVD/
cd NVD
dolt table import -r CVE CVE.csv
dolt table import -r CVSS2 CVSS2.csv
dolt table import -r CVSS3 CVSS3.csv
dolt table import -r references references.csv
dolt table import -r reference_tags reference_tags.csv
dolt table import -r affected_products affected_products.csv
dolt add .
dolt commit -m "update from nvd feed"
dolt push origin master
