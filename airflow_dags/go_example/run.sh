#!/bin/bash
root=$(dirname "$0")
cd $root
exec go run .
