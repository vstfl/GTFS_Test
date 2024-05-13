#!/bin/sh

for i in $@; do
    jq '.cells |= map(.execution_count = null | .outputs = [])' $i > tmp.ipynb
    mv tmp.ipynb $i
done
