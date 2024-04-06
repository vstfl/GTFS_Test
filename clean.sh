#!/bin/sh

for i in $@; do
    jq '.cells |= map(.execution_count = null | .outputs = [])' $i | sponge $i
done
