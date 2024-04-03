#!/bin/sh

jq '.cells |= map(.execution_count = null | .outputs = [])' $1 | sponge $1