#!/bin/bash
bucket=$1; max_depth=$2; path=${3:-}; depth=${4:-1};
[ $depth -gt $max_depth ] || \
aws s3 ls "s3://$bucket/$path" | \
awk -v bucket="$bucket" -v path="$path" -v depth="$depth" -v max_depth="$max_depth" -f s3-tree.awk
