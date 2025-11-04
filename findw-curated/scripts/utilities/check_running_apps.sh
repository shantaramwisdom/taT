#!/bin/bash
file=$1

random_sleep_seconds=$(shuf -i 1-120 -n 1)
echo Sleeping $random_sleep_seconds seconds
sleep $random_sleep_seconds

random_sleep_seconds=$(shuf -i 1-60 -n 1)
echo Sleeping $random_sleep_seconds seconds
sleep $random_sleep_seconds

otpt=/home/hadoop/otpt
cnt=$(yarn application -list | grep application_ | cut -d$'\t' -f 1 | wc -w)
echo "EMR is having $cnt running applications"
echo $cnt > $otpt
aws s3 cp $otpt $file
rm $otpt
