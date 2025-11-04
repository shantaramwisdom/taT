#!/bin/bash
app_name=$1
app_id=$2
app_name_t=${app_name}'\t'
apps=$(yarn application -list | grep application_ | grep -P "${app_name_t}" | cut -d$'\t' -f 2 | tr '\n' '|' )
app_ids=$(yarn application -list | grep application_ | grep -P "${app_name_t}" | cut -d$'\t' -f 1)
cnt=$(echo $apps | grep -o "${app_name}" | wc -l)
if [ $cnt -gt 1 ]; then
 kill_other_apps=$(echo $app_ids | sed -e "s/.*$app_id//g")
 echo "Multiple Applications Running with Same App Name '$app_name' (Current APP ID $app_id will be terminated by the Python Program) and other App IDs are $kill_other_apps and will continue with the runs"
 #echo "Multiple Applications Running with Same App Name '$app_name' (Current APP ID $app_id will be terinated by the Python Program) and other App IDs (that will be killed now) are $kill_other_apps"
 #yarn application --kill $kill_other_apps
 exit 10
else
 echo "Only App Running with App Name '$app_name' is $app_id"
fi
