#!/bin/bash
###################################################################################################
# DESCRIPTION - This program deletes the database and removes the data
#                Input Variable - Semi Colon (;) Delimited database name (glue_database_name), that are continous without spaces
#
# Usage: sh drop_database.sh "database1;database2"
#
# 01/27/2022 - PRATHYUSH PREMACHANDRAN - AWS MIGRATION RTS 1333, Initial Release
###################################################################################################
export AWS_SHARED_CREDENTIALS_FILE=/application/financedw/scripts/.aws/credentials
db_list=$1
for db in $(echo "$db_list" | sed "s/;/ /g")
do
echo ""
environment=$(echo $db | awk -F. '{print $4}')
total_split_count=$(echo $db | awk -F. '{print NF}')
db_suffix=$(echo $db | awk -F. '{print $5}')
echo Deleting Database : $db at $(date +%Y-%m-%d_%H:%M:%S)
echo Deleting total_split_count : $total_split_count
echo Deleting db_suffix : $db_suffix
db_location=$(aws glue get-database --name $db | jq -r '.[].LocationUri')
aws glue delete-database --name $db
RC=$?
if [ $RC != "0" ]; then
echo "ERROR_100: Failed Deleting Table $db, with Return Code $RC at $(date +%Y-%m-%d_%H:%M:%S)"
exit 100
fi
if [ -z "$db_location" ]; then
if [ "$db_suffix" = "financedwdq" ]
then
echo "Fetching financedwdq s3 path"
if [ "$total_split_count" -gt "5" ]; then
project_name=$(echo $db | awk -F. '{print $NF}')
db_location="s3://ta-individual-findw-$environment-governancedataquality/$project_name/"
else
db_location="s3://ta-individual-findw-$environment-governancedataquality/financedw/"
fi
echo "Delete s3 path: $db_location at $(date +%Y-%m-%d_%H:%M:%S)"
aws s3 rm $db_location --recursive --only-show-errors
else
echo "Database s3 uri missing. Hence not executing s3 rm for the glue database"
fi
else
echo "Delete s3 path: $db_location at $(date +%Y-%m-%d_%H:%M:%S)"
aws s3 rm $db_location --recursive --only-show-errors
fi
echo Deleted Database : $db at $(date +%Y-%m-%d_%H:%M:%S)
echo "*************************************************************************************************************"
done
