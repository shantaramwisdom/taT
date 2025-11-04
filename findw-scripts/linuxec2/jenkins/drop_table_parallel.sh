#!/bin/bash
###################################################################################################
# DESCRIPTION - This program deletes the Table and removes the data, in parallel
#                Input Variable - Semi Colon (;) Delimited Table names (glue_database_name.table_name), that are continous without spaces
#
# Usage: sh drop_table_parallel.sh "database1.table1;database2.table2"
#
# 08/27/2023 - PRATHYUSH PREMACHANDRAN - AWS MIGRATION RTS 1333, Initial Release
###################################################################################################
export AWS_SHARED_CREDENTIALS_FILE=/application/financedw/scripts/.aws/credentials
table_list=$1
drop_table()
{
table=$1
database_name=$(echo $table | cut -d'.' -f1)
table_name=$(echo $table | cut -d'.' -f2)
echo Deleting Table : $table at $(date +%Y-%m-%d_%H:%M:%S)
table_location=$(aws glue get-table --database-name $database_name --name $table_name | jq -r '.[].StorageDescriptor.Location')
aws glue delete-table --database-name $database_name --name $table_name
RC=$?
if [ $RC != "0" ]; then
echo "ERROR_100: Failed Deleting Table $table, with Return Code $RC at $(date +%Y-%m-%d_%H:%M:%S)"
exit 100
fi
if [[ $table_location == "s3://"* ]]; then
echo Deleting S3 Files of table $table, From S3 location $table_location at $(date +%Y-%m-%d_%H:%M:%S)
aws s3 rm $table_location --recursive --only-show-errors
else
echo Table $table has a non S3 Location $table_location. So not Deleting at $(date +%Y-%m-%d_%H:%M:%S)
fi
echo Deleted Table : $table at $(date +%Y-%m-%d_%H:%M:%S)
}
export -f drop_table
tables=$(echo "$table_list" | sed "s/;/ /g")
/usr/local/bin/parallel --will-cite -j 5 --lb --halt soon,fail=1 drop_table ::: $tables
RC=$?
if [ $RC != "0" ]; then
echo "ERROR_200: Failed Deleting Tables $table_list, with Return Code $RC at $(date +%Y-%m-%d_%H:%M:%S). Please Check Logs."
exit 200
fi
