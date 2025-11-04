#!/bin/bash
###################################################################################################
# DESCRIPTION - This program deletes the database and removes the data
#                Input Variable - Semi Colon (;) Delimited datbsase name (glue_database_name), that are continous without spaces
#
# Usage: sh drop_database.sh "database1;database2"
#
# 01/27/2022 - PRATHYUSH PREMACHANDRAN - AWS MIGRATION RTS 1333, Initial Release
###################################################################################################
export AWS_SHARED_CREDENTIALS_FILE=/application/financedw/scripts/.aws/credentials
db_list=$1
buildData()
{
output=""
temp=""
output2=""
temp2=""
output3=""
temp3=""
for suf in $input_main
do
temp="$db_location/$suf/"
output=$output$temp
done
for pref in $output
do
for ss in $sources
do
temp2="$pref/$ss/"
output2=$output2$temp2
done
done
for suf in $input_other
do
temp3="$db_location/$suf/"
output3=$output3$temp3
done
looper=$output2$output3$output$db_location
}
drop_db()
{
db=$1
environment=$(echo $db | awk -F. '{print $4}')
total_split_count=$(echo $db | awk -F. '{print NF}')
db_suffix=$(echo $db | awk -F. '{print $5}')
db_location=$(aws glue get-database --name $db | jq -r '.[].LocationUri')
echo Deleting Database : $db at $(date +%Y-%m-%d_%H:%M:%S)
echo Deleting Database Suffix : $db_suffix
echo Deleting Database Location : $db_location
aws glue delete-database --name $db
RC=$?
if [ $RC != "0" ]; then
echo "ERROR_100: Failed Deleting Database $db, with Return Code $RC at $(date +%Y-%m-%d_%H:%M:%S)"
exit 100
fi
if [ -z "$db_location" ]; then
echo "Database s3 uri missing. Hence not executing s3 rm for the glue database location"
elif [ "$db_suffix" = "financestagedq" ] || [ "$db_suffix" = "financemasterdatastore" ]; then
input_other="orchestration"
input_main="enterprisedatacleansed historical landing"
sources="p75 p5 p6 p65 spl bancs alm mkdb refdata refdataalm vantageone aah"
elif [ "$db_suffix" = "financedatagovernancedq" ]; then
input_other="orchestration"
input_main="financedatagovernancedq"
sources="p75 p5 p6 p65 spl datastore alm vantageone gdnumericidlookup"
fi
buildData
/usr/local/bin/parallel --will-cite --lb --link -j 10 aws s3 rm {} --recursive --only-show-errors ::: $looper
fi
echo Deleted Database : $db at $(date +%Y-%m-%d_%H:%M:%S)
echo "*************************************************************************************************************"
}
dbs=$(echo "$db_list" | sed "s/;/ /g")
export -f buildData
export -f drop_db
echo "*************************************************************************************************************"
/usr/local/bin/parallel --will-cite --lb --link -j 10 drop_db ::: $dbs
