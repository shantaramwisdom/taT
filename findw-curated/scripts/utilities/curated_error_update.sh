#!/bin/bash

DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi

ROOT_DIR=$DIR
DIR=${DIR}/..

source "$DIR/script.properties"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)

if [[ $env == 'prd' ]] || [[ $env == 'mdl' ]]; then
 Initial_load='N'
fi

LOGFILE=${logFilePath}/error_table_rebuild_${TIMESTAMP}.log
ERRORFILE=${logFilePath}/error_table_rebuild_${TIMESTAMP}.err

echo Log File is $LOGFILE | tee -a $LOGFILE
echo Error File is $ERRORFILE | tee -a $LOGFILE
echo "Error Table Rebuild Script started at: $(date +%Y-%m-%d' '%H:%M:%S)" | tee -a $LOGFILE

spark-submit --master yarn $spark_properties \
 $ROOT_DIR/curated_error_update.py 2>> $ERRORFILE | tee -a $LOGFILE

RC=${PIPESTATUS[0]}

if [ $RC -ne 0 ]; then
 echo "ERROR 20: Curated Load Script failed with $RC at: $(date +%Y-%m-%d' '%H:%M:%S)" | tee -a $LOGFILE
 exit $RC
fi
