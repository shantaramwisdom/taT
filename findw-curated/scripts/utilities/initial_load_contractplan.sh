#!/bin/bash
while getopts "s:o:" opt
do
 case "$opt" in
  s) source_system="$OPTARG" ;;
  o) odate="$OPTARG" ;;
  \?) echo "Invalid option"
      exit 99
  ;;
 esac
done

if [ -z "$source_system" ] || [ -z "$odate" ]; then
 echo "ERROR 10: Invalid parameters"
 exit 10
fi

DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi
ROOT_DIR=$DIR
DIR=${DIR}/..

source "$DIR/script.properties"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOGDIR=${logFilePath}/utilities
mkdir -p $LOGDIR
LOGFILE=${LOGDIR}/contractplan_initial_load_${source_system}_${TIMESTAMP}.log
ERRORFILE=${LOGDIR}/contractplan_initial_load_${source_system}_${TIMESTAMP}.err

echo Source System is $source_system | tee -a $LOGFILE
echo Odate is $odate | tee -a $LOGFILE
echo "Script started at: $(date +%Y-%m-%d' '%H:%M:%S)" | tee -a $LOGFILE

spark-submit --master yarn $spark_properties $ROOT_DIR/utilities/initial_load_contractplan.py -s $source_system -o $odate 2>> $ERRORFILE | tee -a $LOGFILE

RC=${PIPESTATUS[0]}

if [ $RC -ne 0 ]; then
 echo "ERROR 20: Script failed with $RC at: $(date +%Y-%m-%d' '%H:%M:%S)" | tee -a $LOGFILE
 exit $RC
fi

echo "Script completed at: $(date +%Y-%m-%d' '%H:%M:%S)" | tee -a $LOGFILE
