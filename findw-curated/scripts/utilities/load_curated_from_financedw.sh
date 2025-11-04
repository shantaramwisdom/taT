#!/bin/bash

########################################################################################################
# DEVELOPMENT LOG
# DESCRIPTION  : Script to load Finance Curated tables from financedw tables
# USAGE        : sh /application/financedw/curated/scripts/utilities/load_curated_from_financedw.sh -c <chunk_size> -s <source_system_name>
#                <chunk_size> (Optional) = Number of Spark Applications allowed to run in Parallel. Defaulted to 3 if not provided
#                <source_system_name> (Optional) = VantageP0, VantageP0S, VantageP0SPL, VantageP7S, ALL, all
#                <domain_name> (Optional) = contract contractexception activity fund
#                <skip_audit> (Optional) = Y|NULL. Data Copy is executed for batchtracking and gdqdelete. Any non-NULL value skips data copy.
#
# USAGE 1 : To copy data from financedw to curated for all source systems and domains including batchtracking and gdqdelete -
#           sh /application/financedw/curated/scripts/utilities/load_curated_from_financedw.sh
# USAGE 2 : To copy data for a given source system and domain including batchtracking and gdqdelete :
#           sh /application/financedw/curated/scripts/utilities/load_curated_from_financedw.sh -s VantageP0S -d activity
# USAGE 3 : To copy data for a given source system and domain excluding batchtracking and gdqdelete :
#           sh /application/financedw/curated/scripts/utilities/load_curated_from_financedw.sh -s VantageP0S -d activity -a Y
# USAGE 4 : To copy data for batchtracking only :
#           sh /application/financedw/curated/scripts/utilities/load_curated_from_financedw.sh -s all -d batchtracking
# USAGE 5 : To copy data for gdqdelete only :
#           sh /application/financedw/curated/scripts/utilities/load_curated_from_financedw.sh -s all -d gdqdelete
#
# CHANGES
# 12/04/2023 : Sagar Sawant - Initial Development
# 12/15/2023 : Prathyush Premachandran - Enhancements for VantagePAS activity and logging mechanism for traceability
########################################################################################################

copyDomains() {
  domain_name="$(echo $domain)"
  source_system_name="$(echo $ssname)"
  TIMESTAMP=$(date +%Y%m%d_%H%M%S)
  LOG_FILE=${LOGDIR}/load_curated_from_financedw_${source_system_name}_${domain_name}_${TIMESTAMP}.log
  ERROR_FILE=${LOGDIR}/load_curated_from_financedw_${source_system_name}_${domain_name}_${TIMESTAMP}.err
  echo "Log File is  : $LOG_FILE" | tee -a ${LOG_FILE}
  echo "Spark Error Log File is : $ERROR_FILE" | tee -a ${LOG_FILE}
  echo "*******************************************************************************************" | tee -a ${LOG_FILE}
  echo "Spark System and Domain being processed ::: ${source_system_name} ${domain_name}" | tee -a ${LOG_FILE}
  $SPARK_CONNECTION ${ROOT_DIR}/load_curated_from_financedw.py -s ${source_system_name} -d ${domain_name} 2>> ${ERROR_FILE} | tee -a ${LOG_FILE}
  RC=$?
  if [[ $RC -eq 0 ]]; then
    echo "Load Curated from financedw Script execution completed successfully for ${source_system_name} ${domain_name} at $(date +'%Y-%m-%d %H:%M:%S')" | tee -a ${LOG_FILE}
  else
    echo "ERROR 100: Load Curated from financedw Script for ${source_system_name} ${domain_name} Failed with Return Code ${RC} at $(date +'%Y-%m-%d %H:%M:%S')" | tee -a ${LOG_FILE}
    exit 100
  fi
  echo "*******************************************************************************************" | tee -a ${LOG_FILE}
}

parallelProcess() {
  value=$1
  ssname="$(echo ${value} | cut -d',' -f 1)"
  domain="$(echo ${value} | cut -d',' -f 2)"
  copyDomains
}

while getopts "c:s:d:a:" opt
do
  case "$opt" in
    c ) chunk_size="$OPTARG" ;;
    s ) ss="$OPTARG" ;;
    d ) dom="$OPTARG" ;;
    a ) skip_audit="$OPTARG" ;;
    * ) echo "Invalid option"
        exit 99
        ;;
  esac
done

DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi
ROOT_DIR=$DIR
DIR=${DIR}/..

##### Getting LOGDIR and all other variables from script.properties file
source "$DIR/script.properties"
LOGDIR=${logFilePath}/utilities
mkdir -p $LOGDIR
LOG_FL=${LOGDIR}/load_curated_from_financedw-overall-$(date +%Y%m%d_%H%M%S).log

echo | tee -a ${LOG_FL}
echo "Remote Script execution started at: $(date +'%Y-%m-%d %H:%M:%S')" | tee -a ${LOG_FL}
SPARK_CONNECTION="spark-submit --deploy-mode client --master yarn --driver-cores 5 --executor-cores 8 --driver-memory 90G \
 --executor-memory 32G --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.maxExecutors=30 \
 --conf spark.sql.broadcastTimeout=2000 --conf spark.sql.autoBroadcastJoinThreshold=50M --conf spark.default.parallelism=2000 \
 --conf spark.shuffle.service.enabled=true --conf spark.sql.shuffle.partitions=2000 --conf spark.driver.maxResultSize=6g \
 --conf spark.yarn.maxAppAttempts=1 --conf spark.driver.extraJavaOptions=-Xss4M --conf spark.executor.extraJavaOptions=-Xss4M \
 --conf spark.shuffle.spill.numElementsForceSpillThreshold=500000 --conf spark.kryoserializer.buffer.max=2000m \
 --conf spark.sql.parquet.fs.optimized.committer.optimization-enabled=true --conf spark.sql.files.maxRecordsPerFile=0"
# <<<UNREADABLE>>> (if any additional flags existed in your view, add them here)

export -f parallelProcess
export -f copyDomains
export ROOT_DIR=${ROOT_DIR}
export LOGDIR=${LOGDIR}
export SPARK_CONNECTION="$SPARK_CONNECTION"
export env=${env}
export project=${project}

if [[ -z $chunk_size ]]; then
  chunk_size=3
fi

if [[ -z $ss ]]; then
  ss="ALL VantagePAS VantagePASPL VantageP6 VantageP7S VantageP9S"
fi
ss=$ss

if [[ -z $dom ]]; then
  dom1="contract contractexception party contractexceptionfund activity"
  dom2="fund"
  dom3="contract contractexception party contractexceptionfund"
else
  dom1=$dom
  dom2=$dom
  dom3=$dom
fi

if [[ -z $skip_audit ]]; then
  audit=''
else
  audit=' all+gdqdelete all+batchtracking'
fi

list=""
for i in $ss; do
  dom=${dom1}
  if [[ ${i} == 'ALL' ]]; then
    dom=${dom2}
  elif [[ ${i} == 'VantagePASSPL' ]]; then
    dom=${dom3}
  fi

  for x in ${dom}; do
    list="${echo $list} ${i},${x}${audit}"
  done
done

echo "Maximum Parallel financedw to curated Spark Applications allowed - ${chunk_size}" | tee -a ${LOG_FL}
echo "Domain List to be Processed - list" | tee -a ${LOG_FL}
echo "*******************************************************************************************" | tee -a ${LOG_FL}

/usr/local/bin/parallel --will-cite -j ${chunk_size} parallelProcess ::: ${list} | tee -a ${LOG_FL}
RC=$?
if [[ $RC -ne 0 ]]; then
  echo "Error 150: Load Curated from financedw Script execution failed at $(date +'%Y-%m-%d %H:%M:%S'), with return code ${RC}" | tee -a ${LOG_FL}
  exit 150
fi

echo "Load Curated from financedw Script execution completed successfully at $(date +'%Y-%m-%d %H:%M:%S')" | tee -a ${LOG_FL}