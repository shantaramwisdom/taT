#!/bin/bash

########################################################################################################################
# **********************************************  DEVELOPMENT LOG  *************************************************** #
# DESCRIPTION - Script to Load Finance Curated tables from GDQdatastore tables for LTCG & LTCGHYBRID                  #
# USAGE       - sh /application/financedw/curated/scripts/utilities/load_curated_from_gdqdatastore.sh -c <chunk_size> \
#               -s <source_system_name>                                                                               #
#               <chunk_size> (Optional) = Number of Spark Applications allowed to run in Parallel. Defaulted to 3 if  #
#                                         not provided                                                                 #
#               source_system_name (Optional) = ltcg, ltcghybrid                                                       #
#               domain_name (Optional) = activity, claim, claimbenefit, contract, contractoption, contractoptionbenefit, \
#                                        party, partyrelationship, partycontractrelationship, masterpool, batchtracking, \
#                                        gdqdelete                                                                     #
#               <skip_audit> (Optional) = If NULL, Data Copy is executed for batchtracking and gdqdelete.              #
#                                         Any non-NULL value skips data copy.                                          #
#                                                                                                                      #
# USAGE 1 : To copy data from GDQdatastore to curated for both ltcg and ltcghybrid source systems and domains         #
#           including batchtracking and gdqdelete -                                                                    #
#           sh /application/financedw/curated/scripts/utilities/load_curated_from_gdqdatastore.sh -s all -d all        #
#                                                                                                                      #
# USAGE 2 : To copy data for a given source system and domain including batchtracking and gdqdelete -                  #
#           sh /application/financedw/curated/scripts/utilities/load_curated_from_gdqdatastore.sh -s ltcg -d activity  #
#                                                                                                                      #
# USAGE 3 : To copy data for a given source system and domain excluding batchtracking and gdqdelete -                   #
#           sh /application/financedw/curated/scripts/utilities/load_curated_from_gdqdatastore.sh -s ltcg -d activity \
#           -a Y                                                                                                       #
#                                                                                                                      #
# USAGE 4 : To copy data for batchtracking only -                                                                      #
#           sh /application/financedw/curated/scripts/utilities/load_curated_from_gdqdatastore.sh -s ltcg -d batchtracking \
#           -a Y                                                                                                       #
#                                                                                                                      #
# USAGE 5 : To copy data for gdqdelete only -                                                                          #
#           sh /application/financedw/curated/scripts/utilities/load_curated_from_gdqdatastore.sh -s ltcg -d gdqdelete \
#           -a Y                                                                                                       #
#                                                                                                                      #
# CHANGES : 12/07/2023 - Sagar Sawant - Initial Development                                                            #
########################################################################################################################

copyDomains()
{
  domain_name=$(echo ${domain})
  source_system_name=$(echo ${ssname})

  TIMESTAMP=$(date +'%Y%m%d_%H%M%S')
  LOG_FILE=${LOGDIR}/load_curated_from_gdqdatastore_${source_system_name}_${domain_name}_${TIMESTAMP}.log
  ERROR_FILE=${LOGDIR}/load_curated_from_gdqdatastore_${source_system_name}_${domain_name}_${TIMESTAMP}.err

  echo "Log File is : ${LOG_FILE}" | tee -a ${LOG_FILE}
  echo "Spark Error Log File is : ${ERROR_FILE}" | tee -a ${LOG_FILE}

  echo "********************************************************************************************************" | tee -a ${LOG_FILE}
  echo "Domain being Processed at $(date +'%Y-%m-%d %H:%M:%S') : ${source_system_name} : ${domain_name}" | tee -a ${LOG_FILE}
  echo "********************************************************************************************************" | tee -a ${LOG_FILE}

  $SPARK_CONNECTION ${ROOT_DIR}/load_curated_from_gdqdatastore.py -s ${source_system_name} -d ${domain_name} 2>> ${ERROR_FILE} | tee -a ${LOG_FILE}
  RC=$?

  if [[ ${RC} -eq 0 ]]; then
    echo "Load Curated from GDQdatastore Script execution completed successfully for ${source_system_name} ${domain_name} at $(date +'%Y-%m-%d %H:%M:%S')" | tee -a ${LOG_FILE}
  else
    echo "Error 100: Load Curated from GDQdatastore Script for ${source_system_name} ${domain_name} Failed with Return Code ${RC} at $(date +'%Y-%m-%d %H:%M:%S')" | tee -a ${LOG_FILE}
    exit 100
  fi

  echo "********************************************************************************************************" | tee -a ${LOG_FILE}
}

parallelProcess()
{
  value=$1
  ssname=$(echo ${value} | cut -d '+' -f 1)
  domain=$(echo ${value} | cut -d '+' -f 2)
  copyDomains
}

while getopts "s:d:c:a:" opt
do
  case "${opt}" in
    s ) ss="${OPTARG}" ;;
    d ) dom="${OPTARG}" ;;
    c ) chunk_size="${OPTARG}" ;;
    a ) skip_audit="${OPTARG}" ;;
    \? ) echo "Invalid option"
         exit 99 ;;
  esac
done

DIR="$(dirname "${BASH_SOURCE%/*}")"
if [[ ! -d "${DIR}" ]]; then DIR="$PWD"; fi
ROOT_DIR=${DIR}
DIR=${DIR}/..

##### Getting LOGDIR and all other variables from script.properties file
source "${DIR}/script.properties"

TIMESTAMP=$(date +'%Y%m%d_%H%M%S')
LOGDIR=${logFilePath}/utilities
mkdir -p ${LOGDIR}
LOG_FL=${LOGDIR}/load_curated_from_gdqdatastore-overall-${TIMESTAMP}.log
echo "Overall Process Log File is : ${LOG_FL}"

echo
echo "Remote Script execution started at $(date +'%Y-%m-%d %H:%M:%S')" | tee -a ${LOG_FL}
SPARK_CONNECTION="spark-submit --deploy-mode client --master yarn \
  --driver-cores 3 --executor-cores 8 --driver-memory 90G --executor-memory 320G \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.sql.broadcastTimeout=2000 \
  --conf spark.sql.autoBroadcastJoinThreshold=50M \
  --conf spark.default.parallelism=2000 \
  --conf spark.shuffle.service.enabled=true \
  --conf spark.shuffle.partitions=2000 \
  --conf spark.driver.maxResultSize=6g \
  --conf spark.executor.memoryOverhead=8G \
  --conf spark.driver.extraJavaOptions=-Xss4M \
  --conf spark.executor.extraJavaOptions=-Xss4M \
  --conf spark.shuffle.spill.numElementsForceSpillThreshold=500000 \
  --conf spark.kryoserializer.buffer.max=2000m \
  --conf spark.sql.parquet.fs.optimized.committer.optimization-enabled=true \
  --conf spark.sql.files.maxRecordsPerFile=0 \
  --conf spark.dynamicAllocation.maxExecutors=25"

export -f parallelProcess
export -f copyDomains
export DIR=${ROOT_DIR}
export LOGDIR=${LOGDIR}
export SPARK_CONNECTION=${SPARK_CONNECTION}
export source_system_name=${ss}
export env=${env}
export project=${project}

if [[ -z ${chunk_size} ]]; then
  chunk_size=3
fi

if [[ -z ${ss} ]]; then
  ss="ltcg ltcghybrid"
else
  ss=${ss}
fi

if [[ -z ${dom} ]]; then
  dom1="activity claim claimbenefit contract contractoption contractoptionbenefit party partyrelationship partycontractrelationship masterpool"
  dom2="activity claim claimbenefit contract contractoption contractoptionbenefit party partyrelationship partycontractrelationship"
else
  dom=${dom}
  dom2=${dom}
fi

if [[ -z ${skip_audit} ]]; then
  audit=""
else
  audit="ltcg+gdqdelete ltcg+batchtracking"
fi

List=""
for i in ${ss}; do
  dom=${dom1}
  if [[ ${i} == 'ltcghybrid' ]]; then
    dom=${dom2}
  fi

  for x in ${dom}; do
    List="${List} ${x}+${i}"
  done
done

echo "Maximum Parallel gdqdatastore to curated Spark Applications allowed - ${chunk_size}" | tee -a ${LOG_FL}
List="${List} ${audit}"
echo "Domain List to be Processed - ${List}" | tee -a ${LOG_FL}
echo "********************************************************************************************************" | tee -a ${LOG_FL}

/usr/local/bin/parallel --will-cite -u -lb 1 ::: ${chunk_size} parallelProcess ::: ${List}
RC=${PIPESTATUS[0]}

if [[ ${RC} -ne 0 ]]; then
  echo "Error 150: Load Curated from GDQdatastore Script execution failed at $(date +'%Y-%m-%d %H:%M:%S'), with return code ${RC}" | tee -a ${LOG_FL}
  exit 150
fi

echo "Load Curated from GDQdatastore Script execution completed successfully at $(date +'%Y-%m-%d %H:%M:%S')" | tee -a ${LOG_FL}