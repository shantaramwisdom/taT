#!/bin/bash
###########################################################################################################
#                                        DEVELOPMENT LOG                                                 #
###########################################################################################################
# DESCRIPTION - Script to Perform NYDFS Cleanups i.e. purging data from datastage, masterdatastage, curated hops of Finance Data Warehouse.
# USAGE        - sh /application/financedw/curated/scripts/nydfs/nydfs_cleanup.sh -s <source_system_name> -h <hop_name> -t <table_name>
#                 -f <table_list_file_name> -d <cycle_date> -l <load_date_del_ind>
# <source_system_name> (Mandatory)    => VantageP5, VantageP6, VantageP65, VantageP75, ltcg, ltcghybrid
# <hop_name> (Optional)               => datastage, gqd, curated
# <table_name> (Optional)             => dcpProductsegmentdb, ltcg_contract
# <table_list_file_name> (Optional)   => ltcg.txt, ltcg.txt. This is a metadata
#                                       file which contains list of tables names and key columns delimited by (|)
# <chunk_size> (Optional)             => Number of Spark Applications allowed to run in Parallel. Defaulted to 3 if not provided
# <rollback> (Optional)               => Rollback Indicator (Y / N)
# <column_val> (Optional)             => Driving Cleanup Column Values
# <load_date_del_ind> (Optional)      => Delete Indicator For Activity Related Tables (Y / N)
# <cycle_date> (Mandatory)            => Date of the load (YYYYMMDD)
#
# USAGE 1: To Load driver table -
# sh /application/financedw/curated/scripts/nydfs/nydfs_cleanup.sh -s VantageP75 -h datastage -t dcpProductsegmentdb -d 240415 -u 'key_qualifier,company_code'
#
# USAGE 2: To perform cleanups on datastage tables file based -
# sh /application/financedw/curated/scripts/nydfs/nydfs_cleanup.sh -s VantageP75 -f P75.txt -d 240415
#
# USAGE 3: To perform cleanups on VantageP65 table -
# sh /application/financedw/curated/scripts/nydfs/nydfs_cleanup.sh -s VantageP65 -f VantageP65.txt -d 240827 -l N
#
# USAGE 4: To perform cleanups on ltcg_masterpool or any activity table -
# sh /application/financedw/curated/scripts/nydfs/nydfs_cleanup.sh -s ltcg -t ltcg_masterpool -d 240827 -l Y -h curated
#
# USAGE 5: To perform rollback on VantageP65 table -
# sh /application/financedw/curated/scripts/nydfs/nydfs_cleanup.sh -s VantageP65 -f VantageP65.txt -d 240827 -r Y
#
# CHANGES:
# 04/08/2024 - Sagar Sawant - Initial Development
###########################################################################################################

Checker()
{
if [[ $load_date_del_ind == 'Y' ]]; then
 if ( [[ $table_name == *"ltcg_masterpool"* || $table_name == *"_activity"* ]] || \
      ( [[ $table_name == *"_general_ledger_header"* || $table_name == *"_general_ledger_line_item"* ]] && \
        [[ $source_system_name == "ltcg" || $source_system_name == "ltcghybrid" ]] ) && $hop_name == "curated" ) || \
      ( [[ $source_system_name == "mkdb" || $source_system_name == "refdataelm" ]] && \
        [[ $hop_name == "annuities" ]] ) || \
      ( [[ $source_system_name == "aah" ]] && \
        [[ $hop_name == "datastage" ]] ) || \
      ( [[ $table_name == *"trxvh"* || $table_name == *"suspense"* || $table_name == *"inforcevaluation_rider_data"* || \
          $table_name == *"directsegmentdb_role_identification"* || $table_name == *"splinforcevaluation_fund_data"* || \
          $table_name == *"p65inforcevaluation_fund_data"* || $table_name == *"dwagentdbarchvfile"* || \
          $table_name == *"acctctn"* || $table_name == *"kc2_na_infopc"* ]] && \
        [[ $hop_name == "datastage" || $hop_name == "gqd" ]] ); then
 echo "Load Delete Indicator is Y and Parameter Validation Succeeded for $table_name"
 else
 echo "Error 80: For Table Name $table_name, Delete Indicator Y not allowed." | tee -a ${LOG_FL}
 exit 80
 fi
else
 load_data_del_ind="N"
 if ( [[ $table_name == *"ltcg_masterpool"* || $table_name == *"_activity"* ]] || \
      ( [[ $table_name == *"_general_ledger_header"* || $table_name == *"_general_ledger_line_item"* ]] && \
        [[ $source_system_name == "ltcg" || $source_system_name == "ltcghybrid" ]] ) && $hop_name == "curated" ) || \
      ( [[ $source_system_name == "mkdb" || $source_system_name == "refdataelm" ]] && \
        [[ $hop_name == "annuities" ]] ) || \
      ( [[ $source_system_name == "aah" ]] && \
        [[ $hop_name == "datastage" ]] ) || \
      ( [[ $table_name == *"trxvh"* || $table_name == *"suspense"* || $table_name == *"inforcevaluation_rider_data"* || \
          $table_name == *"directsegmentdb_role_identification"* || $table_name == *"splinforcevaluation_fund_data"* || \
          $table_name == *"p65inforcevaluation_fund_data"* || $table_name == *"dwagentdbarchvfile"* || \
          $table_name == *"acctctn"* || $table_name == *"kc2_na_infopc"* ]] && \
        [[ $hop_name == "datastage" || $hop_name == "gqd" ]] ); then
 echo "Load Delete Indicator is N and Parameter Validation Succeeded for $table_name"
 else
 echo "Error 90: For Table Name $table_name, Delete Indicator N not allowed." | tee -a ${LOG_FL}
 exit 90
 fi
fi
if [[ ( $table_name == *"_partycontactrelationship"* || $table_name == *"_partyclientrelationship"* || $table_name == *"_party"* ) ]] && [[ ( $source_system_name == "ltcg" || $source_system_name == "ltcghybrid" ) ]] && [[ $hop_name == "curated" ]] && [[ $file_based == "Y" ]]; then
 echo "Error 95: Table Name $table_name not allowed in File Based Pattern" | tee -a ${LOG_FL}
 exit 95
fi
}

SetHqlPath()
{
if [[ ( $hop_name == "datastage" ) || ( $hop_name == "gdq" ) ]]; then
 if [[ $source_system_name == "VantageP65SPL" ]]; then
  fltr='spl'
 else
  fltr=$(echo $source_system_name | sed s/"Vantage"// | tr [:upper:] [:lower:])
 fi
 if [[ $hop_name == "datastage" ]]; then
  sql_path=/application/financedw/financedatastage/sql/${fltr}/
  temp_path=/application/financedw/financedatastage/tmp/
  db_name=ta_individual_fin_dw_${env}_financedatastage${project_ovr}
  s3_path=ta-individual-fin_dw-${env}-${hop_name}
 fi
 if [[ $hop_name == "gdq" ]]; then
  sql_path=/application/financedw/financemasterdatastore/sql/gdq/vantage/${fltr}/
  temp_path=/application/financedw/financemasterdatastore/tmp/
  db_name=ta_individual_fin_dw_${env}_financemasterdatastore${project_ovr}
  s3_path=ta-individual-fin_dw-${env}-masterdatastore
 fi
elif [[ $hop_name == "curated" ]]; then
 if [[ ( $table_name == *"_general_ledger_header"* || $table_name == *"_general_ledger_line_item"* ) ]]; then
  sql_path=/application/financedw/curated/sql/erpdw/findw/
 else
  fltr=$(echo $source_system_name | tr [:upper:] [:lower:])
  sql_path=/application/financedw/curated/sql/${fltr}/
 fi
 temp_path=/application/financedw/curated/tmp/
 db_name=ta_individual_fin_dw_${env}_financecurated${project_ovr}
 s3_path=ta-individual-fin_dw-${env}-finance-${hop_name}
elif [[ $hop_name == "annuities" ]]; then
 fltr='annuities'
 if [[ $table_name == *"annuitiestables"* ]]; then
  fltr='annuitiestables'
 fi
 sql_path=/application/financedw/annuities/sql/${fltr}/
 temp_path=/application/financedw/annuities/tmp/
 db_name=ta_individual_fin_dw_${env}_annuitiescurated${project_ovr}
 s3_path=ta-individual-fin_dw-${env}-annuities-curated
else
 echo "Error 1: Hop Name: ${hop_name} is invalid. Kindly provide correct Hop Name" | tee -a ${LOG_FL}
 exit 1
fi
echo "sql_path is: ${sql_path}" | tee -a ${LOG_FL}
echo "temp_path is: ${temp_path}" | tee -a ${LOG_FL}
echo "db_name is: ${db_name}" | tee -a ${LOG_FL}
echo "s3_path is: ${s3_path}" | tee -a ${LOG_FL}
}

CheckOtherApps()
{
cnt=$(yarn application -list | grep -v "NYDFS" | grep -v "HIVE" | grep application_ | cut -d$'\t' -f 1)
if [[ ! -z $cnt ]]; then
 echo "ERROR 50: Spark Applications running on EMR other than NYDFS processes are ${cnt}. Failing Script at $(date +%Y-%m-%d\ %H:%M:%S)" | tee -a ${LOG_FL}
 exit 50
fi
}

loadtables()
{
 table_name=$(echo $tableName)
 column_list=$(echo $columnList)
 load_data_del_ind=$(echo $ind)
 hop_name=$(echo $hop_name)
 export table_name=$table_name
 CheckOtherApps
 SetHqlPath
 ####Copying table HQL files
 echo "Copying ${table_name} HQL files from ${sql_path} to ${temp_path}" | tee -a ${LOG_FL}
 ###Creating table hql files
 sed -i "s/\${databasename}/$db_name/g" ${sql_path}${table_name}.hql
 sed -i "s/\${bucketname}/s3a:\/\/${projectname}\//g" ${sql_path}${table_name}.hql
 TIMESTAMP=$(date +%Y%m%d%H%M%S)
 mkdir -p ${LOGDIR}/${hop_name}
 LOG_FILE=${LOGDIR}/${hop_name}/${table_name}-${TIMESTAMP}.log
 ERROR_FILE=${LOGDIR}/${hop_name}/${table_name}-${TIMESTAMP}.err
 echo "Overall Process Log File: ${LOG_FILE}" | tee -a ${LOG_FILE}
 echo "Log File for ${table_name} is ${LOG_FILE}" | tee -a ${LOG_FILE}
 echo "Spark Error file for ${table_name} is ${ERROR_FILE}" | tee -a ${LOG_FILE}
 echo "*****************************************************************************************" | tee -a ${LOG_FILE}
 echo "Source System and Table being Processed at $(date +%Y-%m-%d\ %H:%M:%S): ${source_system_name} ${table_name}" | tee -a ${LOG_FILE}
 if [[ ! -z $column_list ]]; then
  $SPARK_CONNECTION ${ROOT_DIR}/nydfs_cleanup.py -s $source_system_name -h $hop_name -t $table_name -c $column_list -l $load_data_del_ind -r $rollback 2>> ${ERROR_FILE} | tee -a ${LOG_FILE}
 else
  $SPARK_CONNECTION ${ROOT_DIR}/nydfs_cleanup.py -s $source_system_name -h $hop_name -t $table_name -l $load_data_del_ind -r $rollback 2>> ${ERROR_FILE} | tee -a ${LOG_FILE}
 fi
 RC=${PIPESTATUS[0]}
 if [[ $RC -eq 0 ]]; then
  echo "Load NYDFS Script execution completed for $source_system_name $table_name at $(date +%Y-%m-%d\ %H:%M:%S)" | tee -a ${LOG_FILE}
 else
  echo "Error 100: Load NYDFS Script for $source_system_name $table_name Failed with Return Code $RC at $(date +%Y-%m-%d\ %H:%M:%S)" | tee -a ${LOG_FILE}
  exit 100
 fi
 echo "*****************************************************************************************" | tee -a ${LOG_FILE}
}
parallelProcessFileBased()
{
 SAVEIFS=$IFS
 while IFS="|" read -r hop_name table ind columnList
 do
  loadtables
 done < $1
 IFS=$SAVEIFS
}

while getopts "s:h:t:f:c:d:l:r:u:" opt
do
 case "$opt" in
  s ) source_system_name="$OPTARG" ;;
  h ) hop_name="$OPTARG" ;;
  t ) table_name="$OPTARG" ;;
  f ) table_list_file_name="$OPTARG" ;;
  c ) chunk_size="$OPTARG" ;;
  d ) cycle_date="$OPTARG" ;;
  l ) load_date_del_ind="$OPTARG" ;;
  r ) rollback="$OPTARG" ;;
  u ) column_val="$OPTARG" ;;
  \? ) echo "Invalid option"
       exit 99
 ;;
 esac
done

DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi
ROOT_DIR=$DIR
DIR=${DIR}/..
####Getting LOGDIR and all other variables from script.properties file
source "${DIR}/script.properties"
TIMESTAMP=$(date +%Y%m%d%H%M%S)
file_based='N'
TMPDIR=$DIR/tmpdir
LOGDIR=${logFilePath}/nydfs/${source_system_name}
mkdir -p $LOGDIR

mkdir -p $LOGDIR
if [[ -z ${rollback} || ${rollback} != 'y' ]]; then
 rollback='N'
fi
log_sub='overall'
if [[ ${rollback} == 'y' ]]; then
 log_sub='rollback'
fi
LOG_FL=${LOGDIR}/nydfs_${log_sub}_overall_${TIMESTAMP}.log
ERROR_FILE=${LOGDIR}/nydfs_${log_sub}_${table_name}_${TIMESTAMP}.log
echo Overall Process Log File is = ${LOG_FL} | tee -a ${LOG_FL}
table_list_file=${ta_individual_findw_findhiveenv}/nydfs/${table_list_file_name}

if [[ -z ${table_list_file_name} && -z ${table_name} ]] && [[ -z ${hop_name} ]]; then
 echo "Error 5: Table Name or Table List File Name or Hop Name Need to be Provided. Certain Options are mutually Exclusive" | tee -a ${LOG_FL}
 exit 5
fi

if [[ ! -z ${table_list_file_name} && ! -z ${table_name} ]]; then
 echo "Error 10: Table Name and Table List File Name / Hop Name Options are mutually Exclusive. Only one of them can be provided at a time" | tee -a ${LOG_FL}
 exit 10
fi

if [[ -z ${hop_name} && -z ${table_name} ]] || [[ ! -z ${hop_name} && ! -z ${table_name} ]]; then
 echo "Error 11: Hop Name if Provided, then Table Name Should Be Provided and Vice Versa" | tee -a ${LOG_FL}
 exit 11
fi

if [[ -z ${source_system_name} || -z ${cycle_date} ]]; then
 echo "Error 15: Source System Name, Cycle Date are mandatory. Please provide inputs for them." | tee -a ${LOG_FL}
 exit 15
fi

echo "*****************************************************************************************" | tee -a ${LOG_FL}
echo "Remote Script execution started at $(date +%Y-%m-%d\ %H:%M:%S)" | tee -a ${LOG_FL}

if [[ -z ${chunk_size} ]]; then
 chunk_size=12
fi

if [[ ${env_name_short} == "annuities" ]] || [[ ${env_name_short} == "maintenance" ]]; then
 chunk_size=$((chunk_size * 2))
fi

echo "Default Maximum Parallel NYDFS Spark Applications allowed - ${chunk_size}" | tee -a ${LOG_FL}

if [[ -z ${table_list_file_name} ]]; then
 table_list_fl=${TMPDIR}/${table_list_file_name}
 echo "Input File containing Table List to be Processed is - ${table_list_fl}" | tee -a ${LOG_FL}
 aws s3 cp ${table_list_file} ${table_list_fl} --only-show-errors | tee -a ${LOG_FL}
 dos2unix ${table_list_fl} | tee -a ${LOG_FL}
 RC=${PIPESTATUS[0]}
 if [[ $RC -ne 0 ]]; then
  echo "Error 20: S3 File ${table_list_fl} is Invalid" | tee -a ${LOG_FL}
  exit 20
 fi
 tot_table_count=$(cat ${table_list_fl} | wc -l)
 echo "Total Tables to be Processed is : ${tot_table_count}" | tee -a ${LOG_FL}
 if [[ ${tot_table_count} -eq 0 ]]; then
  echo "Error 21: S3 File ${table_list_fl} is Empty" | tee -a ${LOG_FL}
  exit 21
 fi
fi

chunk_size_d=$chunk_size
if [[ (${tot_table_count} -lt ${chunk_size}) && (${tot_table_count} -lt 5) ]] || [[ (${tot_table_count} -ge ${chunk_size}) && ((${tot_table_count} / ${chunk_size}) -lt 0.5) ]]; then
 chunk_size_d=$((tot_table_count / 5))
elif [[ (${tot_table_count} -lt 5) ]]; then
 chunk_size_d=1
fi
if [[ $chunk_size_d -le 1 ]]; then
 chunk_size_d=2
fi
chunk_size=$chunk_size_d

max_exec="--conf spark.dynamicAllocation.maxExecutors=30"
if [[ ${source_system_name} == "ltcg" || ${source_system_name} == "ltcghybrid" ]]; then
 max_exec="--conf spark.dynamicAllocation.maxExecutors=40"
fi

if [[ -z ${table_name} ]]; then
max_exec = ''
fi
 chunk_size=$chunk_size_d

 max_exec="--conf spark.dynamicAllocation.maxExecutors=30"
 if [[ ( $source_system_name == "ltcg" || $source_system_name == "ltcghybrid" ) ]]; then
  max_exec="--conf spark.dynamicAllocation.maxExecutors=40"
 fi

 if [[ -z $table_name ]]; then
 fi
 SPARK_CONNECTION="spark-submit --deploy-mode client --master yarn $max_exec --driver-cores 5 --executor-cores 8 --driver-memory 90G --executor-memory 32G \
 --conf spark.dynamicAllocation.enabled=true \
 --conf spark.port.maxRetries=50 --conf spark.sql.broadcastTimeout=2000 --conf spark.sql.autoBroadcastJoinThreshold=50M --conf spark.default.parallelism=2000 \
 --conf spark.shuffle.service.enabled=true \
 --conf spark.executor.memoryOverhead=8G --conf spark.sql.shuffle.partitions=2000 --conf spark.driver.maxResultSize=4g --conf spark.driver.memoryOverhead=8G \
 --conf spark.driver.extraJavaOptions=-Xss4m \
 --conf spark.executor.extraJavaOptions=-Xss4m --conf spark.shuffle.spill.numElementsForceSpillThreshold=500000 --conf spark.kryoserializer.buffer.max=2000m \
 --conf spark.parquet.fs.optimized.committer.optimization-enabled=true --conf spark.sql.files.maxRecordsPerFile=8"

 export -f parallelProcessFileBased
 export -f SetHqlPath
 export -f Checker
 export -f CheckOtherApps
 export ROOT_DIR=$ROOT_DIR
 export LOGDIR=$LOGDIR
 export SPARK_CONNECTION=$SPARK_CONNECTION
 export env=$env
 export project=$project
 export project_ovrd=$project_ovrd
 export source_system_name=$source_system_name
 export env_name=$env_name_short
 export cycle_date=$cycle_date
 export LOG_FL=$LOG_FL
 export rollback=$rollback
 export file_based=$file_based
 export log_sub=$log_sub

 echo "env is - $env" | tee -a ${LOG_FL}
 echo "project is - $project" | tee -a ${LOG_FL}
 echo "source_system_name is - $source_system_name" | tee -a ${LOG_FL}
 echo "env_name is - $env_name" | tee -a ${LOG_FL}
 echo "cycle_date is - $cycle_date" | tee -a ${LOG_FL}
 echo "RollBack Flag is - $rollback" | tee -a ${LOG_FL}
 if [[ ! -z $table_name ]]; then
  if [[ -z $load_date_del_ind || $load_date_del_ind != 'Y' ]]; then
   load_date_del_ind='N'
  fi
  export hop_name=$hop_name
  export table_name=$table_name
  export load_date_del_ind=$load_date_del_ind
  echo "Hop Name is - $hop_name" | tee -a ${LOG_FL}
  echo "Table Name is - $table_name" | tee -a ${LOG_FL}
  echo "Delete Indicator is - $load_date_del_ind" | tee -a ${LOG_FL}
  Checker
  SetHqlPath
  ###Copying table HQL files
  echo "Copying ${table_name} HQL files from ${sql_path} to ${temp_path}" | tee -a ${LOG_FL}
  cp ${sql_path}${table_name}.hql ${temp_path}
  mkdir -p ${LOGDIR}/${shop_name}
  LOG_FILE=${LOGDIR}/${shop_name}/nydfs_${log_sub}_${table_name}_${TIMESTAMP}.log
  ERROR_FILE=${LOGDIR}/${shop_name}/nydfs_${log_sub}_${table_name}_${TIMESTAMP}.err
  echo Overall Process Log File is = ${LOG_FILE} | tee -a ${LOG_FILE}
  echo "Log File for ${table_name} is ${LOG_FILE}" | tee -a ${LOG_FILE}
  echo "Spark Error file for ${table_name} is ${ERROR_FILE}" | tee -a ${LOG_FILE}
  if [[ $table_name == *"dcploadsegmentdb"* ]] || [[ $hop_name == "datastage" ]] || [[ $hop_name == "gdq" ]];then
   column_list='key_qualifier,company_code'
   if [[ -z ${column_val} ]]; then
    column_list=$column_val
   fi
   load_date_del_ind='N'
  elif [[ ( $table_name == *"_partycontactrelationship"* || $table_name == *"_partyclientrelationship"* || $table_name == *"_party"* ) ]] && ( [[ $source_system_name == "ltcg" || $source_system_name == "ltcghybrid" ]] ) && [[ $hop_name == "curated" ]]; then
   load_date_del_ind='N'
   column_list='contractnumber'
  elif [[ $table_name == *"_party"* ]]; then
   column_list='documentid'
  elif [[ -z $column_val ]]; then
   column_list=$column_val
  else
   if [[ ( $table_name == *"_contract"* ) && ( $source_system_name == "ltcg" || $source_system_name == "ltcghybrid" ) && ( $hop_name == "curated" ) ]]; then
    column_list='contractnumber,contractedinistrationlocationcode'
   elif [[ $load_date_del_ind == "Y" ]]; then
    echo "Error 94: Invalid Combination for $column_list" | tee -a ${LOG_FL}
    exit 94
   fi
  fi
  echo "Delete Indicator is - $load_date_del_ind" | tee -a ${LOG_FL}
  echo "Column List Passed is - $column_val" | tee -a ${LOG_FL}
  echo "*****************************************************************************************" | tee -a ${LOG_FL}
  echo "Source System and Driver Table being Processed at $(date +%Y-%m-%d\ %H:%M:%S): ${source_system_name} ${table_name}" | tee -a ${LOG_FL}
  if [[ ! -z $column_list ]]; then
   $SPARK_CONNECTION ${ROOT_DIR}/nydfs_cleanup.py -s $source_system_name -h $hop_name -t $table_name -c $column_list -l $load_date_del_ind -r $rollback 2>> ${ERROR_FILE} | tee -a ${LOG_FILE}
  else
   $SPARK_CONNECTION ${ROOT_DIR}/nydfs_cleanup.py -s $source_system_name -h $hop_name -t $table_name -l $load_date_del_ind -r $rollback 2>> ${ERROR_FILE} | tee -a ${LOG_FILE}
  fi
  RC=${PIPESTATUS[0]}
  if [[ $RC -eq 0 ]]; then
   echo "Load NYDFS Script execution completed successfully for $source_system_name $table_name at $(date +%Y-%m-%d\ %H:%M:%S)" | tee -a ${LOG_FILE}
  else
   echo "Error 100: Load NYDFS Script for $source_system_name $table_name Failed with Return Code $RC at $(date +%Y-%m-%d\ %H:%M:%S)" | tee -a ${LOG_FILE}
   exit 100
  fi
  echo "*****************************************************************************************" | tee -a ${LOG_FILE}
 else
  echo "Recalculated Maximum Parallel NYDFS Spark Applications allowed - ${chunk_size}" | tee -a ${LOG_FL}
  file_based='Y'
export file_based=$file_based
sed -i -e '$a\' $table_list_fl
SAVEIFS=$IFS
while IFS="|" read -r hop_name table ind columnList
do
 if [[ -z $table ]] || [[ -z $table ]] || [[ -z $ind ]]; then
  echo "ERROR 40: Hop Name / Table Name / Delete Indicator/ Column List is Empty. Please check the S3 File" | tee -a ${LOG_FL}
  exit 40
 fi
 if [[ -z $hop_name ]] || ([[ $hop_name != "datastage" ]] && [[ $hop_name != "gdq" ]] && [[ $hop_name != "curated" ]] && [[ $hop_name != "annuities" ]]); then
  echo "ERROR 41: Hop Name $hop_name is Invalid. Please check the S3 File" | tee -a ${LOG_FL}
  exit 41
 fi
 if [[ -z $ind ]] || ([[ $ind != "Y" ]] && [[ $ind != "N" ]]); then
  echo "ERROR 42: Delete Indicator $ind is invalid. Please check the S3 File" | tee -a ${LOG_FL}
  exit 42
 fi
 if [[ -z $(echo $columnList | tr -d ' ') ]] && [[ $ind == "Y" ]]; then
  echo "ERROR 43: Column List $columnList and Delete Indicator $ind Combination is invalid. Please check the S3 File" | tee -a ${LOG_FL}
  exit 43
 fi
 if [[ $table == "dcpProductsegmentdb" ]]; then
  echo "Error 60: Table Name is $table. Driver Table like dcpProductsegmentdb are not allowed in metadata file." | tee -a ${LOG_FL}
  exit 60
 fi
 if [[ $table == "ltcg_contract" ]] || [[ $table == "ltcghybrid_contract" ]]; then
  echo "Error 70: Table Name is $table. Driver Tables like ltcg_contract are not allowed in metadata file." | tee -a ${LOG_FL}
  exit 70
 fi
 export hop_name=$hop_name
 export table_name=$table
 export load_date_del_ind=$ind
 Checker
done < $table_list_fl
IFS=$SAVEIFS

mkdir -p ${TMPDIR}/nydfs/${source_system_name}
rm ${TMPDIR}/nydfs/${source_system_name}/${table_list_file_name}
split -l $chunk_size $table_list_fl ${TMPDIR}/nydfs/${source_system_name}/${table_list_file_name}
echo "*****************************************************************************************" | tee -a ${LOG_FL}
/usr/local/bin/parallel --will-cite -u -j $chunk_size parallelProcessFileBased ::: ${TMPDIR}/nydfs/${source_system_name}/${table_list_file_name} | tee -a ${LOG_FL}
RC=${PIPESTATUS[0]}
if [[ $RC -ne 0 ]]; then
 echo "Error 208: NYDFS Script execution failed at $(date +%Y-%m-%d\ %H:%M:%S), with return code $RC" | tee -a ${LOG_FL}
 exit 200
fi
fi
echo "NYDFS Script execution completed at $(date +%Y-%m-%d\ %H:%M:%S)" | tee -a ${LOG_FL}

