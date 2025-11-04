#!/bin/bash
#######################################################################################################################
#                                           DEVELOPMENT LOG                                                            #
#######################################################################################################################
# DESCRIPTION - Shell script added as a step to create FINDW Database and tables associated with it
# Usage 1: Create Database
#       sh hive_ddl_runner.sh -e dev -p financedw -h controls
# Usage 2: Deploy Config
#       sh hive_ddl_runner.sh -e dev -p financedw -h controls -c Y
# Usage 3: Drop Database
#       sh hive_ddl_runner.sh -e dev -p financedw -h controls -d Y
# 03/16/2024 - PRATHYUSH PREMACHANDRAN - AWS MIGRATION RTS 133, Initial Release
#######################################################################################################################
helpFunction()
{
echo
echo "Usage: $0 -p projectname -e env -h hop -c config -d drop"
echo -e "\t-p Project Name like financedw/financedwut/financedwstg/financedwut - projectname (Mandatory Field)"
echo -e "\t-e Environment Name like dev/stg/mdl/prd - env (Mandatory Field)"
echo -e "\t-h Hope Name like controls/datastage/masterdatastore/curated - hop (Mandatory Field)"
echo -e "\t-c Config File Run. If given then Config File will be deployed, else entire Database will be created"
echo -e "\t-c config (Optional Field.. Mutually Exclusive with drop run)"
echo -e "\t-d Drop Database Run. If given the Database will be dropped - drop (Optional Field.. Mutually Exclusive with config run)"
exit 1
}

cleanups()
{
aws s3 sync /application/financedw/logs s3://ta-individual-findw-$env-logs/linuxEC2/logs/ --only-show-errors
aws s3 sync /application/financedw/temp s3://ta-individual-findw-$env-logs/linuxEC2/temp/ --only-show-errors
}

static_data_copy()
{
echo "COPYING STATIC DATA - Shop" | tee -a $logfile
if [ $hop == "datastage" ]
then
aws s3 sync s3://$script_bucket/$projectname/data/datastage/lkup_controltotal_measures/ s3://$s3bucketname/$projectname/orchestration --delete --exact-timestamps --only-show-errors | tee -a $logfile
aws s3 sync s3://$script_bucket/$projectname/data/datastage/metadata_input/ s3://$s3bucketname/$projectname/orchestration/metadata_input/ --delete --exact-timestamps --only-show-errors | tee -a $logfile
elif [ $hop == "masterdatastore" ]
then
aws s3 sync s3://$script_bucket/$projectname/data/masterdatastore/gdq_workflow/ s3://$s3bucketname/$projectname/orchestration/gdq_workflow/ --delete --exact-timestamps --only-show-errors | tee -a $logfile
aws s3 sync s3://$script_bucket/$projectname/data/masterdatastore/gdqdatadictionary/ s3://$s3bucketname/$projectname/orchestration/gdqdatadictionary/ --delete --exact-timestamps --only-show-errors | tee -a $logfile
aws s3 sync s3://$script_bucket/$projectname/data/masterdatastore/gdqprecondition/ s3://$s3bucketname/$projectname/orchestration/gdqprecondition/ --delete --exact-timestamps --only-show-errors | tee -a $logfile
aws s3 sync s3://$script_bucket/$projectname/data/masterdatastore/gdqrulesdictionary/ s3://$s3bucketname/$projectname/orchestration/gdqrulesdictionary/ --delete --exact-timestamps --only-show-errors | tee -a $logfile
aws s3 sync s3://$script_bucket/$projectname/data/masterdatastore/gdqrunorder/ s3://$s3bucketname/$projectname/orchestration/gdqrunorder/ --delete --exact-timestamps --only-show-errors | tee -a $logfile
aws s3 sync s3://$script_bucket/$projectname/data/masterdatastore/lkup_controltotal_measures/ s3://$s3bucketname/$projectname/orchestration --delete --exact-timestamps --only-show-errors | tee -a $logfile
aws s3 sync s3://$script_bucket/$projectname/data/masterdatastore/lkupqueries/ s3://$s3bucketname/$projectname/orchestration/gdqlookupqueries/ --delete --exact-timestamps --only-show-errors | tee -a $logfile
elif [ $hop == "controls" ]
then
aws s3 sync s3://$script_bucket/$projectname/data/controls/bancscontroloperationsref/ s3://$s3bucketname/$projectname/bancscontroloperationsref/ --delete --exact-timestamps --only-show-errors | tee -a $logfile
aws s3 sync s3://$script_bucket/$projectname/data/controls/bsigactuarialparameter/ s3://$s3bucketname/$projectname/bsigactuarialparameter/ --delete --exact-timestamps --only-show-errors | tee -a $logfile
aws s3 sync s3://$script_bucket/$projectname/data/controls/consumptioncontrolsparmetertable/ s3://$s3bucketname/$projectname/consumptioncontrolsparmetertable/ --delete --exact-timestamps --only-show-errors | tee -a $logfile
aws s3 sync s3://$script_bucket/$projectname/data/controls/reconciliationparameter/ s3://$s3bucketname/$projectname/reconciliationparameter/ --delete --exact-timestamps --only-show-errors | tee -a $logfile
aws s3 sync s3://$script_bucket/$projectname/data/controls/controlsourceparameter/ s3://$s3bucketname/$projectname/controlsourceparameter/ --delete --exact-timestamps --only-show-errors | tee -a $logfile
aws s3 sync s3://$script_bucket/$projectname/data/controls/controlreconciliation/ s3://$s3bucketname/$projectname/controlreconciliation/ --delete --exact-timestamps --only-show-errors | tee -a $logfile
aws s3 sync s3://$script_bucket/$projectname/data/controls/consumptionviewparameter/ s3://$s3bucketname/$projectname/consumptionviewparameter/ --delete --exact-timestamps --only-show-errors | tee -a $logfile
aws s3 sync s3://$script_bucket/$projectname/data/controls/p65kc2reportsmetadata/ s3://$s3bucketname/$projectname/p65kc2reportsmetadata/ --delete --exact-timestamps --only-show-errors | tee -a $logfile
aws s3 sync s3://$script_bucket/$projectname/data/controls/lkup_bancscontrols/ s3://$s3bucketname/$projectname/lkup_bancscontrols/ --delete --exact-timestamps --only-show-errors | tee -a $logfile
fi
}

start_emr()
{
command="python3 /application/financedw/scripts/emr_serverless/emr_serverless.py -e $env -p $projectname -a create -t Hive -s $sys -c $cycle_date"
echo $command | tee -a $logfile
stdbuf -oL $command 2>&1 | tee -a $logfile
RC=${PIPESTATUS[0]}
if [ $RC != "0" ]; then
echo "ERROR 20: Failed Creating EMR Serverless Application at $(date +%Y-%m-%d\ %H:%M:%S)" | tee -a $logfile
cleanups
exit ${RC}
fi
}

stop_emr()
{
command="python3 /application/financedw/scripts/emr_serverless/emr_serverless.py -e $env -p $projectname -a stop -t Hive -s $sys -c $cycle_date"
echo $command | tee -a $logfile
stdbuf -oL $command 2>&1 | tee -a $logfile
RC=${PIPESTATUS[0]}
if [ $RC != "0" ]; then
echo "ERROR 100: Failed Stopping EMR Serverless Application at $(date +%Y-%m-%d\ %H:%M:%S)" | tee -a $logfile
cleanups
exit ${RC}
fi
}

drop_database()
{
database_location=s3://$s3bucketname/$projectname/
echo "DATABASE NAME: $databasename will be dropped from $database_location" | tee -a $logfile
init_file=$ddls/db_drop.sql
s3_ddl_file=s3loc/db_drop.sql
echo "DROP DATABASE IF EXISTS $databasename CASCADE" > $init_file
aws s3 cp $init_file $s3_ddl_file | tee -a $logfile
start_emr
hql_file_log="$logDeploy/hql_deploy_config_${TIMESTAMP}.log"
echo "Running Drop Database $databasename" | tee -a $logfile
params="jenkins|$databasename|$s3bucketname"
command="python3 /application/financedw/scripts/emr_serverless/emr_serverless.py -e $env -p $projectname -a run -t Hive -s $sys -c $cycle_date -d drop_database -s3_ddl_file $s3_ddl_file -pa $params"
echo "Command -> $command" | tee -a $logfile
stdbuf -oL $command 2>&1 | tee -a $hql_file_log
RC=${PIPESTATUS[0]}
if [ $RC != "0" ]; then
stop_emr
cleanups
echo "ERROR 80: Failed Dropping Database $databasename at $(date +%Y-%m-%d\ %H:%M:%S)" | tee -a $logfile
exit ${RC}
fi
}

database_creation()
{
declare -a processid
declare -a file_names
declare -a pid_log_map
declare -a pid_rc_map
database_location=s3://$s3bucketname/$projectname/
echo "DATABASE NAME: $databasename will be created in $database_location" | tee -a $logfile
init_file=$ddls/db_create.sql
s3init_file=s3loc/db_create.sql
echo "CREATE DATABASE IF NOT EXISTS $databasename LOCATION '$s3bucketname/$projectname/'" > $init_file
aws s3 cp $init_file $s3init_file | tee -a $logfile
echo "Copying files inside the temp directory $ddls" | tee -a $logfile
aws s3 sync s3://ta-individual-findw-$env-codeddeployment/$projectname/ddls/$hop/ $ddls --delete --exact-timestamps --only-show-errors | tee -a $logfile
path="/**/*.hql"
if [[ $hop == "controls" ]]; then
path="/*.hql"
fi
for filename in ${ddls}${path};
do
fileex=$(echo $filename | rev | cut -d'/' -f1 | rev)
envsubsts $filename > $hqls/$fileex
done
total_files=$(find $hqls -type f -name "*.hql" | wc -l)
counter=0
increment=$((total_files/chunk_size))
file_counter=1
echo "Total Files: $total_files" | tee -a $logfile
echo "Chunk size: $chunk_size" | tee -a $logfile
echo "Increment: $increment" | tee -a $logfile
for filename in $hqls/*.hql;
do
file_names=(${file_names[@]} "$filename")
done
echo ${#file_names[@]} | tee -a $logfile
echo "Increment: $increment" | tee -a $logfile
while [ $counter -le $total_files ]
do
tracker=$((counter+increment))
echo "#######" | tee -a $logfile
do
    echo "############################################" | tee -a $logfile
    tracker=$((counter+increment))
    echo "tracker: $tracker" | tee -a $logfile
    if [ $tracker -ge $total_files ] || [ $tracker -eq 0 ]; then
        echo "Reached final stage" | tee -a $logfile
        echo ${file_names[e]:$counter:$total_files} | tee -a $logfile
        cat ${file_names[e]:$counter:$total_files} > $consolidated_ddls/consolidated_ddl_$file_counter.hql
        break
    fi
    echo "Counter: $counter" | tee -a $logfile
    echo ${file_names[e]:$counter:$increment} | tee -a $logfile
    echo "Concatenating array" | tee -a $logfile
    cat ${file_names[e]:$counter:$increment} > $consolidated_ddls/consolidated_ddl_$file_counter.hql
    counter=$((counter+increment))
    file_counter=$((file_counter+1))
done
start_emr
log_counter=1
for filename in $consolidated_ddls/*.hql;
do
    filex=$(echo $filename | rev | cut -d"/" -f1 | rev)
    job=${filex::-4}
    s3_ddl_file="$s3loc/$filex"
    aws s3 cp $filename $s3_ddl_file | tee -a $logfile
    hql_file_log="$logDeploy/hql_${log_counter}_${TIMESTAMP}.log"
    params="jenkins|$databasename|$s3bucketname"
    command="python3 /application/financedw/scripts/emr_serverless/emr_serverless.py -e $env -p $projectname -a run -t Hive -s $sys -c $cycle_date -j $job -f $s3_ddl_file -g $s3init_file -pa $params"
    echo "Command $log_counter -> $command" | tee -a $logfile
    nohup stdbuf -oL $command >> $hql_file_log 2>&1 &
    procid="$!"
    echo "Proc id: $procid" | tee -a $logfile
    processid=(${processid[@]} "$procid")
    pid_log_map["$procid"]="$hql_file_log"
    log_counter=$((log_counter+1))
done
echo ${#processid[@]} | tee -a $logfile
for pid in ${processid[*]};
do
    echo "Waiting for $pid to be completed" | tee -a $logfile
    wait $pid
    RC=$?
    echo "pid : $pid, rc : $RC" | tee -a $logfile
    pid_rc_map["$pid"]="$RC"
    echo "$pid returned $RC" | tee -a $logfile
done
exit_cond=false
echo "Validating if any PID failed" | tee -a $logfile
for pid in "${!pid_rc_map[@]}";
do
    echo "$pid : ${pid_rc_map[$pid]}" | tee -a $logfile
    if [ ${pid_rc_map[$pid]} -ne 0 ]; then
        echo "Error: Return code for PID:$pid is ${pid_rc_map[$pid]}. Please check the ${pid_log_map[$pid]} for more information" | tee -a $logfile
        exit_cond=true
    fi
done
if $exit_cond; then
    stop_emr
    cleanups
    echo "ERROR 50: Create database and table failed fo Shop at $(date +%Y-%m-%d\ %H:%M:%S)" | tee -a $logfile
    exit 50
fi
static_data_copy
echo "Database and tables successfully created" | tee -a $logfile
}
deploy_config()
{
    echo "Downloading deploy.config" | tee -a $logfile
    aws s3 cp s3://ta-individual-findw-$env-codeddeployment/$projectname/ddls/Shop/deploy.config $configTemp/deploy.config | tee -a $logfile
    while read -r line || [ -n "$line" ]; do
        echo "HQL file name: $line" | tee -a $logfile
        aws s3 cp "s3://ta-individual-findw-$env-codeddeployment/$projectname/ddls/Shop/$line" $configTemp
        envsubst < $configTemp/$line > $hqls/$line
    done < "$configTemp/deploy.config"
    consolid_ddl=$consolidated_ddls/consolidated.hql
    echo "Generating a single file to consolidate ddls" | tee -a $logfile
    find $hqls -type f -name "*.hql" -exec cat {} + >> $consolid_ddl
    start_emr
    s3_ddl_file="$s3loc/consolidated.hql"
    aws s3 cp $consolid_ddl $s3_ddl_file | tee -a $logfile
    hql_file_log="$logDeploy/hql_deploy_config_${TIMESTAMP}.log"
    echo "Running deploy config ddls" | tee -a $logfile
    params="jenkins|$databasename|$s3bucketname"
    command="python3 /application/financedw/scripts/emr_serverless/emr_serverless.py -e $Env -p $projectname -a run -t Hive -s $sys -c $cycle_date -j consolidated_ddl -f $s3_ddl_file -pa $params"
    echo "Command - $command --> log file is $hql_file_log" | tee -a $logfile
    stdbuf -oL $command 2>&1 | tee -a $hql_file_log
    RC=${PIPESTATUS[0]}
    if [ $RC != "0" ]; then
        stop_emr
        cleanups
        echo "ERROR 70: Failed creating tables for Shop at $(date +%Y-%m-%d\ %H:%M:%S)" | tee -a $logfile
        exit ${RC}
    fi
}
export AWS_SHARED_CREDENTIALS_FILE=/application/financedw/scripts/.aws/credentials
while getopts "p:e:h:c:d:" opt
do
    case "$opt" in
        p ) projectname="$OPTARG" ;;
        e ) env="$OPTARG" ;;
        h ) hop="$OPTARG" ;;
        c ) config="$OPTARG" ;;
        d ) drop="$OPTARG" ;;
        ? ) helpFunction ;;
    esac
done
if [[ -z $projectname ]] || [[ -z $env ]] || [[ -z $hop ]]; then
    echo "ERROR 1: Environment Name, Project Name and Hop Name are mandatory"
    helpFunction
fi
if [ $env != 'dev' ] && [ $env != 'tst' ] && [ $env != 'mdl' ] && [ $env != 'prd' ]; then
    echo "ERROR 5: Environment ($env) is invalid"
    helpFunction
fi
if [[ ! -z $config ]] && [[ ! -z $drop ]]; then
    echo "ERROR 7: Config and Delete Run are Mutually exclusive"
    helpFunction
fi
case $hop in
    "datastage")
        chunk_size=40
        sys=jdst
        ;;
    "masterdatastore")
        chunk_size=40
        sys=jmds
        ;;
    "controls")
        chunk_size=4
        sys=jcon
        ;;
    "annuities")
        chunk_size=4
        sys=jann
        ;;
    "curated")
        chunk_size=8
        sys=jcur
        ;;
    *)
        echo "ERROR 10: Hop ($env) is invalid"
        helpFunction
        ;;
esac
TIMESTAMP=$(date +%Y-%m-%d_%H:%M:%S:%N)
cycle_date=$(date +%y%m%d)
tempdir=/application/financedw/temp/jenkins/$projectname
logfile=/application/financedw/logs/jenkins_ddl_${projectname}_${hop}_${TIMESTAMP}.log
logDeploy=/application/financedw/logs/deployment/$projectname/$hop
ddls=$tempdir/ddls/Shop
hqls=$tempdir/hqls/Shop
configTemp=$tempdir/config/Shop
s3loc=s3://ta-individual-findw-$env-extracts/jenkins/$projectname/Shop
consolidated_ddls=$tempdir/consolidated_ddls/Shop
if [[ $projectname == "financedw" ]]
then
    prj=""
else
    prj="_$projectname"
fi
rm -rf $ddls
rm -rf $hqls
rm -rf $consolidated_ddls
rm -rf $logDeploy
rm -rf $configTemp
mkdir -p $ddls
mkdir -p $hqls
mkdir -p $consolidated_ddls
mkdir -p $logDeploy
mkdir -p $configTemp
echo "Script Execution Started at $(date +%Y-%m-%d\ %H:%M:%S)" | tee -a $logfile
echo "Log File is ${logfile}" | tee -a $logfile
script_bucket=ta-individual-findw-$env-codeddeployment
if [ $hop == 'annuities' ]; then
    databasename=ta_individual_findw_${env}_${hop}curated${prj}
    s3bucketname=s3://ta-individual-findw-${env}-annuities-curated
elif [ $hop == 'curated' ]; then
    databasename=ta_individual_findw_${env}_finance${hop}${prj}
    s3bucketname=s3://ta-individual-findw-${env}-finance-curated
else
    databasename=ta_individual_findw_${env}_finance${hop}${prj}
    s3bucketname=s3://ta-individual-findw-${env}-${hop}
fi
export databasename s3bucketname projectname
if [[ -z $config ]] && [[ -z $drop ]]; then
    database_creation
elif [[ -z $config ]]; then
    deploy_config
else
    drop_database
fi
stop_emr
cleanups
echo "Script Execution Completed Successfully at $(date +%Y-%m-%d\ %H:%M:%S)" | tee -a $logfile
