#!/bin/bash
########################################################################################################
############################### DEVELOPMENT LOG ########################################################
# DESCRIPTION - INVOKES THE PYSPARK SCRIPT FOR RUNNING LANDING CONTROLS ###############################
# USAGE - sh run_landing_controls.sh -w refdataalm -c wf_refdataalm_OptionValue -c MAFMHRE13D -p financedw -e dev -p financedw -o YYMMDD
# 07/05/2019 - SHRAVAN MUTHYALA - INITIAL DEVELOPMENT
# 12/19/2019 - Releevating to TEST
# 06/15/2020 - Naveen S - Elevating to MODEL and MODEL-UAT
# 04/06/2021 - PRATHYUSH PRACHANDRAN - AWS MIGRATION RTS 1333
# 08/04/2022 - Abhishek Nair - Added changes for workflow wf_BANCS_KC2_Monthly_Controlfile_to_Landing
# 09/01/2022 - Abhishek Nair - Added additional condition to check mount location and mhdb changes
########################################################################################################

cleanups()
{
aws s3 sync /application/financedw/logs s3://ta-individual-findw-$ENVIRONMENT_NAME-logs/linuxEC2/logs/ --only-show-errors
aws s3 sync /application/financedw/temp s3://ta-individual-findw-$ENVIRONMENT_NAME-logs/linuxEC2/temp/ --only-show-errors
}

export AWS_SHARED_CREDENTIALS_FILE=/application/financedw/scripts/.aws/credentials
echo "Remote Script execution started at $(date +'%Y%m%d_%H%M%S')"
DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then
DIR="$PWD"
fi
LOGDIR=/application/financedw/logs
TEMPDIR=/application/financedw/temp
##### Reading Arguments from Command Line/Input
while getopts a:w:c:p:e:o: option
do # while Loop starts
case "$option" in
a)
ADMIN_SYS="$OPTARG"
;;
w)
WORKFLOW_SPLIT_NAME="$OPTARG"
;;
c)
CONTROL_JOB="$OPTARG"
;;
p)
PROJECT_NAME="$OPTARG"
;;
e)
ENVIRONMENT_NAME="$OPTARG"
;;
o)
ODATE="$OPTARG"
;;
\?)
echo "Invalid option"
exit 99
;;
esac
done

if [ -z "${ADMIN_SYS}" ] || [ -z "${WORKFLOW_SPLIT_NAME}" ] || [ -z "${PROJECT_NAME}" ] || [ -z "${ENVIRONMENT_NAME}" ] || [ -z "${ODATE}" ] ; then
echo "ERROR_70: One of the arguments (ADMIN_SYS or WORKFLOW_SPLIT_NAME or PROJECT_NAME or ENVIRONMENT_NAME or ODATE) is not provided, please check"
exit 70
fi
if [ $ENVIRONMENT_NAME != 'dev' ] && [ $ENVIRONMENT_NAME != 'tst' ] && [ $ENVIRONMENT_NAME != 'md1' ] && [ $ENVIRONMENT_NAME != 'prd' ]; then
echo "ERROR_80: Environment ($ENVIRONMENT_NAME) is invalid"
exit 80
fi
if [ -z $CONTROL_JOB ]; then
CONTROL_JOB='NOT_DEFINED'
fi
s3_script_loc=s3://ta-individual-findw-${ENVIRONMENT_NAME}-codedeployment/${PROJECT_NAME}/scripts/datastage
aws s3 cp $s3_script_loc/script.properties $TEMPDIR/${PROJECT_NAME}_script.properties
DT=$(date +%b-%Y)
DATE=$(date +'%d-%b-%Y')
TIMESTAMP=$(date +'%Y%m%d_%H%M%S')
SCRIPT_PROPERTIES_FILE=${TEMPDIR}/${PROJECT_NAME}_script.properties
project_ovrd=''
if [ $PROJECT_NAME != 'financedw' ]; then
project_ovrd=$PROJECT_NAME
fi
findw_db=$(echo $PROJECT_NAME | sed "s/financedw/findw/g")
datastagedb=ta_individual_findw_${ENVIRONMENT_NAME}_financedatastage${project_ovrd}
controlsdb=ta_individual_findw_${ENVIRONMENT_NAME}_financecontrols${project_ovrd}
DATABASE_NAME=$datastagedb
CONTROLS_DATABASE_NAME=$controlsdb
mkdir -p ${LOGDIR}/runlanding
LOG_FILE=${LOGDIR}/runlanding/run-landing-controls-${ADMIN_SYS}-${WORKFLOW_SPLIT_NAME}-${TIMESTAMP}.log
echo "Log File is - $LOG_FILE" | tee -a ${LOG_FILE}
echo "****************************************************************************************************" | tee -a ${LOG_FILE}
echo "Remote Script execution started at $(date +'%Y%m%d_%H%M%S')" | tee -a ${LOG_FILE}
admin_sys=${ADMIN_SYS,,}
SPLIT_NAME=$(echo ${WORKFLOW_SPLIT_NAME} | cut -d'_' -f 3-)
ss=$(echo ${WORKFLOW_SPLIT_NAME} | cut -d'_' -f 2-)
ss=${ss^^}
WORKFLOW_SPLIT_NAME=wf_${ss}_${SPLIT_NAME}
if [[ $admin_sys == 'p5' || $admin_sys == 'p6' || $admin_sys == 'p65' || $admin_sys == 'p75' ]]; then
admin_sys2=${admin_sys^^}
else
admin_sys2=${admin_sys}
fi
SDIR=s3://ta-individual-findw-${ENVIRONMENT_NAME}-datastage
SPLIT_LIST=${TEMPDIR}/${PROJECT_NAME}-split-list-${TIMESTAMP}.txt
grep "${admin_sys}_${SPLIT_NAME)"_file" ${SCRIPT_PROPERTIES_FILE} | cut -d'=' -f2 > $SPLIT_LIST
grep "${admin_sys}_${SPLIT_NAME)_Part" ${SCRIPT_PROPERTIES_FILE} | cut -d'=' -f2 >> $SPLIT_LIST
echo "****************************************************************************************************" | tee -a ${LOG_FILE}
echo "Project name = ${PROJECT_NAME}" | tee -a ${LOG_FILE}
echo "Environment name = ${ENVIRONMENT_NAME}" | tee -a ${LOG_FILE}
echo "Script Properties file = ${SCRIPT_PROPERTIES_FILE}" | tee -a ${LOG_FILE}
echo "Datastage Database name = ${DATABASE_NAME}" | tee -a ${LOG_FILE}
echo "Controls Database name = ${CONTROLS_DATABASE_NAME}" | tee -a ${LOG_FILE}
echo "Admin system name = ${ADMIN_SYS}" | tee -a ${LOG_FILE}
echo "Workflow Split name = ${WORKFLOW_SPLIT_NAME}" | tee -a ${LOG_FILE}
echo "Split name = ${SPLIT_NAME}" | tee -a ${LOG_FILE}
echo "Split List File located at = ${SPLIT_LIST}" | tee -a ${LOG_FILE}
echo "Environment name = ${ENVIRONMENT_NAME}" | tee -a ${LOG_FILE}
echo "Split files are = `cat ${SPLIT_LIST}`" | tee -a ${LOG_FILE}
echo "Control Job Name = ${CONTROL_JOB}" | tee -a ${LOG_FILE}
SOURCE_DIR=/mnt/FINMOD/${PROJECT_NAME}/orchestration/batchcycleidinfo
batchidcnt=$(ls ${SOURCE_DIR} | tail -1 | cut -d'_' -f2)
TARGET_DIR=${SDIR}/${PROJECT_NAME}/orchestration/batchcycleidinfo/
aws s3 cp ${SOURCE_DIR} ${TARGET_DIR} --recursive | tee -a ${LOG_FILE}
RC=${PIPESTATUS[0]}
if [ $RC -ne 0 ] || [ -z "${SOURCE_DIR}" ]; then
echo "ERROR_95 : S3 Copy Process Failed for batchcycleidinfo from ${SOURCE_DIR} for Admin System ${ADMIN_SYS} to the S3 location ${TARGET_DIR} with Return Code $RC...." | tee -a ${LOG_FILE}
cleanups
exit 95
else
echo "S3 Copy is successful for batchcycleidinfo ...." | tee -a ${LOG_FILE}
fi
echo "****************************************************************************************************" | tee -a ${LOG_FILE}
echo "Moving Landing Files for Admin System ${ADMIN_SYS} and SPLIT ${SPLIT_NAME} ...." | tee -a ${LOG_FILE}
echo "****************************************************************************************************" | tee -a ${LOG_FILE}
if [ -s ${SPLIT_LIST} ]; then
while read line
do
for segment in $(echo ${line} | sed "s/,/ /g")
do
if [ ! -z ${segment} ]; then
SOURCE_DIR=/mnt/FINMOD/${PROJECT_NAME}/landing/${admin_sys}/${segment}/
TARGET_DIR=${SDIR}/${PROJECT_NAME}/landing/${admin_sys}/${segment}/
echo "Copying landing Files in ${SOURCE_DIR} for Admin System ${ADMIN_SYS} and Segment ${segment} to the S3 location ${TARGET_DIR}...." | tee -a ${LOG_FILE}
delcmd="sudo find ${SOURCE_DIR} -type f -not \( -name '*${batchid}*.csv' -or -name '*${segment}*.csv' \) -delete -print0"
eval $delcmd
aws s3 cp ${SOURCE_DIR} ${TARGET_DIR} --recursive | tee -a ${LOG_FILE}
RC=${PIPESTATUS[0]}
if [ $RC -ne 0 ] || [ -d "${SOURCE_DIR}" ]; then
echo "ERROR_100 : S3 Copy Process Failed for the Landing Files from ${SOURCE_DIR} for Admin System ${ADMIN_SYS} for Segment ${segment} to the S3 location ${TARGET_DIR}. Files are missing for the current batch....Return Code $RC" | tee -a ${LOG_FILE}
cleanups
exit 100
else
echo "S3 Copy is successful ...." | tee -a ${LOG_FILE}
fi
done
done < ${SPLIT_LIST}
else
echo "ERROR_110 : S3 MOVE Failed for the Admin System ${ADMIN_SYS} as the Split ${SPLIT_NAME} is invalid or missing in the Property file ${SCRIPT_PROPERTIES_FILE} ...." | tee -a ${LOG_FILE}
cleanups
exit 110
fi
echo "****************************************************************************************************" | tee -a ${LOG_FILE}
if [ $admin_sys != 'bancs' ] && [ $admin_sys != 'mkdb' ]; then
SOURCE_DIR=/mnt/FINMOD/${PROJECT_NAME}/landing/${admin_sys}/sourcecounts/
TARGET_DIR=${SDIR}/${PROJECT_NAME}/landing/${admin_sys}/sourcecounts/
delcmd="sudo find ${SOURCE_DIR} -type f -not \( -name '*${batchid}*.csv' -or -name 'sourcecounts.csv' \) -delete -print0"
eval $delcmd
echo "Moving sourcecounts file from ${SOURCE_DIR} to ${TARGET_DIR} ...." | tee -a ${LOG_FILE}
sudo aws s3 mv ${SOURCE_DIR} ${TARGET_DIR} --recursive | tee -a ${LOG_FILE}
fi
echo "****************************************************************************************************" | tee -a ${LOG_FILE}
if [ "$WORKFLOW_SPLIT_NAME" == "wf_MKDB_Daily_Split" ]; then
SOURCE_DIR=/mnt/FINMOD/${PROJECT_NAME}/landing/${admin_sys}/sourcecontrols/
TARGET_DIR=${SDIR}/${PROJECT_NAME}/landing/${admin_sys}/sourcecontrols/
echo "Moving sourcecontrols file from ${SOURCE_DIR} to ${TARGET_DIR} ...." | tee -a ${LOG_FILE}
sudo aws s3 mv ${SOURCE_DIR} ${TARGET_DIR} --recursive | tee -a ${LOG_FILE}
echo "****************************************************************************************************" | tee -a ${LOG_FILE}
fi
if test "$WORKFLOW_SPLIT_NAME" = "wf_BANCS_KC6_Controlfile_to_Landing" -o "$WORKFLOW_SPLIT_NAME" = "wf_BANCS_KC2_Monthly_Controlfile_to_Landing" -o "$WORKFLOW_SPLIT_NAME" = "wf_BANCS_Source_Controls_Daily_Split"
then
echo "Skipping Landing controls Pyspark execution as Workflow is $WORKFLOW_SPLIT_NAME" | tee -a ${LOG_FILE}
echo "Job completed successfully at $(date +'%Y-%m-%d_%H:%M:%S')" | tee -a ${LOG_FILE}
cleanups
exit 0
fi
if [ $ENVIRONMENT_NAME == 'dev' || $ENVIRONMENT_NAME == 'tst' ]; then
spark_properties="--conf spark.dynamicAllocation.maxExecutors=10 --conf spark.dynamicAllocation.enabled=true --conf spark.port.maxRetries=50"
else
spark_properties="--conf spark.dynamicAllocation.maxExecutors=15 --conf spark.dynamicAllocation.enabled=true --conf spark.port.maxRetries=50"
fi
echo "***************Invoking the PySpark script landing_controls.py to get the job done*****************" | tee -a ${LOG_FILE}
program=$s3_script_loc/landing_controls.py
params="$spark_properties --py-files $s3_script_loc/scripts.zip --conf spark.params.dbname=$DATABASE_NAME --conf spark.params.adminsystem=$ADMIN_SYS --conf spark.params.split.name=$WORKFLOW_SPLIT_NAME"
script_args="$ADMIN_SYS $WORKFLOW_SPLIT_NAME $findw_db $ENVIRONMENT_NAME $CONTROL_JOB $CONTROLS_DATABASE_NAME"
stdbuf -oL python3 /application/financedw/scripts/emr_serverless/emr_serverless.py -e $ENVIRONMENT_NAME -p $PROJECT_NAME -a run -t Spark -s all -c $ODATE -j ${ADMIN_SYS}_${WORKFLOW_SPLIT_NAME} -f ${program} -pa "$params" -g "$script_args" 2>&1 | tee -a ${LOG_FILE}
RC=${PIPESTATUS[0]}
if [ $RC -eq 0 ]; then
while read line
do
for segment in $(echo ${line} | sed "s/,/ /g")
do
if [ ! -z ${segment} ]; then
echo "Removing Files from the File Share (/mnt/FINMOD/${PROJECT_NAME}/landing/${admin_sys}/${segment}/) as the Run is successful ...." | tee -a ${LOG_FILE}
nohup sudo rm /mnt/FINMOD/${PROJECT_NAME}/landing/${admin_sys}/${segment}/* >> ${LOG_FILE} 2>&1 &
fi
done
done < ${SPLIT_LIST}
cleanups
echo "Landing controls Pyspark execution completed successfully at $(date +'%Y-%m-%d_%H:%M:%S')" | tee -a ${LOG_FILE}
else
echo "ERROR_120 : Landing controls Pyspark execution failed with return code $RC" | tee -a ${LOG_FILE}
cleanups
exit 120
fi