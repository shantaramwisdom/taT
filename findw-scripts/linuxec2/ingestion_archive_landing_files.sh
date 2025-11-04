#!/bin/bash
########################################################################################################
####### SCRIPT TO MOVE THE LANDING FILES TO ARCHIVE DIRECTORY ##########################################
####### TAKES REGIONNAME, DATABASENAME, ADMIN SYSTEM NAME (AND FILENAME OPTIONALLY) ####################
####### AND MOVES THE CORRESPONDING LANDING FILES TO ARCHIVE DIRECTORY #################################
####### THIS CAN BE USED ON BOTH OLD FRAMEWORK AND NEW FRAMEWORK #######################################
########################################################################################################
###################################### DEVELOPMENT LOG #################################################
########################################################################################################
# 1/31/2019 - SHRAVAN MUTHYALA - INITIAL DEVELOPMENT
# 4/20/2020 - PRATHYUSH PREMACHANDRAN - RTSI333 AWS Cloud Migration
########################################################################################################
archive_split()
{
while read line
do
	for segment in $(echo ${line} | sed "s/,/ /g")
	do
		if [ ! -z ${segment} ]; then
			SOURCE_DIR=${S3DTSDIR}/${PROJECT_NAME}/landing/${ADMIN_SYS1}/${segment}/
			ARCHIVE_DIR=${S3ARCHDIR}/${PROJECT_NAME}/landing/${ADMIN_SYS1}/${segment}/
			echo "Archiving Landing Files in $SOURCE_DIR for Admin System ${ADMIN_SYS} from Split $SPLIT_NAME to the location $ARCHIVE_DIR ....." | tee -a ${LOG_FILE}
			aws s3 mv $SOURCE_DIR $ARCHIVE_DIR --recursive --exclude "*" | tee -a ${LOG_FILE}
			RC=$?
			if [ $RC -ne 0 ]; then
				echo "ERROR_99 : Archival Process Failed for the Landing Files in ${SOURCE_DIR} for Admin System ${ADMIN_SYS} from Split $SPLIT_NAME to the location $ARCHIVE_DIR with Return Code $RC....." | tee -a ${LOG_FILE}
				ERROR_CODE=99
			else
				echo "Archival is successful ....." | tee -a ${LOG_FILE}
			fi
		fi
	done
done < ${SPLIT_LIST}
}
export AWS_SHARED_CREDENTIALS_FILE=/application/financedw/scripts/.aws/credentials
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
#Determining script execution directory for importing other files from the same scripts directory
EXEC_DIR="${BASH_SOURCE%/*}"
if [[ ! -d "${EXEC_DIR}" ]]; then
	EXEC_DIR="$PWD"
fi
ADMIN_SYS=""
SPLIT_NAME=""
FILE_NAME_PATTERN=""
PROJECT_NAME=""
ENVIRONMENT_NAME=""
ERROR_CODE=0
while getopts a:s:f:p:e: option
do # while Loop starts
	case "$option" in
		a)
			ADMIN_SYS="$OPTARG"
		;;
		s)
			SPLIT_NAME="$OPTARG"
		;;
		f)
			FILE_NAME_LIST="$OPTARG"
		;;
		p)
			PROJECT_NAME="$OPTARG"
		;;
		e)
			ENVIRONMENT_NAME="$OPTARG"
		;;
		\?)
			echo "Invalid option"
			exit 99
		;;
	esac
done
LOGDIR=/application/financedw/logs
TEMPDIR=/application/financedw/temp
aws s3 cp s3://ta-individual-findw-${ENVIRONMENT_NAME}-codedeployment/${PROJECT_NAME}/scripts/datastage/script.properties \
$TEMPDIR/${PROJECT_NAME}_script.properties
SCRIPT_PROPERTIES_FILE=$TEMPDIR/${PROJECT_NAME}_script.properties
S3DIR=s3://ta-individual-findw-${ENVIRONMENT_NAME}
S3ARCHDIR=$S3DIR-archive
S3DTSDIR=$S3DIR-datastage
LOG_FILE=${LOGDIR}/ingestion_archive_landing_${PROJECT_NAME}_${ADMIN_SYS}_${TIMESTAMP}.log
echo "Script execution started at $TIMESTAMP" | tee -a ${LOG_FILE}
# Stop the execution if input arguments are not provided as expected
if [ -z $ADMIN_SYS ] || [ -z $ENVIRONMENT_NAME ] || [ -z $PROJECT_NAME ]; then
	echo "ERROR_90 : One of the arguments (ADMIN_SYS, ENVIRONMENT_NAME, PROJECT_NAME) is not provided, please check the command"
	exit 90
fi
if [ ! -z ${FILE_NAME_LIST} ] && [ ! -z ${SPLIT_NAME} ]; then
	echo = ERROR_91: FILE_NAME_LIST and SPLIT_NAME are mutually exclusive, please provide only one" | tee -a ${LOG_FILE}
	exit 91
fi
if [ ! -f ${SCRIPT_PROPERTIES_FILE} ]; then
	echo "ERROR_92: SCRIPT_PROPERTIES_FILE file - ${SCRIPT_PROPERTIES_FILE} doesn't exist...Please check" | tee -a ${LOG_FILE}
	exit 92
fi
if [ ${ENVIRONMENT_NAME} != 'dev' ] && [ ${ENVIRONMENT_NAME} != 'tst' ] && [ ${ENVIRONMENT_NAME} != 'mdl' ] && [ ${ENVIRONMENT_NAME} != 'prd' ]; then
	echo "ERROR_94: Region is invalid. Should be one from the list (dev, tst, mdl, prd)" | tee -a ${LOG_FILE}
	exit 94
fi
SPLIT_LIST=${TEMPDIR}/${ADMIN_SYS}_archive_split_list.txt
SPLIT_LIST_TEMP1=${TEMPDIR}/${ADMIN_SYS}_archive_split_list_TEMP1.txt
SPLIT_LIST_TEMP2=${TEMPDIR}/${ADMIN_SYS}_archive_split_list_TEMP2.txt
rm -f $SPLIT_LIST_TEMP1
rm -f $SPLIT_LIST_TEMP2
SKIP_ARCHIVAL='N'
ADMIN_SYS1=${ADMIN_SYS}
if [[ ${ADMIN_SYS} == *"monthly"* ]] && [[ ${ADMIN_SYS/%monthly/} =~ e(p5|p5|p65|p75|spl) ]]; then
	ADMIN_SYS1=${ADMIN_SYS/%monthly/}
fi
echo "Environment Name	: ${ENVIRONMENT_NAME}" | tee -a ${LOG_FILE}
echo "Project Name		: ${PROJECT_NAME}" | tee -a ${LOG_FILE}
echo "Admin system Name	: ${ADMIN_SYS}" | tee -a ${LOG_FILE}
echo "Admin system Name Ovrd	: ${ADMIN_SYS1}" | tee -a ${LOG_FILE}
echo "Split Name		: ${SPLIT_NAME}" | tee -a ${LOG_FILE}
echo "File Name List		: ${FILE_NAME_LIST}" | tee -a ${LOG_FILE}
echo "Log files Directory	: ${LOGDIR}" | tee -a ${LOG_FILE}
echo "Temp files Directory	: ${TEMPDIR}" | tee -a ${LOG_FILE}
echo "Script Properties file	: ${SCRIPT_PROPERTIES_FILE}" | tee -a ${LOG_FILE}
echo "Log File Name		: ${LOG_FILE}" | tee -a ${LOG_FILE}
echo "Split List File		: ${SPLIT_LIST}" | tee -a ${LOG_FILE}
echo "Split List Temp File 1	: ${SPLIT_LIST_TEMP1}" | tee -a ${LOG_FILE}
echo "Split List Temp File 2	: ${SPLIT_LIST_TEMP2}" | tee -a ${LOG_FILE}
echo "S3 Datastage Bucket Name	: ${S3DTSDIR}/" | tee -a ${LOG_FILE}
echo "S3 Archive Bucket Name	: ${S3ARCHDIR}/" | tee -a ${LOG_FILE}
echo "***********************************************************************************************************" | tee -a ${LOG_FILE}
SOURCE_DIR=${S3DTSDIR}/${PROJECT_NAME}/landing/${ADMIN_SYS1}/
if [ -z ${FILE_NAME_LIST} ] && [ ! -z ${SPLIT_NAME} ]; then
	echo "Checking if all the files were received for the Admin System ${ADMIN_SYS} and Split ${SPLIT_NAME} in the location ${SOURCE_DIR} for the last batch date ....." | tee -a ${LOG_FILE}
	grep "${ADMIN_SYS}.${SPLIT_NAME}.files" ${SCRIPT_PROPERTIES_FILE} | grep -iP "^${ADMIN_SYS}\." | cut -d'=' -f2 > ${SPLIT_LIST_TEMP1}
else
	echo "Checking if all the files were received for the Admin System ${ADMIN_SYS} and File List (${FILE_NAME_LIST}) in the location ${SOURCE_DIR} for the last batch date ....." | tee -a ${LOG_FILE}
	echo ${FILE_NAME_LIST} > ${SPLIT_LIST_TEMP1}
fi
if [ -s ${SPLIT_LIST_TEMP1} ]; then
	while read line
	do
		for segment in $(echo ${line} | sed "s/,/ /g")
		do
			if [ ! -z ${segment} ]; then
				echo "${segment}" >> ${SPLIT_LIST_TEMP2}
			fi
		done
	done < ${SPLIT_LIST_TEMP1}
	awk '!seen[$0]++' ${SPLIT_LIST_TEMP2} > ${SPLIT_LIST}
	while read segment
	do
		SOURCE_DIR=${S3DTSDIR}/${PROJECT_NAME}/landing/${ADMIN_SYS1}/${segment}/
		if [ $(aws s3 ls ${SOURCE_DIR} | wc -l) == 0 ]; then
			echo "ERROR_95 : Archival Process Failed as the Landing Files for ${segment} are missing for the Admin System ${ADMIN_SYS} in the location ${SOURCE_DIR} ....." | tee -a ${LOG_FILE}
			ERROR_CODE=95
			MISSING_FILES="${segment},${MISSING_FILES}"
		else
			RECEIVED_FILES="${segment},${RECEIVED_FILES}"
		fi
	done < ${SPLIT_LIST}
	if [ ! -z ${MISSING_FILES} ]; then
		MISSING_FILES=${MISSING_FILES:0:${#MISSING_FILES}-2}
	fi
	if [ ! -z ${RECEIVED_FILES} ]; then
		RECEIVED_FILES=${RECEIVED_FILES:0:${#RECEIVED_FILES}-2}
	fi
	echo "***********************************************************************************************************" | tee -a ${LOG_FILE}
fi
if [ ${ERROR_CODE} -eq 0 ]; then
	echo "All files received (${RECEIVED_FILES}) for the last batch date for Admin System ${ADMIN_SYS} and now going to archive ....." | tee -a ${LOG_FILE}
else
	if [ -z ${RECEIVED_FILES} ]; then
		echo "There are no files to archive for the last batch date for Admin System ${ADMIN_SYS} ....." | tee -a ${LOG_FILE}
		SKIP_ARCHIVAL='Y'
	else
		echo "These Files (${MISSING_FILES}) are missing for the last batch date for Admin System ${ADMIN_SYS} and now going to archive the following received files (${RECEIVED_FILES}) ....." | tee -a ${LOG_FILE}
	fi
fi
echo "***********************************************************************************************************" | tee -a ${LOG_FILE}
if [ ${SKIP_ARCHIVAL} == 'N' ]; then
	echo "Archiving Files for the Admin System ${ADMIN_SYS} to the S3 Bucket ${S3ARCHDIR}" | tee -a ${LOG_FILE}
	if [ -z ${FILE_NAME_LIST} ] && [ -z ${SPLIT_NAME} ]; then
		if [ ${ADMIN_SYS} != ${ADMIN_SYS1} ]; then
			archive_split
		else
			SOURCE_DIR=${S3DTSDIR}/${PROJECT_NAME}/landing/${ADMIN_SYS1}/
			ARCHIVE_DIR=${S3ARCHDIR}/${PROJECT_NAME}/landing/${ADMIN_SYS1}/
			echo "Executing archiving logic for the complete Admin System ${ADMIN_SYS} from the location ${SOURCE_DIR} to the location $ARCHIVE_DIR ....." | tee -a ${LOG_FILE}
			aws s3 mv $SOURCE_DIR $ARCHIVE_DIR --recursive --exclude "*" | tee -a ${LOG_FILE}
			RC=$?
			if [ $RC -ne 0 ]; then
				echo "ERROR_97 : Archival Process Failed for the complete Admin System ${ADMIN_SYS} from the location ${SOURCE_DIR} to the location $ARCHIVE_DIR with Return Code $RC....." | tee -a ${LOG_FILE}
				ERROR_CODE=97
			else
				echo "Archival is successful ....." | tee -a ${LOG_FILE}
			fi
		fi
	elif [ ! -z ${SPLIT_NAME} ]; then
		echo "Executing archiving logic for Admin System ${ADMIN_SYS} and Split ${SPLIT_NAME} ....." | tee -a ${LOG_FILE}
		grep "${ADMIN_SYS}.${SPLIT_NAME}.files" ${SCRIPT_PROPERTIES_FILE} | grep -iP "^${ADMIN_SYS}\." | cut -d'=' -f2 > ${SPLIT_LIST}
		if [ -s ${SPLIT_LIST} ]; then
			archive_split
		else
			echo "ERROR_100 : Archival Process Failed for the Admin System ${ADMIN_SYS} as the Split ${SPLIT_NAME} is invalid or missing in the Property file ${SCRIPT_PROPERTIES_FILE} ....." | tee -a ${LOG_FILE}
			ERROR_CODE=100
		fi
	else
		echo "Executing archiving logic for Admin System ${ADMIN_SYS} and Landing Files ${FILE_NAME_LIST} ....." | tee -a ${LOG_FILE}
		for fname in $(echo ${FILE_NAME_LIST} | sed "s/,/ /g")
		do
			SOURCE_DIR=${S3DTSDIR}/${PROJECT_NAME}/landing/${ADMIN_SYS1}/${fname}/
			ARCHIVE_DIR=${S3ARCHDIR}/${PROJECT_NAME}/landing/${ADMIN_SYS1}/${fname}/
			if [ $(aws s3 ls ${SOURCE_DIR} | wc -l) == 0 ]; then
				echo "There are no files in ${SOURCE_DIR} to Archive for Admin System ${ADMIN_SYS} ....." | tee -a ${LOG_FILE}
				echo "ERROR_101 : There are no files in ${SOURCE_DIR} to Archive for Admin System ${ADMIN_SYS} ....." | tee -a ${LOG_FILE}
				ERROR_CODE=101
				break
			else
				echo "Archiving Landing Files in ${SOURCE_DIR} for Admin System ${ADMIN_SYS} to the location ${ARCHIVE_DIR} ....." | tee -a ${LOG_FILE}
				aws s3 mv $SOURCE_DIR $ARCHIVE_DIR --recursive --exclude "*" | tee -a ${LOG_FILE}
				RC=$?
				if [ $RC -ne 0 ]; then
					echo "ERROR_102 : Archival Process Failed for the Landing Files in ${SOURCE_DIR} for Admin System ${ADMIN_SYS} to the location $ARCHIVE_DIR with Return Code $RC....." | tee -a ${LOG_FILE}
					ERROR_CODE=102
				else
					echo "Archival is successful ....." | tee -a ${LOG_FILE}
				fi
			fi
		done
	fi
else
	echo "Skipping Archival Process ....." | tee -a ${LOG_FILE}
fi
echo "***********************************************************************************************************" | tee -a ${LOG_FILE}
echo "***********************************************************************************************************" | tee -a ${LOG_FILE}
if [ ${ERROR_CODE} -eq 0 ]; then
	echo "Script Execution completed successfully, please check the logfile ${LOG_FILE} for execution details" | tee -a ${LOG_FILE}
elif [ ${ERROR_CODE} -eq 95 ]; then
	echo "Script Execution had errors as Landing Files were missing. All the other Landing Files were Archived Successfully. Forcing the Script to complete the run successfully. Please check the logfile ${LOG_FILE} for execution details. " | tee -a ${LOG_FILE}
else
	echo "Script Execution is unsuccessful and RC=${ERROR_CODE}, please check the logfile ${LOG_FILE} for errors" | tee -a ${LOG_FILE}
fi