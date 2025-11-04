#!/bin/bash
########################################################################################################
############################### SCRIPT TO Crawl Vantage Datastage into IDL #############################
########################################################################################################
####################################### DEVELOPMENT LOG ################################################
########################################################################################################
# 3/28/2025 - PRATHYUSH PREMACHANDRAN - RTSI333 AWS Cloud Migration
########################################################################################################
export AWS_SHARED_CREDENTIALS_FILE=/application/financedw/scripts/.aws/credentials
TIMESTAMP=$(date +%Y-%m-%d_%H:%M:%S)
#Determining script execution directory for importing other files from the same scripts directory
EXEC_DIR="${BASH_SOURCE%/*}"
if [[ ! -d "${EXEC_DIR}" ]]; then
	EXEC_DIR="$PWD"
fi
ADMIN_SYSTEM=""
FREQUENCY=""
ENVIRONMENT_NAME=""
PROJECT_NAME=financedw
while getopts e:a:f: option
do # while Loop starts
	case "$option" in
		e)
			ENVIRONMENT_NAME="$OPTARG"
		;;
		a)
			ADMIN_SYSTEM="$OPTARG"
		;;
		f)
			FREQUENCY="$OPTARG"
		;;
		\?) echo "Invalid option"
		exit 99
	;;
	esac
done
LOGDIR=/application/financedw/logs
TEMPDIR=/application/financedw/temp
LogFile=${LOGDIR}/crawler_${PROJECT_NAME}_${ADMIN_SYSTEM}_${FREQUENCY}_${TIMESTAMP}.log
if [ -z $ADMIN_SYSTEM ] || [ -z $ENVIRONMENT_NAME ] || [ -z $FREQUENCY ]; then
	echo "ERROR_10 : One of the arguments (ADMIN_SYSTEM, ENVIRONMENT_NAME, FREQUENCY) is not provided, please check the command"
	exit 10
fi
if [ $ENVIRONMENT_NAME != 'dev' ] && [ $ENVIRONMENT_NAME != 'tst' ] && [ $ENVIRONMENT_NAME != 'mdl' ] && [ $ENVIRONMENT_NAME != 'prd' ]; then
	echo "ERROR 20: Region is invalid. Should be one from the list (dev, tst, mdl, prd)" | tee -a ${LogFile}
	exit 20
fi
if [[ ${ADMIN_SYSTEM} == *"monthly"* ]] && [[ ${ADMIN_SYSTEM/%monthly/} =~ e(p5|p6|p65|p75|spl) ]]; then
	ADMIN_SYS_OVRD=${ADMIN_SYSTEM/%monthly/}
elif [[ ${ADMIN_SYSTEM} =~ e(p5|p6|p65|p75|spl) ]]; then
	ADMIN_SYS_OVRD=${ADMIN_SYSTEM}
fi
echo "Environment Name      : ${ENVIRONMENT_NAME}" | tee -a ${LogFile}
echo "Project Name          : ${PROJECT_NAME}" | tee -a ${LogFile}
echo "Admin system Name     : ${ADMIN_SYSTEM}" | tee -a ${LogFile}
echo "Admin system Name Ovrd: ${ADMIN_SYS_OVRD}" | tee -a ${LogFile}
aws s3 cp s3://ta-individual-findw-${ENVIRONMENT_NAME}-codedeployment/${PROJECT_NAME}/scripts/datastage/script.properties $TEMPDIR/${PROJECT_NAME}_script.properties
SCRIPT_PROPERTIES_FILE=$TEMPDIR/${PROJECT_NAME}_script.properties
if [[ ${ADMIN_SYS_OVRD} =~ e(p5|p6|p65|p75|spl) ]]; then
	SPLIT_LIST_TEMP1=$TEMPDIR/${ADMIN_SYSTEM}-split-list_crawler_temp1.txt
	SPLIT_LIST_TEMP2=$TEMPDIR/${ADMIN_SYSTEM}-split-list_crawler_temp2.txt
	SPLIT_LIST=$TEMPDIR/${ADMIN_SYSTEM}-split-list_crawler.txt
	trap 'rm -f "$SPLIT_LIST_TEMP1" "$SPLIT_LIST_TEMP2" "$SPLIT_LIST"' EXIT
	grep "^${ADMIN_SYSTEM}\." -i $SCRIPT_PROPERTIES_FILE | grep -iP "^${ADMIN_SYSTEM}\." | cut -d'=' -f2 > $SPLIT_LIST_TEMP1
	if [ -s "$SPLIT_LIST_TEMP1" ]; then
		> "$SPLIT_LIST_TEMP2"
		> "$SPLIT_LIST"
		while read line
		do
			for segment in $(echo ${line} | sed "s/,/ /g")
			do
				if [ ! -z ${segment} ]; then
					echo "${segment}" >> $SPLIT_LIST_TEMP2
				fi
			done
		done < $SPLIT_LIST_TEMP1
		awk '!seen[$0]++' $SPLIT_LIST_TEMP2 > $SPLIT_LIST
		paths=$(tr '\n' ',' < $SPLIT_LIST | sed 's/,$//')
		python3 ${EXEC_DIR}/glue/idl_crawler.py -i -r -e ${ENVIRONMENT_NAME} -s ${ADMIN_SYS_OVRD} -f $FREQUENCY -t "${paths}" | tee -a $LogFile
		RC=${PIPESTATUS[0]}
		if [ $RC != "0" ]; then
			echo "ERROR: Execution of Crawler Run failed and return code is ${RC}" | tee -a $LogFile
			exit ${RC}
		else
			echo "Crawler Completed Successfully at $(date +%Y-%m-%d\ %H:%M:%S)" | tee -a $LogFile
		fi
	else
		echo "ERROR_30: No matching files found in configuration to Crawl" | tee -a $LogFile
		exit 30
	fi
fi