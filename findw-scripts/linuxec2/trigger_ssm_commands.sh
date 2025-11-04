#!/bin/bash
##########################################################################################################
####################################### DEVELOPMENT LOG ##################################################
##########################################################################################################
# DESCRIPTION - This script can do the following things -
# a) Can trigger a script in an EMR
# Usage : sh trigger_ssm_commands.sh -e dev -p financedw -c "echo hello" -s ondemand-emr-all-curated-financedw
#
# b) Can trigger a script in BDE or PowerCenter EC2 Server or in FINDW’s EC2 Server from another environment like Production EC2 can trigger a script in Model EC2.
# Usage 1: sh trigger_ssm_commands.sh -e dev -u spatactfindev -t dei
# Usage 2: sh trigger_ssm_commands.sh -e dev -u spatactfindev -t pctr
# Usage 3: sh trigger_ssm_commands.sh -e dev -u spatactfindev -t findw
# Usage 4: sh trigger_ssm_commands.sh -e dev -u spatactfindev -t datalake
# -c "/efs/informatica/ControlLM/Bde/FINNDW/dev/SWEEP_JOBS/app_cv_batch_and_control_Logging/wf_m_CV_BATCH_AND_CONTROL_LOGGING /efs/informatica/detl/server/infa_shared/Bde/ParameterFiles/FINNDW/SWEEP_JOBS/cv_Logging_party_param.xml"
#
# c) Deploy Code to any running EMRs of a project
# Usage: sh trigger_ssm_commands.sh -e dev -p financedw -d Y
#
# d) Delete Orphan DNS record for the Project
# Usage: sh trigger_ssm_commands.sh -e dev -p financedw -r Y
#
# e) Table Name to do MSCK Repair. databasename.tablename. Takes only Table Name for now. Need All the rest parameters to run a command in EMR to use this functionality. Command can be anything as the SSM wont execute instead MSCK Repair will run.
# Usage: sh trigger_ssm_commands.sh -e dev -p financedw -c "echo hello" -s ondemand-emr-all-curated-financedw -n ta_individual_financdwd_dev_datastage.tablename
# * The Parameters needed to run this script are *
# 1. Environment Name (e)
# 2. EMR Search Name (s)
# 3. Command to be executed in EMR or BDE EC2 Server (c)
# 4. User Override (Optional, default is hadoop for EMR, Support Accounts for DEI/BDE or PowerCenter or FINDW EC2 Server) (u)
# 5. Type of EC2 Machine (Optional, Valid Values are dei or pctr or findw or datalake. Mandatory when User Override is provided) (t)
# 6. Code Deployment (Optional, default is set to N. Setting Y will deploy the code to all running EMRs for the project) (d)
# 7. Route 53 Clear (Optional, default is set to N. Setting Y will clean all Orphan DNS records for the project) (r)
# 8. Project Name (Mandatory when Code Deployment is set Y. Also is mandatory when executing the DEI Mapping in the DEI Server from the Control M Location (c) (p)
# 9. Table Name to do MSCK Repair. databasename.tablename. Takes only Table Name for now. (n)
#
# 05/13/2021 - PRIATHIYUSH PREMACHANDRAN - AWS MIGRATION RTS 133, Initial Release
##########################################################################################################

helpFunction()
{
	echo "Usage 1: $0 -e environment -p project -s search -c command"
	echo "Usage 2: $0 -e environment -p project -t dei/pctr/findw/datalake -c command -u user"
	echo "Usage 3: $0 -e environment -p project -d Y"
	echo "Usage 4: $0 -e environment -p project -r Y"
	echo -e "\t-e Environment like dev, tst, mdl, prd"
	echo -e "\t-s EMR Search Criteria"
	echo -e "\t-c Command to be passed to EMR to execute a specific script"
	echo -e "\t-u Support User who executes the script. Optional Parameter. Default is hadoop for EMR"
	echo -e "\t-t This type of EC2 machine in which the command need to be executed, Valid Values are dei or pctr or findw or datalake"
	echo -e "\t-d This Flag will deploy the code to all running EMR for the Project. Default is set as N"
	echo -e "\t-r This Flag will clear Orphan DNS Records in Route 53 for the Project. Default is set as N"
	echo -e "\t-p The Name of the Project for which the code needs get deployed for all its running EMRs"
	echo -e "\t-n The Name of the Table that need to be repaired by running MSCK Repair"
	echo "ERROR 1: The Parameters passed are invalid"
	exit 1
}

get_details()
{
	stdout=$(aws ssm get-command-invocation \
		--command-id $commandId \
		--instance-id $Ec2InstanceId | jq -r '.StandardOutputUrl')
	stderr=$(aws ssm get-command-invocation \
		--command-id $commandId \
		--instance-id $Ec2InstanceId | jq -r '.StandardErrorUrl')
	ResponseCode=$(aws ssm get-command-invocation \
		--command-id $commandId \
		--instance-id $Ec2InstanceId | jq -r '.ResponseCode')
	outputPath=$(aws ssm list-command-invocations --command-id "$commandId" --details --query "CommandInvocations[].CommandPlugins[].OutputS3KeyPrefix" --output text)

	echo "The return Code is $ResponseCode. The Command's standard output/error is uploaded to: s3://${s3_bucket_name}/${outputPath}/0.awsrunShellScript/"
	#aws s3 cp --quiet --only-show-errors s3://${s3_bucket_name}/${outputPath}/0.awsrunShellScript/stdout /dev/stdout
	echo "***********************STDOUT Printed below from s3://${s3_bucket_name}/${outputPath}/0.awsrunShellScript/stdout***********************"
	aws s3 cp s3://${s3_bucket_name}/${outputPath}/0.awsrunShellScript/stdout /dev/stdout
	echo -e "\n************************************************************************************************************"
}

emr_determination()
{
	step=""
	source_system=""

	case "$search" in
		*-aah-*)
			source_system=aah
			;;
		*-bancs-*)
			source_system=bancs
			;;
		*-rdm-*)
			source_system=rdm
			;;
		*-alm-*)
			source_system=alm
			;;
		*-curated-*)
			step=curated
			source_system=$(cut -d - -f3 <<<"${search}")
			;;
		*-maintenance-*)
			step=maintenance
			source_system=maintenance
			;;
		*-annuities-*)
			step=annuities
			source_system=annuities
			;;
	esac

	case "$search" in
		*-ingestion-*)
			step=ingestion
			source_system=$(cut -d - -f3 <<<"${search}")
			;;
	esac

	new_search=$(echo $search | sed -e 's/\(-\)/\1\n/g' | grep - | wc -l)
	((cnt=cnt+1))
	project_name=$(cut -d - -f$cnt <<<"${search}")

	if [[ ! -z $source_system ]] && [[ ! -z $step ]] && [[ ! -z $project_name ]] && [[ $step == 'ingestion' ]]; then
		# vantageone, p5, p6, p65, p75, spl -- All Ingestion
		new_search=ondemand-emr-all-curated-$project_name
	elif [[ ! -z $source_system ]] && [[ $source_system == 'all' ]] && [[ $step == 'curated' ]] && [[ -z $project_name ]]; then
		# Curated Generic
		new_search=ondemand-emr-all-curated-$project_name
	elif [[ ! -z $source_system ]] && [[ $step == 'curated' ]] && [[ -z $project_name ]]; then
		# New Curated EMR by source system
		new_search=ondemand-emr-$source_system-curated-$project_name
	elif [[ ($source_system == 'bancs' || $source_system == 'rdm' || $source_system == 'alm' || $source_system == 'aah') ]] && [[ ! -z $project_name ]]; then
		# Only needed till Monthend Control M Commands for BANCS and RDM and ALM Control M Jobs are fixed to use the correct EMR.
		new_search=ondemand-emr-all-curated-$project_name
	fi
}

create_missing_emr()
{
	echo "INFO: There is no such running EMR with name ta-individual-findw-$environment-X${search}X. Trying to create an EMR at $(date +%Y-%m-%d\ %H:%M:%S)"
	emr_determination
	cnt=$(echo $search | sed -e 's/\(-\)/\1\n/g' | grep - | wc -l)
	((cnt=cnt+1))
	project_name=$(cut -d - -f$cnt <<<"${search}")
	logfile=/application/financedw/logs/create_missing_emr_${project_name}_${source_system}_${step}_${TIMESTAMP}.log
	echo Project is $project_name | tee -a $logfile
	echo Step is $step | tee -a $logfile
	echo Source System is $source_system | tee -a $logfile
	echo Log file: $logfile

	if [[ ! -z $source_system ]] && [[ ! -z $step ]] && [[ ! -z $project_name ]] && [[ $step == 'ingestion' ]]; then
		# vantageone, p5, p6, p65, p75, spl -- All Ingestion
		echo "python3 /application/financedw/scripts/emr/main.py -e $environment -p $project_name -s curated" | tee -a $logfile
		stdbuf -oL python3 /application/financedw/scripts/emr/main.py -e $environment -p $project_name -s curated >&1 | tee -a $logfile
		RC=${PIPESTATUS[0]}
	elif [[ ! -z $source_system ]] && [[ ! -z $step ]] && [[ ! -z $project_name ]]; then
		# maintenance, annuities, [curated (all systems that means not at Source System Level)]
		echo "python3 /application/financedw/scripts/emr/main.py -e $environment -p $project_name -s $step" | tee -a $logfile
		stdbuf -oL python3 /application/financedw/scripts/emr/main.py -e $environment -p $project_name -s $step >&1 | tee -a $logfile
		RC=${PIPESTATUS[0]}
	elif [[ ! -z $source_system ]] && [[ -z $project_name ]] && [[ $step == 'curated' ]]; then
		# New Curated EMR by source system
		echo "python3 /application/financedw/scripts/emr/main.py -e $environment -p $project_name -ss $source_system -s curated" | tee -a $logfile
		stdbuf -oL python3 /application/financedw/scripts/emr/main.py -e $environment -p $project_name -ss $source_system -s curated >&1 | tee -a $logfile
		RC=${PIPESTATUS[0]}
	elif [[ ($source_system == 'bancs' || $source_system == 'rdm' || $source_system == 'alm' || $source_system == 'aah') ]] && [[ ! -z $project_name ]]; then
		# Only needed till Monthend Control M Commands for BANCS and RDM and ALM Control M Jobs are fixed to use the correct EMR..
		echo "python3 /application/financedw/scripts/emr/main.py -e $environment -p $project_name -ss $ovrd" | tee -a $logfile
		stdbuf -oL python3 /application/financedw/scripts/emr/main.py -e $environment -p $project_name -ss $ovrd >&1 | tee -a $logfile
		RC=${PIPESTATUS[0]}
	else
		echo "ERROR 9: There is no such running EMR with name ta-individual-findw-$environment-X${search}X and such a combination cant be created using the current script"
		exit 9
	fi

	if [[ ! "$RC" == "0" ]]; then
		echo "ERROR 99: There is no such running EMR with name ta-individual-findw-$environment-X${search}X and the EMR Creation Script has failed with Return Code $RC"
		exit 99
	fi

	cluster_id=$(cat $logfile | grep -i | tac | awk 'NF{print $NF;exit;}' | sed "s/\"//g" | sed "s/\;//g")
	echo "EMR Cluster ${cluster_id}, created at $(date +%Y-%m-%d\ %H:%M:%S)"
}

check_if_emr_same()
{
	echo "Checking if EMR is still present at $(date +%Y-%m-%d\ %H:%M:%S)"
	new_cluster_id=$(aws emr list-clusters --query "Clusters[?starts_with(Name, 'ta-individual-findw-$environment')][?Status.State!='TERMINATED'] | [?Status.State!='TERMINATED_WITH_ERRORS']|[?contains(Name, '${search}-20')].Id" --output text)
	if [[ -z "$new_cluster_id" ]] || [[ "$new_cluster_id" != "${cluster_id}" ]]; then
		echo "ERROR 108: EMR Cluster is missing or New Cluster present and SSM running in older Cluster. New Cluster is $new_cluster_id and old Cluster is $cluster_id"
		exit 108
	fi
	echo "EMR Cluster ID $cluster_id is still same as in ${new_cluster_id}, at $(date +%Y-%m-%d\ %H:%M:%S)"
}

get_emr_details()
{
	bkup_search=$search
	emr_determination

	if [[ ! -z $new_search ]]; then
		search=$new_search
	fi

	cluster_id=$(aws emr list-clusters --query "Clusters[?starts_with(Name, 'ta-individual-findw-$environment')][?contains(Name, '${search}-20')].Id" --output text)
	echo "WAITING EMRs $cluster_id"
	cluster_id2=$(aws emr list-clusters --query "Clusters[?starts_with(Name, 'ta-individual-findw-$environment')][?contains(Name, '${search}-20')].Id" --output text)
	echo "RUNNING EMRs $cluster_id2"
	cluster_id=$(echo $cluster_id $cluster_id2)

	if [[ $deploy == 'Y' ]]; then
		cluster_id3=$(aws emr list-clusters --query "Clusters[?starts_with(Name, 'ta-individual-findw-$environment')][?contains(Name, '${search}-20')].Id" --output text)
		echo "BOOTSTRAPPING EMRs $cluster_id3"
		cluster_id=$(echo $cluster_id $cluster_id3)
	fi

	# unique cluster ids
	cluster_id=$(echo $cluster_id | tr " " "\n" | sort -u | tr "\n" " " | sed 's/^[ \t]*//;s/[ \t]*$//')
	echo "Active EMRs $cluster_id"
	echo "Received EMR Cluster Id at $(date +%Y-%m-%d\ %H:%M:%S)"

	len=$(echo $#cluster_id)
	if [[ -z $cluster_id ]] && ([[ $regular_run == 'Y' ]] || [[ $ec2 == 'dei' ]]); then
		search=$bkup_search
		create_missing_emr
	elif [[ $len -gt 15 ]] && ([[ $regular_run == 'Y' ]] || [[ $ec2 == 'dei' ]]); then
		echo "ERROR 10: More than One EMR Clusters are Returned. Failing the Job. The EMR Clusters are $cluster_id"
		exit 10
	fi

	if [[ $deploy == 'Y' ]] && [[ ! -z $cluster_id ]]; then
		tempfile=/application/financedw/temp/deploy_emr_code_${project}_cluster_ids_${TIMESTAMP}.txt
		echo "Details are stored in $tempfile"
		echo $cluster_id | tr " " "\n" > ${tempfile}
		while read -r line; do
			cluster_id=$line
			get_instance_id_details
			Ec2InstanceIdList="${Ec2InstanceIdList}${Ec2InstanceIdList:+,}${Ec2InstanceId}"
			ClusterNameList="${ClusterNameList}${ClusterNameList:+,}${ClusterName}"
		done < ${tempfile}
		Ec2InstanceId=$Ec2InstanceIdList

	elif [[ $route53 == 'Y' ]]; then
		tempfile=/application/financedw/temp/route53_records_${search}_${TIMESTAMP}.txt
		echo "Details are stored in $tempfile"
		dns_search=findw-$environment-$search
		echo Hosted Zone Name: $HOSTED_ZONE_NAME
		echo Hosted Zone ID: $HOSTED_ZONE_ID
		echo DNS Record Search Name: $dns_search

		aws route53 list-resource-record-sets --hosted-zone-id $HOSTED_ZONE_ID --query 'ResourceRecordSets[?starts_with(Name, `'$dns_search'`)].ResourceRecords[].Value' | jq -r '.[]' > ${tempfile}
		aws route53 list-resource-record-sets --hosted-zone-id $HOSTED_ZONE_ID --query 'ResourceRecordSets[?starts_with(Name, `'$dns_search'`)].ResourceRecords[].Value' >> ${tempfile}

		if [ ! -s ${tempfile} ]; then
			echo "No DNS Entries to be deleted"
		else
			while IFS=, read -r dns_name dns_type dns_ttl dns_ip_value; do
				delete_dns=Y
				if ping -c 1 -W 1 $dns_name; then
					if [[ -z $cluster_id ]]; then
						echo "No DNS Name $dns_name will not be deleted as the machine is still alive and its Ip Address is $dns_ip_value"
						delete_dns=N
					else
						echo "Since there are no EMR Clusters active for the project ${project}, the DNS Name $dns_name will be deleted eventhough the machine is still alive and its Ip Address is $dns_ip_value"
					fi
				fi

				if [[ -z $cluster_id ]] || [[ $delete_dns == 'Y' ]]; then
					echo "DNS Name to be deleted is $dns_name and its Ip Address is $dns_ip_value, at $(date +%Y-%m-%d\ %H:%M:%S)"
					aws route53 change-resource-record-sets --hosted-zone-id $HOSTED_ZONE_ID \
						--change-batch '{ "Changes": [ { "Action": "DELETE","ResourceRecordSet": { "Name": "'$dns_name'", "Type": "'$dns_type'", "TTL": 120, "ResourceRecords": [ { "Value": "'$dns_ip_value'" } ] } } ] }'
				fi
			done < ${tempfile}
		fi

		if [[ -z $cluster_id ]]; then
			echo "There are still running EMRs and its DNS Records weren't deleted."
			echo "Cluster Ids - $cluster_id"
		elif [[ $regular_run == 'Y' ]] || [[ $ec2 == 'dei' ]]; then
			get_instance_id_details
		else
			skip_flg=Y
		fi
	fi
}

get_instance_id_details()
{
	ClusterName=$(aws emr describe-cluster --cluster-id $cluster_id --query 'Cluster.Name' --output text)
	echo "Received EMR Cluster Name at $(date +%Y-%m-%d\ %H:%M:%S)"
	MasterPublicDnsName=$(aws emr describe-cluster --cluster-id $cluster_id --query 'Cluster.MasterPublicDnsName' --output text)
	echo "Received EMR Cluster Master Public DNS Name at $(date +%Y-%m-%d\ %H:%M:%S)"

	Ec2InstanceId=$(aws emr list-instances --cluster-id $cluster_id --query "Instances[? @.PrivateDnsName == '$MasterPublicDnsName' && @.Status.State == 'RUNNING' ].Ec2InstanceId" --output text)
	if [[ $deploy == 'Y' ]] && [[ -z $Ec2InstanceId ]]; then
		Ec2InstanceId=$(aws emr list-instances --cluster-id $cluster_id --query "Instances[? @.PrivateDnsName == '$MasterPublicDnsName' && @.Status.State == 'BOOTSTRAPPING' ].Ec2InstanceId" --output text)
	fi

	echo "Received EMR Cluster Master Instance ID at $(date +%Y-%m-%d\ %H:%M:%S)"
	echo -e "\n************************************************************************************************************"
	echo "The Cluster ID is: $cluster_id"
	echo "The Cluster Name is: $ClusterName"
	echo "The Master Public DNS Name is: $MasterPublicDnsName"
	echo "The EC2 Instance ID of the EMR Master Node is: $Ec2InstanceId"
	echo -e "\n************************************************************************************************************"
}

ssm()
{
	command="sudo -H -u ${user} bash -c \"$JAVA${command}\""
	echo "Command being executed is \"$command\""

	CommandId=$(aws ssm send-command \
		--document-name "AWS-RunShellScript" \
		--parameters "commands=[$command],executionTimeout=$execution_timeout" \
		--output-s3-bucket-name "$s3_bucket_name" \
		--output-s3-key-prefix "$s3_bucket_prefix" \
		--targets "Key=instanceIds,Values=$Ec2InstanceId" | jq -r '.[].CommandId')

	echo "The Command ID received at $(date +%Y-%m-%d\ %H:%M:%S) is: $CommandId"
	if [[ -z $CommandId ]]; then
		echo "ERROR 12: Command Id is Invalid. Failing the Job"
		exit 12
	fi

	# Status: 'Pending'|'InProgress'|'Delayed'|'Success'|'Cancelled'|'TimedOut'|'Failed'|'Cancelling'
	# exit_status='Cancelled TimedOut Failed Cancelling'
	continue_status='Pending InProgress Delayed'

	if [[ $deploy == 'Y' ]]; then
		echo -e "\n************************************************************************************************************"
		echo "Code Deployed to all Running EMRs $ClusterNameList (${Ec2InstanceId}) for the project ${project} at $(date +%Y-%m-%d\ %H:%M:%S)"
		exit 0
	fi

	Status=''
	count=0
	sleep 30

	if [[ ! -z $new_search ]]; then
		search=$new_search
	fi

	while [[ $Status == 'Pending' ]] || [[ $Status == 'InProgress' ]] || [[ $Status == 'Delayed' ]] || [[ -z $Status ]]; do
		Status=$(aws ssm get-command-invocation \
			--command-id $CommandId \
			--instance-id $Ec2InstanceId | jq -r '.Status')
		echo "$Status at $(date +%Y-%m-%d\ %H:%M:%S)"

		if [[ $count -ge 60 ]] && ([[ $Status == 'Pending' ]] || [[ $Status == 'Delayed' ]]); then
			echo "ERROR 13: Command Not started in One Hour, Status is $Status"
			exit 13
		fi

		((count++))
		if [[ ($count % 15) == 0 ]]; then
			check_if_emr_same
		fi

		if [[ $Status == 'Cancelled' ]] || [[ $Status == 'TimedOut' ]] || [[ $Status == 'Failed' ]] || [[ $Status == 'Cancelling' ]] || [[ -z $Status ]]; then
			echo "ERROR 14: Command Failed and Status is $Status"
			get_details
			echo "***********************STDERR Printed below from s3://${s3_bucket_name}/${outputPath}/0.awsrunShellScript/stderr***********************"
			aws s3 cp s3://${s3_bucket_name}/${outputPath}/0.awsrunShellScript/stderr /dev/stderr
			echo -e "\n************************************************************************************************************"
			echo "Script Execution Failed at $(date +%Y-%m-%d\ %H:%M:%S)"
			exit 14
		elif [[ $Status == 'Success' ]]; then
			echo "Command Completed Successfully. Breaking the loop"
			get_details
			echo "Script Execution Completed at $(date +%Y-%m-%d\ %H:%M:%S)"
			break
		fi

		sleep $seconds
	done
}

step()
{
	retry_counter=0

	StepId=$(aws emr add-steps --cluster-id $cluster_id \
		--steps Type=CUSTOM_JAR,Name="$step_name",ActionOnFailure=CONTINUE,Jar=command-runner.jar,Args=["bash","-c","$command"] | jq -r '.StepIds[0]')

	if [[ -z $StepId ]]; then
		echo "ERROR 122: Step Id is Invalid. Failing the Job"
		exit 122
	fi

	echo "************************************************************************************************************"
	echo "EMR Cluster Id is $cluster_id"
	echo "Step ID is $StepId"
	echo "Step name is $step_name"
	echo "************************************************************************************************************"

	Status='UNKNOWN'
	count=0

	while [[ -z $Status ]]; do
		Status=$(aws emr describe-step --cluster-id $cluster_id --step-id $StepId | jq -r '.[].Status.State')
		echo "Step $StepId Status = $Status at $(date +%Y-%m-%d\ %H:%M:%S)"

		if [[ $count -ge 60 ]] && [[ $Status == 'PENDING' ]]; then
			echo "ERROR 123: Step Not started in One Hour, Status is $Status"
			echo "Script Execution Failed at $(date +%Y-%m-%d\ %H:%M:%S)"
			exit 123
		elif [[ $Status == 'CANCELLED' ]] || [[ $Status == 'FAILED' ]] || [[ $Status == 'CANCEL_PENDING' ]] || [[ $Status == 'INTERRUPTED' ]] || [[ -z $Status ]]; then
			echo "ERROR 124: Step Command Failed and Status is $Status"
			echo "Script Execution Failed at $(date +%Y-%m-%d\ %H:%M:%S)"
			exit 124
		elif [[ $Status == 'COMPLETED' ]]; then
			echo "Step Completed Successfully. Breaking the loop"
			echo "Script Execution Completed at $(date +%Y-%m-%d\ %H:%M:%S)"
			break
		elif [[ $count -ge 240 ]] && [[ $Status == 'RUNNING' ]]; then
			echo "ERROR 125: Step Running for more than 4 Hours. Status is $Status"
			echo "Script Execution Failed at $(date +%Y-%m-%d\ %H:%M:%S)"
			exit 125
		elif [[ -z $Status ]]; then
			if [[ $retry_counter -le 3 ]]; then
				((retry_counter++))
				echo "Retrying - Sleeping 60 Seconds: Retry Counter $retry_counter of 3.. Status was Empty"
				sleep 60
			else
				echo "ERROR 126: Step Status is Empty"
				echo "Script Execution Failed at $(date +%Y-%m-%d\ %H:%M:%S)"
				exit 126
			fi
		fi

		((count++))
		sleep $seconds
	done
}

export AWS_SHARED_CREDENTIALS_FILE=/application/financedw/scripts/.aws/credentials
TIMESTAMP=$(date +%Y-%m-%d_%H:%M:%S:%N)
DATE=$(date +%Y-%m-%d)
seconds=60
execution_timeout="43200"
Ec2InstanceIdList=""
ClusterNameList=""
skip_flg='N'
step_name=''

while getopts "e:s:c:u:t:d:r:p:n:" opt; do
	case "$opt" in
		e ) environment="$OPTARG" ;;
		s ) search="$OPTARG" ;;
		c ) command="$OPTARG" ;;
		u ) user="$OPTARG" ;;
		t ) ec2="$OPTARG" ;;
		d ) deploy="$OPTARG" ;;
		r ) route53="$OPTARG" ;;
		p ) project="$OPTARG" ;;
		n ) table_name="$OPTARG" ;;
		? ) helpFunction ;;
	esac
done

if [[ -z $deploy ]] || [[ $deploy == 'n' ]] || [[ $deploy == 'N' ]]; then
	deploy=N
else
	deploy=Y
fi

if [[ -z $route53 ]] || [[ $route53 == 'n' ]] || [[ $route53 == 'N' ]]; then
	route53=N
else
	route53=Y
fi

regular_run='Y'
if [[ $deploy == 'Y' ]] || [[ $route53 == 'Y' ]]; then
	regular_run='N'
fi

if [[ $deploy == 'Y' ]] && [[ $route53 == 'Y' ]]; then
	echo "ERROR 15: Deploy Mode and Route 53 Mode are mutually exclusive. Please enable only one."
	exit 15
fi

if [[ $regular_run == 'Y' ]]; then
	if [[ -z $environment ]] || [[ -z $command ]]; then
		echo "ERROR 2: One of the mandatory arguments (environment name (e) or command to be executed (c)) is not provided, please check"
		exit 2
	fi
fi

if [[ $environment != 'dev' ]] && [[ $environment != 'tst' ]] && [[ $environment != 'mdl' ]] && [[ $environment != 'prd' ]]; then
	echo "ERROR 3: Environment is invalid. Should be one from the list (dev, tst, mdl, prd)"
	exit 3
fi

if [[ $command == *"fullfilemonthendcheck.sh"* ]]; then
	step_name='Full File Monthend Check'
	y=("${command}.sh")
	x=${y[1]}
	step_name="$step_name $x"
elif [[ $command == *"bancs_kc2_notification_monthly.sh"* ]]; then
	step_name='BANCS KC2 Monthly Notification Ingestion'
	y=("${command}.sh")
	x=${y[1]}
	step_name="$step_name $x"
elif [[ $command == *"p65_kc6_run.sh"* ]]; then
	step_name='P65 KC6'
	y=("${command}.sh")
	x=${y[1]}
	step_name="$step_name $x"
elif [[ $command == *"p65_finmod_kc6_parsing_daily.sh"* ]]; then
	step_name='P65 FINMOD KC6 Daily Parsing'
	y=("${command}.sh")
	x=${y[1]}
	step_name="$step_name $x"
elif [[ $command == *"p65_reserves_kc6_parsing_monthly.sh"* ]]; then
	step_name='P65 Reserves FINMOD KC6 Monthly Parsing'
	y=("${command}.sh")
	x=${y[1]}
	step_name="$step_name $x"
elif [[ $command == *"run_dcompletedbatchldinfo.sh"* ]]; then
	step_name='Datastage Completed Batch'
	y=("${command}.sh")
	x=${y[1]}
	step_name="$step_name $x"
elif [[ $command == *"run_landing_controls.sh"* ]]; then
	step_name='Datastage Run Landing'
	y=("${command}.sh")
	x=${y[1]}
	step_name="$step_name $x"
elif [[ $command == *"oldfw_runtimeplate.sh"* ]]; then
	step_name='Datastage Old Framework'
	y=("${command}.sh")
	x=${y[1]}
	step_name="$step_name $x"
fi

echo "Script Execution Started at $(date +%Y-%m-%d\ %H:%M:%S)"
sh /application/financedw/scripts/set_aws_config.sh

s3_bucket_name=ta-individual-findw-${environment}-logs
s3_bucket_prefix=linux/EC2/ssm/runs/${DATE}

# project=`echo $search | awk -F'-' '{print $NF}'`
if [[ $regular_run == 'N' ]] && [[ -z $project ]]; then
	echo "ERROR 4: Project Name is not set for Code Deployment/Route 53. This parameter is Needed to Identify running EMRs"
	exit 4
elif [[ $regular_run == 'N' ]] && [[ -z $user ]]; then
	echo "ERROR 5: User can't be set when the Deploy/Route 53 Mode is set as Y"
	exit 5
elif [[ $regular_run == 'N' ]] && [[ ! -z $command ]]; then
	echo "ERROR 6: Command shouldn't be passed when Deploy/Route 53 Mode is set as Y"
	exit 6
elif [[ $regular_run == 'N' ]] && [[ ! -z $search ]]; then
	echo "ERROR 7: Search shouldn't be passed when Deploy/Route 53 mode is set as Y"
	exit 7
fi

if [ $deploy == 'Y' ]; then
	deploy_command="aws s3 sync s3://ta-individual-findw-${environment}-codedeployment/${project}/scripts/datastage/ /application/financedw/financedatastage/scripts --only-show-errors --delete --exact-timestamps; \
aws s3 sync s3://ta-individual-findw-${environment}-codedeployment/${project}/scripts/curated/ /application/financedw/curated/scripts --only-show-errors --delete --exact-timestamps; \
aws s3 sync s3://ta-individual-findw-${environment}-codedeployment/${project}/scripts/masterdatastore/ /application/financedw/financemasterdatastore/scripts --only-show-errors --delete --exact-timestamps; \
aws s3 sync s3://ta-individual-findw-${environment}-codedeployment/${project}/scripts/annuities/ /application/financedw/annuities/scripts --only-show-errors --delete --exact-timestamps; \
aws s3 sync s3://ta-individual-findw-${environment}-codedeployment/${project}/scripts/controls/ /application/financedw/financecontrols/scripts --only-show-errors --delete --exact-timestamps; \
"
	deploy_command=$deploy_command'find /application/financedw/financedatastage/scripts/ -type f -iname "*.sh" -exec chmod gu+x {} \; ; \
find /application/financedw/financedatastage/scripts/ -type f -iname "*.py" -exec chmod gu+x {} \; ; \
find /application/financedw/curated/scripts/ -type f -iname "*.sh" -exec chmod gu+x {} \; ; \
find /application/financedw/curated/scripts/ -type f -iname "*.py" -exec chmod gu+x {} \; ; \
find /application/financedw/financemasterdatastore/scripts/ -type f -iname "*.sh" -exec chmod gu+x {} \; ; \
find /application/financedw/financemasterdatastore/scripts/ -type f -iname "*.py" -exec chmod gu+x {} \; ; \
find /application/financedw/financecontrols/scripts/ -type f -iname "*.sh" -exec chmod gu+x {} \; ; \
find /application/financedw/financecontrols/scripts/ -type f -iname "*.py" -exec chmod gu+x {} \; ; \
find /application/financedw/annuities/scripts/ -type f -iname "*.sh" -exec chmod gu+x {} \; ; \
find /application/financedw/annuities/scripts/ -type f -iname "*.py" -exec chmod gu+x {} \; ; \
find /application/financedw/financedatastage/scripts/ -type f -iname "*" -exec dos2unix {} \; ; \
find /application/financedw/financemasterdatastore/scripts/ -type f -iname "*" -exec dos2unix {} \; ; \
find /application/financedw/financecontrols/scripts/ -type f -iname "*" -exec dos2unix {} \; ; \
find /application/financedw/curated/scripts/ -type f -iname "*" -exec dos2unix {} \; ; \
find /application/financedw/annuities/scripts/ -type f -iname "*" -exec dos2unix {} \; ;'
fi

if [ $route53 == 'Y' ]; then
	if [ $environment == 'tst' ]; then
		HOSTED_ZONE_NAME=datalake.tanonprod.aegon.io
		HOSTED_ZONE_ID=Z0599639XNOZYKBZOSSM
	elif [ $environment == 'mdl' ]; then
		HOSTED_ZONE_NAME=datalake.tamodel.aegon.io
		HOSTED_ZONE_ID=Z060055926LYNBMPNNEGO
	elif [ $environment == 'prd' ]; then
		HOSTED_ZONE_NAME=datalake.ta.aegon.io
		HOSTED_ZONE_ID=Z059253517K1PNO1A9AIA
	else
		HOSTED_ZONE_NAME=datalake.tanonprod.aegon.io
		HOSTED_ZONE_ID=Z0599639XNOZYKBZOSSM
	fi
fi

if [[ -z $user ]] && [[ -z $ec2 ]]; then
	if [[ -z $search ]] && [ $regular_run == 'Y' ]; then
		echo "ERROR 8: The mandatory arguments EMR search Name (s) is not provided, please check"
		exit 8
	fi
fi

user=hadoop
JAVA="export JAVA_HOME=/etc/alternatives/jre;"

if [[ $deploy == 'Y' ]]; then
	search=$project
	echo "Deploying the Code in the Running EMRs for the Project $project at $(date +%Y-%m-%d\ %H:%M:%S)"
	get_emr_details

elif [[ $route53 == 'Y' ]]; then
	if [ $environment == 'prd' ] || [ $environment == 'mdl' ]; then
		search=$project
		echo "Fetching the Running EMRs details to identify Orphan DNS Records for the Project $project at $(date +%Y-%m-%d\ %H:%M:%S)"
		get_emr_details
	else
		for prefix in '' 'uat' 1 2 3 4 5 6 7 8 9 10; do
			search=${project}${environment}${prefix}
			if [[ -z $prefix ]]; then
				search=${project}
			elif [ $prefix == 'uat' ]; then
				search=financedwuat
			fi
			echo "Fetching the Running EMRs details to identify Orphan DNS Records for the Project $search at $(date +%Y-%m-%d\ %H:%M:%S)"
			get_emr_details
			echo "************************************************************************************************************"
		done
	fi
	echo "Script Execution Completed at $(date +%Y-%m-%d\ %H:%M:%S)"
	exit 0
fi

# If not deploy or route53 flow above, continue to execute command against EMR/EC2
echo "Executing the Command in EMR using ${user} as the user at $(date +%Y-%m-%d\ %H:%M:%S)"
if [[ $search != *"-maintenance-" ]] && [[ $search != *"-annuities-" ]] && [[ $search != *"-curated-" ]]; then
	cnt=$(echo $search | sed -e 's/\(-\)/\1\n/g' | grep - | wc -l)
	((cnt=cnt+1))
	project_name=$(cut -d - -f$cnt <<<"${search}")
	### Decide on This Later
fi
get_emr_details

if [[ -z $ec2 ]]; then
	echo "ERROR 101: EC2 type (t) is mandatory when User (u) is provided."
	exit 101
elif [ $ec2 != 'dei' ] && [ $ec2 != 'findw' ] && [ $ec2 != 'pctr' ] && [ $ec2 != 'datalake' ]; then
	echo "ERROR 102: Invalid EC2 type $ec2. Allowed values are dei or findw or pctr or datalake."
	exit 102
elif [[ -z $user ]]; then
	echo "ERROR 103: User (u) is mandatory when Ec2 Type (t) is provided."
	exit 103
elif [ $user != 'sptactfindev' ] && [ $user != 'sptactfinest' ] && [ $user != 'sptactfinmodel' ] && [ $user != 'sptactfinprod' ]; then
	echo "ERROR 104: Invalid Support Account ($user) passed. Should be from the list sptactfindev, sptactfinest, sptactfinmodel and sptactfinprod."
	exit 104
elif [[ -z $search ]]; then
	echo "ERROR 105: EMR Search Name (s) is not needed when User (u) and Ec2 Type (t) is provided, to run the script in an EC2 Machine."
	exit 105
fi

if [ $ec2 == 'dei' ] || [ $ec2 == 'pctr' ]; then
	ec2name=ta-technology-emap-lx-${ec2}-${environment}
else
	ec2name=ta-individual-compute-${ec2}-${environment}
fi

echo "Executing the Command in $ec2 EC2 Server $ec2name using ${user} as the user"

if [[ "$command" == "/efs/informatica/ControlM/" ]] && [ $ec2 == 'dei' ]; then
	if [[ -z $project ]]; then
		echo "ERROR 106: Project Name is mandatory to check for the existence of the BDE EMR to run the Control M command for the DEI Mapping"
		exit 106
	fi
	search=ondemand-emr-all-BDE-$project
	echo "Checking if an EMR exists with the name $search at $(date +%Y-%m-%d\ %H:%M:%S)"
	get_emr_details
fi

Ec2InstanceId=$(aws ec2 describe-instances --filter "Name=tag-key,Values=Name" "Name=tag-value,Values=${ec2name}" "Name=instance-state-name,Values=running" | jq -r '.Reservations[].Instances[].InstanceId')
echo "The EC2 Instance ID Received at $(date +%Y-%m-%d\ %H:%M:%S) is: $Ec2InstanceId"
if [[ -z $Ec2InstanceId ]]; then
	echo "ERROR 107: Failed to Retrieve the Instance ID of the EC2 machine ${ec2name}. Please check."
	exit 107
fi

if [ $skip_flg == 'Y' ]; then
	echo -e "\n************************************************************************************************************"
	echo "Skipping Code Deployment as there is no running EMR at $(date +%Y-%m-%d\ %H:%M:%S)."
	exit 0
fi

if [ $deploy == 'Y' ] && [ $user == 'hadoop' ]; then
	command=$deploy_command
fi

if [ $environment == 'mdl' ] || [[ "$command" == *"s3_parquet_merge.sh"* ]]; then
	execution_timeout="86400"
fi

if [[ ! -z $table_name ]] && [ $regular_run == "Y" ]; then
	sh /application/financedw/scripts/utilities/msck_repair.sh $cluster_id $table_name
	RC=$?
	exit $RC
fi

if [[ -z $step_name ]]; then
	ssm
else
	step
fi