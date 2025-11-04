"""
Purpose: Wrapper for submitting steps to EMR from EC2. Takes step and all its arguments, and submits it to the EMR by 
         calling respective functions from aws_utils.py
Usage1: python3 step_submit.py -ev tst -c j-1MN26X9PHUH5J0 --name aum_bancs --script_type "bash" --script_uri \
        /application/financedw/annuities/scripts/run_curated.sh --script_args "s BaNCS d aum -f monthly" --action_on_failure CONTINUE
Usage2: python3 step_submit.py -ev tst --cluster_id $clusterid --action_on_failure CONTINUE --name aum_bancs --script_type "spark-submit" --script_uri \
        /application/financedw/annuities/scripts/load_curated.py --script_args "e tst p financedw s BaNCS d aum -f monthly" \
        --spark_args "--deploy-mode client --master yarn --num-executors 30 \
        --executor-cores 5 --executor-memory 30g --driver-memory 18g --conf spark.sql.broadcastTimeout=2000 \
        --conf spark.sql.autoBroadcastJoinThreshold=50M --conf spark.executor.memoryOverhead=4g" \
        --py_files "s3://ta-individual-findw-tst-codedeployment/financedw/scripts/annuities/scripts.zip"
Usage3: python3 step_submit.py -ev tst --cluster_id $clusterid --action_on_failure CONTINUE --name aum_bancs --script_type "spark-submit" --script_uri \
        "s3://ta-individual-findw-tst-codedeployment/financedw/scripts/annuities/load_curated.py" \
        --script_args "-e tst -p financedw -s BaNCS -d aum -f monthly" \
        --spark_args "--deploy-mode client --master yarn --num-executors 30 \
        --executor-cores 5 --executor-memory 30g --driver-memory 18g --conf spark.sql.broadcastTimeout=2000 \
        --conf spark.sql.autoBroadcastJoinThreshold=50M --conf spark.executor.memoryOverhead=4g" \
        --py_files "s3://ta-individual-findw-tst-codedeployment/financedw/scripts/annuities/scripts.zip"

12/17/2021                LTCG = Initial Development
                          Sowjanya J
"""

import boto3
import argparse
import time
import traceback
import sys
from datetime import datetime, date
from aws_utils import *
from botocore.client import Config
region = 'us-east-1'
config = Config(retries={'max_attempts': 10, 'mode': 'adaptive'})
emr_client = boto3.client('emr', region_name=region, config=config)
s3_client = boto3.resource('s3', region_name=region, config=config)

def parse_arguments(args):
    args.add_argument(
        '-ev', '--env',
        required=True,
        choices=['dev', 'tst', 'mdl', 'prd'],
        help='Environment Where Script Needs to Run'
    )
    args.add_argument(
        '-c', '--cluster_id',
        required=True,
        help='Cluster id of the emr'
    )
    args.add_argument(
        '-n', '--name',
        required=True,
        help='Name of the step'
    )
    args.add_argument(
        '-ty', '--script_type',
        type=str,
        required=True,
        choices=['bash', 'python', 'python3', 'spark-submit'],
        help='Type of script to be run'
    )
    args.add_argument(
        '-uri', '--script_uri',
        type=str,
        required=True,
        help='S3/Linux Location of the Script'
    )
    args.add_argument(
        '-ar', '--script_args',
        type=str,
        required=False,
        help='List of arguments for the Script',
        default=''
    )
    args.add_argument(
        '-sp', '--spark_args',
        type=str,
        required=False,
        help='List of Spark arguments to set Spark Properties. Mandatory for a Spark Program',
        default=None
    )
    args.add_argument(
        '-py', '--py_files',
        type=str,
        required=False,
        help='S3 Zip File location for the Spark Scripts Directory. Mandatory if script_uri is an S3 Location',
        default=None
    )
    args.add_argument(
        '-ac', '--action_on_failure',
        required=False,
        help='Action on failure of the step',
        choices=['TERMINATE_JOB_FLOW', 'TERMINATE_CLUSTER', 'CANCEL_AND_WAIT', 'CONTINUE'],
        default='CONTINUE'
    )
    sys_args = args.parse_args()
    return vars(sys_args)

def step_status(cluster_id, step_id, emr_client, tries=1):
    try:
        response, status = describe_step(cluster_id, step_id, emr_client)
        return response, status
    except Exception as e:
        if e.response['Error']['Code'] == 'ThrottlingException':
            if tries <= 6:
                print(f"Throttling Exception Occurred... {e.response}")
                print(f"Retrying.....Attempt No - {tries} of total 6")
                time.sleep(20 * tries)
                tries += 1
                response, status = step_status(cluster_id, step_id, emr_client, tries)
                return response, status
            else:
                raise Exception(f"Attempted 6 Times to Describe Step But No Success... Failing due to ThrottlingException.. {e.response}")
        else:
            raise Exception(f"Following Exception Occurred {e}")

def step_submit(sys_args):
    step_final_status = ['CANCELLED', 'FAILED', 'CANCEL_PENDING', 'INTERRUPTED']
    if sys_args['script_type'] == 'spark-submit':
        step_id = add_step_spark_submit(sys_args['cluster_id'], sys_args['name'], sys_args['script_uri'],
                                        sys_args['script_args'], emr_client, sys_args['action_on_failure'],
                                        sys_args['spark_args'], sys_args['py_files'])
    else:
        step_id = add_step_command_runner(sys_args['cluster_id'], sys_args['name'], sys_args['script_uri'],
                                          sys_args['script_args'], emr_client, sys_args['action_on_failure'],
                                          sys_args['script_type'])
    count = 0
    while True:
        count += 1
        response, status = step_status(sys_args['cluster_id'], step_id, emr_client, tries=1)
        if status in step_final_status:
            raise Exception(f"Step {sys_args['name']} with ID {step_id} is now at status {status} at {datetime.now()}")
        elif status == 'PENDING' and count >= 60:
            raise Exception(f"Step {sys_args['name']} with ID {step_id} has been in PENDING state since an hour. Please check.")
        elif status == 'RUNNING' and count >= 240:
            raise Exception(f"Step {sys_args['name']} with ID {step_id} has been in RUNNING state since Four hours. Please check, the job/step may be still running.")
        elif status == 'COMPLETED':
            print(f"Step {sys_args['name']} with ID {step_id} is now COMPLETED at {datetime.now()}")
            break
        print("Sleeping for 60 seconds")
        time.sleep(60)

if __name__ == '__main__':
    try:
        args = argparse.ArgumentParser()
        sys_args = parse_arguments(args)
        print(sys_args)
        if sys_args['script_type'] == 'spark-submit':
            if not sys_args['spark_args']:
                raise Exception("Argument spark_args is Mandatory for a Spark Program")
            if 's3://' in sys_args['script_uri']:
                if not sys_args['py_files']:
                    raise Exception("Argument py_files is Mandatory if script_uri is an S3 Location")
            else:
                sys_args['py_files'] = '--py-files ' + sys_args['py_files']
        step_submit(sys_args)
    except Exception as e:
        traceback.print_exc()
        print(f"Error50: The following exception has occurred\n", e)
        print("Script Errored at " + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
        sys.exit(50)