"""
Purpose: Common Functions for EMR Actions to Run Scripts as Step from Airflow
Usage: from emr_actions import *
--------------------DEVELOPMENT LOG--------------------
01/16/2024 - Prathyush Premachandran - Finance DW framework development
"""

import time
import boto3
import re
import json

from botocore.exceptions import ClientError
from botocore.client import Config
from datetime import datetime, timedelta, date
from airflow.exceptions import AirflowException
from airflow.models import Variable

region = 'us-east-1'
config = Config(retries={'max_attempts': 15, 'mode': 'adaptive'})
emr_client = boto3.client('emr', region_name=region, config=config)
s3_resource = boto3.resource('s3', region_name=region, config=config)
s3_client = boto3.client('s3', region_name=region, config=config)
glue_client = boto3.client('glue', region_name=region, config=config)

def run_job_flow(env, emr_name, source_system, project, cycle_date, emr_size = None, emr_auto_scale = None, job_flow_alive = True, emr_version = None):
    timestamp = datetime.now()
    timestamp = timestamp.strftime("%Y-%m-%d-%H-%M-%S")
    from finance.{project_name}.configs.emr_params import emr_config
    emr_config = eval(emr_config)
    if env in ['dev','tst']:
        emr_auto_scale = 'small'
        for i in range(0, len(emr_config['Configurations'])):
            if emr_config['Configurations'][i].get('Classification') == 'spark-defaults':
                emr_config['Configurations'][i]['Properties']['spark.executor.cores'] = "5"
                emr_config['Configurations'][i]['Properties']['spark.executor.memory'] = "15G"
        if not emr_size:
            emr_size = 'small'
    else:
        emr_auto_scale = 'medium'
        if not emr_size:
            emr_size = 'medium'
        if env in ['ind','prd']:
            emr_size = 'large'
        if emr_name in ['maintenance','annuities']:
            emr_auto_scale = 'large'
    #if env == 'prd':
    #   emr_config['ReleaseLabel'] = 'emr-6.15.0'
    if emr_version:
        emr_config['ReleaseLabel'] = emr_version
    if emr_version in ['emr-7.2.0','emr-7.3.0']:
        emr_config['applications'] = ['sqoop']
        emr_major_version = int(emr_config['ReleaseLabel'].split('-')[1].split('.')[0])
        emr_minor_version = int(emr_config['ReleaseLabel'].split('-')[1].split('.')[1])
        if ('BootstrapActions' in emr_config and ({'Name': 'GTS BootStrap Action', 'ScriptBootstrapAction': {'Path': 's3://gts-emr-bootstrap-prod/emr-bootstrap.sh'}} in emr_config['BootstrapActions'])) and (emr_major_version == 7 and emr_minor_version >= 3)) or emr_major_version < 7:
            oozie_hit = None
            for i in range(0, len(emr_config['Configurations'])):
                if emr_config['Configurations'][i].get('Classification') == 'oozie-site':
                    oozie_hit = True
                    break
            if oozie_hit:
                emr_config['Configurations'].pop(i)
    try:
        response = emr_client.run_job_flow(
            Name=emr_config['Name'],
            LogUri=emr_config['LogUri'],
            ReleaseLabel=emr_config['ReleaseLabel'],
            Instances={
                'InstanceFleets': emr_config['InstanceFleets'][emr_size],
                'KeepJobFlowAliveWhenNoSteps': job_flow_alive,
                'EmrManagedMasterSecurityGroup': emr_config['EmrManagedMasterSecurityGroup'][env],
                'EmrManagedSlaveSecurityGroup': emr_config['EmrManagedSlaveSecurityGroup'][env],
                'Ec2KeyName': emr_config['Ec2KeyName'][env],
                'AdditionalMasterSecurityGroups': emr_config['AdditionalMasterSecurityGroups'][env],
                'AdditionalSlaveSecurityGroups': emr_config['AdditionalSlaveSecurityGroups'][env],
                'ServiceAccessSecurityGroup': emr_config['ServiceAccessSecurityGroup'][env],
                'Ec2SubnetId': emr_config['Ec2SubnetIds'][env]
            },
            BootstrapActions=emr_config['BootstrapActions'],
            Applications=[
                {'Name': app}
                for app in emr_config['applications']
            ],
            Configurations=emr_config['Configurations'],
            JobFlowRole=emr_config['JobFlowRole'],
            ServiceRole=emr_config['ServiceRole'],
            AutoScalingRole=emr_config['AutoScalingRole'],
            SecurityConfiguration=emr_config['SecurityConfiguration'],
            ScaleDownBehavior='TERMINATE_AT_TASK_COMPLETION',
            EbsRootVolumeSize=emr_config['EbsRootVolumeSize'],
            VisibleToAllUsers=True,
            Tags=emr_config['Tags']
        )
        cluster_id = response['JobFlowId']
        print("Created cluster:", cluster_id)
    except Exception as e:
        raise AirflowException(f"Couldn't create cluster. Error - {e}")
    else:
        return cluster_id

def describe_cluster(cluster_id):
    try:
        response = emr_client.describe_cluster(ClusterId=cluster_id)
        cluster = response['Cluster']
        MasterPublicDnsName = cluster.get('MasterPublicDnsName', 'Master Node IP is Unavailable at this time')
        status = cluster['Status']
        StateChangeReason = status['StateChangeReason']
        StateChangeReasonMsg = StateChangeReason.get('Message', 'Cluster Status Change Reason is Unavailable at this time')
        print("********************************************************")
        print("Cluster Name:", cluster['Name'])
        print("Cluster ID:", cluster_id)
        print("Cluster Status:", status['State'])
        print("Cluster IP:", MasterPublicDnsName)
        print("Cluster Status Change Reason:", StateChangeReasonMsg)
        print("********************************************************")
    except Exception as e:
        raise AirflowException(f"Couldn't get data for cluster (cluster_id). Error - {e}")
    else:
        return cluster['Name'], status['State']

def terminate_cluster(cluster_id: str) -> None:
    try:
        emr_client.terminate_job_flows(JobFlowIds=[cluster_id])
        print("Terminated cluster:", cluster_id)
    except Exception as e:
        raise AirflowException(f"Couldn't terminate cluster (cluster_id). Error - {e}")

def list_steps_states_cnt(cluster_id, check_step_states: list):
    try:
        cnt = 0
        i = 0
        response = emr_client.list_steps(ClusterId=cluster_id)
        while i < len(response['Steps']):
            if response['Steps'][i]['Status']['State'] in check_step_states:
                cnt += 1
            i += 1
        return cnt
    except Exception as e:
        raise AirflowException(f"list_steps_states_cnt - Couldn't get data for cluster (cluster_id). Error - {e}")

def read_s3(s3_bucket, s3_querypath):
    data = s3_client.get_object(Bucket=s3_bucket, Key=s3_querypath)
    code = data['Body'].read()
    return code.decode('utf-8')

def check_emr_cluster(cluster_id, emr_full_name, action = 'create', from_emr_terminator = False):
    cluster_final_invalid_status = ['TERMINATED', 'TERMINATING', 'TERMINATED_WITH_ERRORS']
    cluster_ids = check_duplicate_emr(emr_full_name, action)
    ret_val = cluster_id
    cluster_final_valid_status = ['WAITING', 'RUNNING']
    if from_emr_terminator and action == 'terminate':
        env = emr_full_name.split('-')[3]
        emr_ss = emr_full_name.split('-')[6]
        emr_nm = emr_full_name.split('-')[7]
        prj = emr_full_name.split('-')[8]
        bucket = f'fta-individual-fin_dw-{env}-logs'
        for cluster_id in cluster_ids:
            file = f'{prj}/emrs/{emr_nm}_{emr_ss}_{cluster_id}'
            emr_output = f's3://{bucket}/{file}'
            kwargs = {}
            kwargs['emr_cluster_id'] = cluster_id
            kwargs['step_name'] = '1 Check if Applications Running in EMR'
            kwargs['action_on_failure'] = 'CONTINUE'
            kwargs['script_type'] = 'bash'
            kwargs['script_args'] = emr_output
            kwargs['script_uri'] = '/application/financedw/curated/scripts/utilities/check_running_apps.sh'
            step_submit(**kwargs)
            cnt_running_apps = read_s3(bucket, file)
            cnt_running_apps = int(cnt_running_apps.replace('\n','0'))
            cnt_pending = list_steps_states_cnt(cluster_id, ['PENDING'])
            name, state = describe_cluster(cluster_id)
            if state != 'RUNNING' and not cnt_pending and not cnt_running_apps:
                terminate_cluster(cluster_id)
            else:
                print(f"*****Cluster ID {cluster_id} of {name} is not Terminated because its Current State is {state} (expected WAITING) or It has {cnt_pending} Pending Steps (expecting Zero) or It has {cnt_running_apps} Running Applications (expecting Zero)*****")
    elif action == 'terminate':
                for cluster_id in cluster_ids:
            terminate_cluster(cluster_id)
    elif cluster_ids and action != 'terminate' and not from_emr_terminator:
        cluster_id = cluster_ids[0]
        ret_val = cluster_id
        if cluster_id:
            cnt = 0
            while True:
                name, state = describe_cluster(cluster_id)
                if state in cluster_final_invalid_status:
                    raise AirflowException(f"EMR Cluster {name} - ID ({cluster_id}) is in Invalid state ({state})")
                elif state in cluster_final_valid_status:
                    break
                cnt += 1
                print("Sleeping for 60 seconds")
                time.sleep(60)
                if cnt % 3 == 0 and emr_full_name:
                    check_duplicate_emr(emr_full_name)
    else:
        print(f"There are no EMRS like ({emr_full_name})")
    return ret_val

def check_duplicate_emr(emr_full_name, action = 'create'):
    emr_clusters = emr_client.list_clusters(ClusterStates=['STARTING','BOOTSTRAPPING','RUNNING','WAITING'])
    cluster_ids = []
    for cluster in emr_clusters['Clusters']:
        if emr_full_name in cluster['Name']:
            cluster_ids.append(cluster['Id'])
    if len(cluster_ids) > 1 and action == 'create':
        for cluster_id in cluster_ids:
            terminate_cluster(cluster_id)
        raise AirflowException(f"More than One EMR Cluster Exists for ({emr_full_name}) and all are Terminated - ({cluster_ids})")
    return cluster_ids

def add_step_spark_submit(cluster_id, name, script_uri, script_args, action_on_failure, spark_args, py_files=None):
    try:
        if py_files:
            py_files = py_files.split(' ')
        else:
            py_files = []
        script_args = script_args.split(' ')
        if script_args == ['']:
            script_args = []
        if spark_args == ['']:
            spark_args = []
        spark_args = spark_args.split(' ')
        response = emr_client.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[{
                'Name': name,
                'ActionOnFailure': action_on_failure,
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['spark-submit'] + spark_args + py_files + [script_uri] + script_args
                }
            }]
        )
        step_id = response['StepIds'][0]
        print("Started step with ID", step_id)
    except Exception as e:
        raise AirflowException(f"Couldn't start step ({name}) with URI ({script_uri}). EMR Cluster ({cluster_id}) may be missing.... Error - {e}")
    else:
        return step_id

def add_step_command_runner(cluster_id, name, script_uri, script_args, action_on_failure, script_type):
    try:
        if script_args:
            script_args = script_args.split(' ')
        else:
            script_args = []
        response = emr_client.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[{
                'Name': name,
                'ActionOnFailure': action_on_failure,
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [script_type, script_uri] + script_args
                }
            }]
        )
        step_id = response['StepIds'][0]
        print("Started step with ID", step_id)
    except Exception as e:
        raise AirflowException(f"Couldn't start step ({name}) with URI ({script_uri}). EMR Custer ({cluster_id}) may be missing.... Error - {e}")
    else:
        return step_id

def add_step_script_runner(cluster_id, name, script_uri, script_args, action_on_failure):
    try:
        response = emr_client.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[{
                'Name': name,
                'ActionOnFailure': action_on_failure,
                'HadoopJarStep': {
                    'Jar': 's3://' + region + '.elasticmapreduce/libs/script-runner/script-runner.jar',
                    'Args': [script_uri] + script_args.split(' ')
                }
            }]
        )
        step_id = response['StepIds'][0]
        print("Started step with ID", step_id)
    except Exception as e:
        raise AirflowException(f"Couldn't start step ({name}) with URI ({script_uri})..... Error - {e}")
    else:
        return step_id

def describe_step(cluster_id, step_id, tries=1):
    try:
        response = emr_client.describe_step(
            ClusterId=cluster_id,
            StepId=step_id
        )
        Step = response['Step']
        Status = Step['Status']
        State = Status['State']
        StateChangeReason = Status['StateChangeReason']
        StateChangeReasonMsg = StateChangeReason.get('Message', 'Step Status Change Reason is Unavailable at this time')
        print("********************************************************")
        print("Step Id:", Step['Id'])
        print("Step Name:", Step['Name'])
        print("Step Status:", State)
        print("Step Status Change Reason:", StateChangeReasonMsg)
        print("Got data for Step:", Step['Name'], "in Cluster", cluster_id)
        print("********************************************************")
        return Step, State
    except Exception as e:
        if e.response['Error']['Code'] == 'ThrottlingException':
            boto3_retries = Variable.get('boto3_retries', '{}')
            boto3_retries = json.loads(boto3_retries)
            throttling_retries = boto3_retries.get('throttling_retries', {})
            retries = throttling_retries.get('retries', 6)
            sleep_secs = throttling_retries.get('sleep_secs', 30)
            if tries < retries:
                print(f"Throttling Exception Occured... {e.response}")
                print(f"Attempt No - {tries} of total {retries}")
                print(f"Retrying....Sleeping for {sleep_secs * tries} Seconds")
                time.sleep(sleep_secs * tries)
                tries += 1
                Step, State = describe_step(cluster_id, step_id, tries)
                return Step, State
            else:
                raise AirflowException(f"Attempted 6 Times to Describe Step But No Success due to ThrottlingException. Error - {e}")
        else:
            raise AirflowException(f"Couldn't get data for step ({step_id}), in Cluster ({cluster_id}). Error - {e}")

def step_status(cluster_id, step_id, tries=1):
    try:
        response, status = describe_step(cluster_id, step_id, tries)
        return status
    except Exception as e:
        raise AirflowException(f"Following Exception Occurred - {e}")

def step_checker(cluster_id, step_id, step_name=None, terminate=False):
    step_final_status = ['CANCELLED','FAILED','CANCEL_PENDING','INTERRUPTED']
    count = 0
    error = None
    secs = 60
    hours_allowed = 4
    if 'nydfs' in step_name.lower() or 'parquet merge' in step_name.lower():
        hours_allowed = 12
    pending_secs = 60/secs * 60
    running_secs = 60/secs * 60 * hours_allowed
    while True:
        count += 1
        status = step_status(cluster_id, step_id, tries=2)
        if status in step_final_status:
            error = f"Step {step_name} with ID ({step_id}) is now at status ({status}) at ({datetime.now()})"
        elif status == 'PENDING' and count >= pending_secs:
            error = f"Step {step_name} with ID ({step_id}) has been in PENDING state since an hour. Please check."
        elif status == 'RUNNING' and count >= running_secs:
            error = f"Step {step_name} has been in RUNNING state since ({hours_allowed}) hours. Please check, the job/step may be still running"
        elif status == 'COMPLETED':
            print(f"Step {step_name} with ID ({step_id}) is now COMPLETED at ({datetime.now()})")
            break
        if error:
            if terminate:
                terminate_cluster(cluster_id)
            raise AirflowException(error)
        print("Sleeping for 60 seconds")
        time.sleep(secs)

def step_submit(**kwargs):
    if kwargs['script_type'] == 'spark-submit':
        step_id = add_step_spark_submit(kwargs['emr_cluster_id'], kwargs['step_name'], kwargs['script_uri'],
                                        kwargs['script_args'], kwargs['action_on_failure'], kwargs['spark_args'])

           



