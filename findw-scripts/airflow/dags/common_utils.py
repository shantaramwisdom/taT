"""
Purpose: Common utils for framework - Contains functions to be used by other pyspark scripts in framework
Usage: from common_utils import *
----------------DEVELOPMENT LOG----------------
08/09/2021 - MD Jabbar - FINANCEDW framework development
11/16/2022 - Prathyush Premachandran / Sandeep Thalla - Redundant code cleanup & enhancements to existing function
"""
import configparser
import json
import time
import re
import boto3
import psycopg
import pandas as pd
import base64
import io
import tempfile
import os
from airflow.models import (TaskInstance, Variable, DagRun)
from airflow.operators.python import PythonOperator
from airflow.utils.state import State
from airflow.exceptions import AirflowException, AirflowSkipException
import airflow.settings as settings
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import ShortCircuitOperator
from finance.{project_name}.customtasksensor import TimeRangeExternalTaskSensor
from finance.{project_name}.custom_smtp_email import send_email_smtp
from finance.{project_name}.emr_actions import *
from finance.{project_name}.emr_serverless import *
from finance.{project_name}.config_qa import *
from botocore.exceptions import ClientError
from datetime import datetime, timedelta, date
from pytz import timezone, utc
from botocore.client import Config

boto3_config = Config(retries={'max_attempts': 15, 'mode': 'adaptive'})
config_interpolation = configparser.ExtendedInterpolation()
config = configparser.ConfigParser(interpolation=config_interpolation)
# Ensure psycopg encoding mapping
psycopg._encodings.py_codecs['utf8'] = 'utf-8'
psycopg._encodings.py_codecs.update(
    (k.encode(), v) for k, v in psycopg._encodings._py_codecs.items()
)
boto3_config = Config(retries={'max_attempts': 15, 'mode': 'adaptive'})
region_name = 'us-east-1'
s3_client = boto3.client('s3', region_name=region_name, config=boto3_config)
glue_client = boto3.client('glue', region_name=region_name, config=boto3_config)
sns_client = boto3.client('sns', region_name=region_name, config=boto3_config)
emr_client = boto3.client('emr', region_name=region_name, config=boto3_config)
ec2_client = boto3.client('ec2', region_name=region_name, config=boto3_config)
ssm_client = boto3.client('ssm', region_name=region_name, config=boto3_config)
athena_client = boto3.client('athena', region_name=region_name, config=boto3_config)

env_acct_dict = {"tst": "209499357643", "dev": "209499357643", "mdl": "510200178882", "prd": "492931852779"}

# Check for S3 Existence
def check_s3_existence(location):
    try:
        path = location.replace("s3://", "").split("/")
        bucket = path.pop(0)
        prefix = "/".join(path)
        result = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        if 'Contents' in result:
            return True
        else:
            return False
    except Exception as e:
        print("Error in check_s3_existence: {}".format(e))
        return False

# Get the External Table's Location details from Glue
def glue_table_location(database, table_name):
    tbl_data = glue_client.get_table(DatabaseName=database, Name=table_name)
    location = tbl_data['Table']['StorageDescriptor']['Location']
    print(f"Table {database}.{table_name} location: {location}")
    return location

# Updates the DAG Status at the end based on Task's Statuses
def final_status(**kwargs):
    for task_instance in kwargs['dag_run'].get_task_instances():
        if task_instance.current_state() == State.FAILED and \
           task_instance.task_id != kwargs['task_instance'].task_id:
            raise AirflowException("Task {} failed. Failing this DAG run".format(task_instance.task_id))

# Set Common Variables
def common_var(applicationName):
    base_dir = '/usr/local/airflow'
    config_file_path = base_dir + '/dags/finance/{project_name}/configs/config.ini'
    config.read(config_file_path)
    dag_id = config.get('dag_config', 'dag_name')
    dag_default_args = eval(config.get('dag_config', 'dag_default_args'))
    schedule_interval = config.get('dag_config', 'schedule_interval')
    default_region = config.get('dag_config', 'aws_default_region')
    # NOTE: original code referenced 'application' and shared_emr check - keep similar behavior
    try:
        if applicationName == eval(config.get('finwd_config', 'shared_emr')):
            shared_emr_list = sorted([i for i in applicationName['source_systems']])
            return base_dir, config_file_path, dag_id, dag_default_args, schedule_interval, default_region, shared_emr_list
    except Exception:
        pass
    return base_dir, config_file_path, dag_id, dag_default_args, schedule_interval, default_region

def getDagParams(project_name, tag, source_system, batch_frequency=None, domain=None, application=None):
    """
    The function get all dag variables from stored in config.ini.
    """
    base_dir = '/usr/local/airflow'
    config_file_path = base_dir + '/dags/finance/' + project_name + '/configs/config.ini'
    config.read(config_file_path)
    dag_id = config.get('dag_config', 'dag_name')
    dag_default_args = eval(config.get('dag_config', 'dag_default_args'))
    schedule_interval = config.get('dag_config', 'schedule_interval')
    tags = eval(config.get('dag_config', 'tag'))
    default_region = config.get('dag_config', 'aws_default_region')
    domain_dict = eval(config.get('finwd_config', 'domain_dict'))
    sp_dict = eval(config.get('finwd_config', 'sp_dict'))
    is_paused_upon_creation = True
    if project_name == 'financedw':
        is_paused_upon_creation = False
    if isinstance(schedule_interval, str) and schedule_interval.lower() == 'none':
        schedule_interval = None
    baseline_domain_list_finwd = []
    derived_domain_list = []
    try:
        dict_wrk = domain_dict['baseline'][domain]
        baseline_domain_list_finwd = [key for key, value in dict_wrk.items() if source_system in value]
    except Exception:
        pass
    try:
        dict_wrk = domain_dict['derived'][domain]
        derived_domain_list = [key for key, value in dict_wrk.items() if source_system in value]
    except Exception:
        pass
    try:
        if application == eval(config.get('finwd_config', 'shared_emr')):
            shared_emr = eval(config.get('finwd_config', 'shared_emr'))
            shared_emr_list = sorted([i for i in shared_emr if i == source_system])
            return config_file_path, dag_id, dag_default_args, schedule_interval, tags, is_paused_upon_creation, sp_dict, default_region, baseline_domain_list_finwd, derived_domain_list, shared_emr_list
    except Exception:
        pass
    return config_file_path, dag_id, dag_default_args, schedule_interval, tags, is_paused_upon_creation, sp_dict, default_region, baseline_domain_list_finwd, derived_domain_list

# Get EMR Cluster ID
def get_emr_clusterid(**kwargs):
    env = kwargs['task_instance'].xcom_pull(task_ids='start', key='env')
    project = kwargs['task_instance'].xcom_pull(task_ids='start', key='project')
    emr_step = kwargs['emr_name']
    try:
        source_system_name = kwargs['source_system_name']
    except Exception:
        source_system_name = None
    if not source_system_name:
        source_system_name = 'all'
    emr_name = f"fta-individual-finwd-{env}-ondemand-emr-{source_system_name}-{emr_step}-{project}-"
    emr_clusters = emr_client.list_clusters(ClusterStates=['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING'])
    cluster_id = []
    for cluster in emr_clusters.get('Clusters', []):
        if emr_name in cluster.get('Name', ''):
            cluster_id.append(cluster.get('Id'))
    if len(cluster_id) == 0 or len(cluster_id) > 1:
        raise AirflowException(f"More than one emr exists for the given emr configuration or No EMR Found. {cluster_id}")
    print(f"EMR Cluster Id is {cluster_id}")
    while True:
        response = emr_client.describe_cluster(ClusterId=cluster_id[0])
        cluster_state_change_reason = response['Cluster']['Status']['StateChangeReason'].get('Message', '')
        cluster_status = response['Cluster']['Status']['State']
        cluster_name = response['Cluster']['Name']
        MasterPublicDnsName = response['Cluster'].get('MasterPublicDnsName', 'Master Node IP is Unavailable at this time')
        print(f"EMR Cluster Name: {cluster_name}")
        print(f"EMR Cluster Status: {cluster_status}")
        print(f"EMR Cluster IP: {MasterPublicDnsName}")
        print(f"EMR Cluster Status Change Reason: {cluster_state_change_reason}")
        if cluster_status == 'WAITING':
            print(f"EMR Cluster Id {cluster_id[0]} is Ready and In WAITING state")
            kwargs['task_instance'].xcom_push(key='cluster_id', value=cluster_id[0])
            break
        print(f"Cluster not in waiting status - Sleeping 60 secs")
        time.sleep(60)
        print("##########################################################")

# External DAG Task Sensor
def external_task_sensor(dag, predecessor_task, successor_task, dag_prefix, external_task_id, allowed_states, source_list, dag_suffix=None):
    quarterly_systems = ['alm', 'performanceplus', 'arr']
    monthly_systems = ['horizonfisr17', 'clearwater', 'icoseminerdocdir', 'oracleap_escheatment_mtly', 'arr_contract', 'yardi']
    if not dag_suffix:
        dag_suffix = ''
    else:
        dag_suffix = '-' + dag_suffix
    with TaskGroup(group_id="task_sensors") as task_sensor_tg:
        if source_list:
            for source in source_list:
                dag_suffix_t = dag_suffix
                if source in quarterly_systems:
                    dag_suffix_t = dag_suffix_t.replace('DAILY', 'QUARTERLY')
                elif source in monthly_systems:
                    dag_suffix_t = dag_suffix_t.replace('DAILY', 'MONTHLY')
                sc_external_task_sensor_main = ShortCircuitOperator(
                    task_id='sc_external_task_sensor_main_' + source,
                    python_callable=TimeRangeExternalTaskSensor.poke,
                    op_kwargs={
                        "external_task_id": external_task_id,
                        "external_dag_id": dag_prefix.upper() + '-' + source.upper() + dag_suffix_t.upper(),
                        "allowed_states": allowed_states
                    },
                    dag=dag)
        else:
            sc_external_task_sensor_main = ShortCircuitOperator(
                task_id='sc_external_task_sensor_main' + dag_suffix,
                python_callable=TimeRangeExternalTaskSensor.poke,
                op_kwargs={
                    "external_task_id": external_task_id,
                    "external_dag_id": dag_prefix.upper() + dag_suffix.upper(),
                    "allowed_states": allowed_states
                },
                dag=dag)
    predecessor_task >> task_sensor_tg >> successor_task

# External DAG Task Sensor for MAIN TAG
def external_task_sensor_main(dag, predecessor_task, successor_task, dag_prefix, external_task_id, allowed_states, source_list, dag_suffix=None):
    if not dag_suffix:
        dag_suffix = ''
    else:
        dag_suffix = '-' + dag_suffix
    with TaskGroup(group_id="task_sensors") as task_sensor_tg:
        sc_external_task_sensor_ingestion_all = ShortCircuitOperator(
            task_id='sc_external_task_sensor_ingestion_all',
            python_callable=TimeRangeExternalTaskSensor.poke,
            op_kwargs={
                "external_task_id": 'finally',
                "external_dag_id": dag_prefix.upper() + '-INGESTION-ALL',
                "allowed_states": allowed_states
            },
            dag=dag)
        sc_external_task_sensor_ingestion_oldfw = ShortCircuitOperator(
            task_id='sc_external_task_sensor_ingestion_oldfw',
            python_callable=TimeRangeExternalTaskSensor.poke,
            op_kwargs={
                "external_task_id": 'finally',
                "external_dag_id": dag_prefix.upper() + '-INGESTION-OLD-FRAMEWORK',
                "allowed_states": allowed_states
            },
            dag=dag)
        sc_external_task_sensor_gdq_vantageone = ShortCircuitOperator(
            task_id='sc_external_task_sensor_gdq_daily_vantageone',
            python_callable=TimeRangeExternalTaskSensor.poke,
            op_kwargs={
                "external_task_id": 'finally',
                "external_dag_id": dag_prefix.upper() + '-VANTAGEONE-GDQ-DAILY',
                "allowed_states": allowed_states
            },
            dag=dag)
        for source in source_list:
            freq = 'DAILY'
            external_task_id_main = external_task_id
            local_dag_suffix = dag_suffix
            if source.lower() == 'bancs':
                external_task_id_main = 'end'
            if source.lower() in ['alm']:
                freq = 'QUARTERLY'
                local_dag_suffix = local_dag_suffix.replace('DAILY', 'QUARTERLY')
            sc_external_task_sensor_main = ShortCircuitOperator(
                task_id='sc_external_task_sensor_main_' + source,
                python_callable=TimeRangeExternalTaskSensor.poke,
                op_kwargs={
                    "external_task_id": external_task_id_main,
                    "external_dag_id": dag_prefix.upper() + '-' + source.upper() + '-MAIN-' + freq,
                    "allowed_states": allowed_states
                },
                dag=dag)
            sc_external_task_sensor_gdq = ShortCircuitOperator(
                task_id='sc_external_task_sensor_gdq_' + freq.lower() + '_' + source,
                python_callable=TimeRangeExternalTaskSensor.poke,
                op_kwargs={
                    "external_task_id": 'finally',
                    "external_dag_id": dag_prefix.upper() + '-' + source.upper().replace('VANTAGE', '') + '-GDQ-' + freq,
                    "allowed_states": allowed_states
                },
                dag=dag)
            if source.lower() not in ['bancs', 'alm', 'vantageone']:
                sc_external_task_sensor_gdq_monthly = ShortCircuitOperator(
                    task_id='sc_external_task_sensor_gdq_monthly_' + source,
                    python_callable=TimeRangeExternalTaskSensor.poke,
                    op_kwargs={
                        "external_task_id": 'finally',
                        "external_dag_id": dag_prefix.upper() + '-' + source.upper().replace('VANTAGE', '') + '-GDQ-MONTHLY',
                        "allowed_states": allowed_states
                    },
                    dag=dag)
            if source.lower() == 'bancs':
                sc_external_task_sensor_ingestion = ShortCircuitOperator(
                    task_id='sc_external_task_sensor_ingestion_' + source,
                    python_callable=TimeRangeExternalTaskSensor.poke,
                    op_kwargs={
                        "external_task_id": 'finally',
                        "external_dag_id": dag_prefix.upper() + '-' + source.upper() + '-INGESTION-DAILY',
                        "allowed_states": allowed_states
                    },
                    dag=dag)
                sc_external_task_sensor_kc2_monthly = ShortCircuitOperator(
                    task_id='sc_external_task_sensor_kc2_monthly_' + source,
                    python_callable=TimeRangeExternalTaskSensor.poke,
                    op_kwargs={
                        "external_task_id": 'finally',
                        "external_dag_id": dag_prefix.upper() + '-' + source.upper() + '-KC2-MONTHLY',
                        "allowed_states": allowed_states
                    },
                    dag=dag)
                sc_external_task_sensor_kc2_quarterly = ShortCircuitOperator(
                    task_id='sc_external_task_sensor_kc2_quarterly_' + source,
                    python_callable=TimeRangeExternalTaskSensor.poke,
                    op_kwargs={
                        "external_task_id": 'finally',
                        "external_dag_id": dag_prefix.upper() + '-' + source.upper() + '-KC2-QUARTERLY',
                        "allowed_states": allowed_states
                    },
                    dag=dag)
            elif source.lower() == 'vantage65':
                sc_external_task_sensor_kc2_quarterly = ShortCircuitOperator(
                    task_id='sc_external_task_sensor_kc2_quarterly_' + source,
                    python_callable=TimeRangeExternalTaskSensor.poke,
                    op_kwargs={
                        "external_task_id": 'finally',
                        "external_dag_id": dag_prefix.upper() + '-' + source.upper() + '-KC2-QUARTERLY',
                        "allowed_states": allowed_states
                    },
                    dag=dag)
        for dom in ['policyprint', 'confirms', 'letters', 'statement', 'statementcert']:
            if dom in ['policyprint', 'confirms', 'letters', 'statement']:
                freq = 'DAILY'
                sc_external_task_sensor_main = ShortCircuitOperator(
                    task_id='sc_external_task_sensor_main_' + dom + '_' + source + '_' + freq,
                    python_callable=TimeRangeExternalTaskSensor.poke,
                    op_kwargs={
                        "external_task_id": external_task_id_main,
                        "external_dag_id": dag_prefix.replace("FINANCEDW", "ANNUITIES-FINANCEDW") + '-' + source.upper() + dom.upper() + '-' + freq,
                        "allowed_states": allowed_states
                    },
                    dag=dag)
            if dom in ['statement', 'statementcert']:
                sc_external_task_sensor_statement_quarterly = ShortCircuitOperator(
                    task_id='sc_external_task_sensor_main_' + dom + '_' + source + '_QUARTERLY',
                    python_callable=TimeRangeExternalTaskSensor.poke,
                    op_kwargs={
                        "external_task_id": external_task_id_main,
                        "external_dag_id": dag_prefix.replace("FINANCEDW", "ANNUITIES-FINANCEDW") + '-' + source.upper() + dom.upper() + '-QUARTERLY',
                        "allowed_states": allowed_states
                    },
                    dag=dag)
    predecessor_task >> task_sensor_tg >> successor_task

# Notify When Curated Loads are Skipped
def notify_skipped_load(**kwargs):
    domain_name = kwargs['domain_name']
    environment = kwargs['environment']
    source_system_name = kwargs['source_system_name']
    batch_frequency = kwargs['batch_frequency']
    cycle_date = kwargs['cycle_date']
    cycle_date = datetime.strptime(cycle_date, "%Y%m%d").strftime('%Y-%m-%d')
    title = f"{environment.upper()} Info: Curated Load Skipped for {source_system_name}"
    email_body = """
    <html>
    <head></head>
    <body>
    Hi, <br>
    The Curated Load was SKIPPED for Order Date {1}, as there are no new loads to process from IDL. <br>
    <br>
    Thanks, <br>
    Finance Datawarehouse Team <br>
    </body>
    </html>""".format(source_system_name, cycle_date)
    custom_send_email(domain_name, environment, 'N/A', batch_frequency, title, email_body, 'job_status', 'all', **kwargs)

# Execute Query in Athena
def execute_athena_query(query, **kwargs):
    try:
        env = kwargs['task_instance'].xcom_pull(task_ids='start', key='env')
        response = athena_client.start_query_execution(
            QueryString=query,
            WorkGroup=f'ta-individual-finwd-{env}-emr',
            ResultConfiguration={
                'OutputLocation': f's3://ta-individual-finwd-{env}-logs/athena/'
            })
        query_execution_id = response['QueryExecutionId']
        print(f"Query - {query}")
        print(f"Athena Query Execution ID - {query_execution_id}")
        while True:
            result = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = result['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(60)
        if status == 'SUCCEEDED':
            return athena_client.get_query_results(QueryExecutionId=query_execution_id)
        else:
            error_reason = result['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
            raise Exception(f"Athena query failed: {status} - {error_reason}")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        raise Exception(f"Athena Client error [{error_code}]: {error_message}")
    except Exception as e:
        raise Exception(f"Athena execution error: {str(e)}")

# Checks For New ARR Batches and determines if the DAG need to be skipped or not
def check_new_arr_batches(**kwargs):
    source_system_name = kwargs['source_system_name']
    batch_frequency = kwargs['batch_frequency']
    arr_domain = kwargs['arr_domain']
    domain_list = list(set(kwargs['domain_list']))
    number_of_domains = len(domain_list)
    domains = "','".join(domain_list)
    env = kwargs['task_instance'].xcom_pull(task_ids='start', key='env')
    arr_models_list = ['laul', 'con_td']
    arr_models = "','".join(arr_models_list)
    arr_query = f"""SELECT COALESCE(MAX(cast(abs(a.dbt_post_approval_id) / 1000 as bigint)), 0) AS batch_id
    FROM ta_individual_arr_{env}.curated.dbt_post_approval_log a
    JOIN ta_individual_arr_{env}.curated.approval_information b
    ON a.approval_information_id = b.approval_information_id
    WHERE b.table_name in ('{arr_domain}')
    AND b.dbt_completion_flag = 'Y'
    AND b.model_name in ('{arr_models}')
    """
    result_response = execute_athena_query(arr_query, **kwargs)
    if result_response and 'ResultSet' in result_response and 'Rows' in result_response['ResultSet'] and len(result_response['ResultSet']['Rows']) > 1:
        arr_batchid = int(result_response['ResultSet']['Rows'][1]['Data'][0].get('VarCharValue', 0))
    else:
        arr_batchid = 0
    print(f"arr_batchid: {arr_batchid}")
    redshift_host, redshift_username, redshift_password, redshift_port, redshift_dbname = fetch_creds(False, **kwargs)
    redshift_query = f"""select nvl(max(batchid),0) 
    from financedwcurated.completedbatch 
    where lower(source_system) = lower('{source_system_name}') 
    and lower(batch_frequency) = lower('{batch_frequency}') 
    and domain_name in ('{domains}') 
    having count(distinct domain_name) = {number_of_domains}"""
    conn = psycopg.connect(dbname=redshift_dbname, host=redshift_host, port=redshift_port, user=redshift_username, password=redshift_password)
    cur = conn.cursor()
    cur.execute(redshift_query)
    redshift_data = cur.fetchall()
    try:
        redshift_batchid = redshift_data[0][0]
    except (IndexError, TypeError):
        redshift_batchid = 0
    print(f"redshift_batchid: {redshift_batchid}")
    conn.close()
    if arr_batchid > redshift_batchid:
        return "create_curated_cluster"
    else:
        return "notify_skipped_load"

# Checks For New IDL Batches and determines if the DAG need to be skipped or not
def check_new_idl_batches(**kwargs):
    source_system_name = kwargs['source_system_name']
    batch_frequency = kwargs['batch_frequency']
    idl_source_system_name = kwargs.get('idl_source_system_name') or kwargs.get('idl_source_system_name')
    if kwargs.get('idl_source_system_name'):
        idl_source_system_name = kwargs['idl_source_system_name']
    domain_list = list(set(kwargs['domain_list']))
    number_of_domains = len(domain_list)
    domains = "','".join(domain_list)
    redshift_host, redshift_username, redshift_password, redshift_port, redshift_dbname, \
    postgres_host, postgres_dbname, postgres_username, postgres_password, postgres_port = fetch_creds(True, **kwargs)
    idl_query = f"""select coalesce(max(batchid),0) from ingestion.batch_tracking 
    where lower(source_system_name) = lower('{idl_source_system_name}') and lower(status) = 'success' 
    and lower(runtype) = 'ingestionpipeline' and lower(batch_type) = lower('{batch_frequency}')"""
    redshift_query = f"""select nvl(max(batchid),0) from financedwcurated.completedbatch 
    where lower(source_system) = lower('{source_system_name}') 
    and lower(batch_frequency) = lower('{batch_frequency}') 
    and domain_name in ('{domains}') 
    having count(distinct domain_name) = {number_of_domains}"""
    conn = psycopg.connect(dbname=postgres_dbname, host=postgres_host, port=postgres_port, user=postgres_username, password=postgres_password)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(idl_query)
    idl_data = cur.fetchall()
    idl_batchid = idl_data[0][0] if idl_data else 0
    print("idl_batchid: ", idl_batchid)
    conn = psycopg.connect(dbname=redshift_dbname, host=redshift_host, port=redshift_port, user=redshift_username, password=redshift_password)
    cur = conn.cursor()
    cur.execute(redshift_query)
    redshift_data = cur.fetchall()
    try:
        redshift_batchid = redshift_data[0][0]
    except (IndexError, TypeError):
        redshift_batchid = 0
    print("redshift_batchid: ", redshift_batchid)
    conn.close()
    if idl_batchid > redshift_batchid:
        return "create_curated_cluster"
    else:
        return "notify_skipped_load"

# execute query and determines if the DAG need to be skipped or not
def check_finwd_data(**kwargs):
    query = kwargs['query']
    next_tasks = kwargs['next_tasks']
    data = python_callable_for_redshift_actions(sql=query, action='exec_custom_sql', return_locally=True, **kwargs)
    if data:
        return next_tasks[0]
    else:
        return next_tasks[1]

# ssm command to run scripts on ec2
def ssm_send_command(**kwargs):
    command = kwargs['command']
    ec2_name = kwargs['task_instance'].xcom_pull(task_ids='start', key='ec2_instance_name')
    ssm_params = kwargs['task_instance'].xcom_pull(task_ids='start', key='ssm_params')
    s3_bucket = ssm_params['s3_logs_bucket']
    s3_prefix = ssm_params['s3_logs_prefix'] + "_" + datetime.now().strftime('%Y-%m-%d')
    response = ec2_client.describe_instances(Filters=[
        {'Name': 'tag-key', 'Values': ['Name']},
        {'Name': 'tag-value', 'Values': [ec2_name]},
        {'Name': 'instance-state-name', 'Values': ['running']}
    ])
    instance_id = response['Reservations'][0]['Instances'][0]['InstanceId']
    print("**************The instance_id is: ", instance_id, "**************")
    print("**************The command is: ", command, "**************")
    response = ssm_client.send_command(
        InstanceIds=[instance_id],
        DocumentName='AWS-RunShellScript',
        Parameters={'commands': [command]},
        OutputS3BucketName=s3_bucket,
        OutputS3KeyPrefix=s3_prefix
    )
    time.sleep(30)
    # fetching command id for the output
    command_id = response['Command']['CommandId']
    print("**************The response is: ", response, "**************")
    print("**************Command id is: ", command_id, "**************")
    count = 0
    while True:
        count += 1
        # fetching command output
        output = ssm_client.get_command_invocation(
            CommandId=command_id,
            InstanceId=instance_id
        )
        if output['Status'] == 'Success':
            print("************Status of ssm send command is: ", output['Status'], "*************")
            print("************Output of ssm command: ", output, "*************")
            return True, output
        elif output['Status'] in ('Pending', 'Delayed') and count >= 60:
            print("************Error25: Status of ssm send command is: ", output['Status'], "*************")
            print("************Command not started in one hour***************")
            print("************Output of ssm command: ", output, "*************")
            exit(25)
        elif output['Status'] in ('Cancelled', 'TimedOut', 'Failed', 'Cancelling'):
            print("************Error35: Status of ssm send command is: ", output['Status'], "*************")
            print("************Output of ssm command: ", output, "*************")
            exit(35)
        time.sleep(60)

# ssm command to run scripts on ec2 to run on EMR
def ssm_send_command_emr(**kwargs):
    ec2_user = kwargs['task_instance'].xcom_pull(task_ids='start', key='ec2_user')
    env = kwargs['task_instance'].xcom_pull(task_ids='start', key='env')
    project = kwargs['task_instance'].xcom_pull(task_ids='start', key='project')
    command = kwargs['command']
    emr_name = kwargs['emr_name']
    try:
        source_system_name = kwargs['source_system_name']
    except Exception:
        source_system_name = None
    if emr_name in ['curated'] and source_system_name is None:
        emr_name = f"ondemand-emr-all-{emr_name}-{project}"
    elif emr_name in ['curated'] and source_system_name is not None:
        emr_name = f"ondemand-emr-{source_system_name}-{emr_name}-{project}"
    else:
        emr_name = f"ondemand-emr-all-{emr_name}-{project}"
    ssm_command = f"sudo -H -u {ec2_user} sh /application/financedw/scripts/trigger_ssm_commands.sh -e {env} -s {emr_name} -c '{command}'"
    kwargs['command'] = ssm_command
    return ssm_send_command(**kwargs)

# Call Create or terminate EMRs
def emr_actions(**kwargs):
    action = kwargs['action']
    emr_name = kwargs['emr_name']
    try:
        source_system_name = kwargs['source_system_name']
    except Exception:
        source_system_name = None
    project = kwargs['task_instance'].xcom_pull(task_ids='start', key='project')
    env = kwargs['task_instance'].xcom_pull(task_ids='start', key='env')
    ec2_user = kwargs['task_instance'].xcom_pull(task_ids='start', key='ec2_user')
    odate = kwargs['task_instance'].xcom_pull(task_ids='start', key='odate')
    if action == 'create':
        if emr_name == 'curated' and source_system_name is not None:
            command = f"sudo -H -u {ec2_user} python3 /application/financedw/scripts/emr/main.py -e {env} -a {action} -p {project} -s {emr_name} -ss {source_system_name} -d {odate}"
        else:
            command = f"sudo -H -u {ec2_user} python3 /application/financedw/scripts/emr/main.py -e {env} -a {action} -p {project} -s {emr_name} -d {odate}"
        kwargs['command'] = command
        status = create_cluster(**kwargs)
    elif action == 'terminate':
        if emr_name == 'curated' and source_system_name is not None:
            command = f"sudo -H -u {ec2_user} python3 /application/financedw/scripts/emr/main.py -e {env} -a {action} -p {project} -s {emr_name} -ss {source_system_name}"
        else:
            command = f"sudo -H -u {ec2_user} python3 /application/financedw/scripts/emr/main.py -e {env} -a {action} -p {project} -s {emr_name}"
        kwargs['command'] = command
        status = terminate_cluster(**kwargs)
    else:
        status = None
    return status

def emr_step_submit(**kwargs):
    env = kwargs['task_instance'].xcom_pull(task_ids='start', key='env')
    project = kwargs['task_instance'].xcom_pull(task_ids='start', key='project')
    command = kwargs['command']
    emr_name = kwargs['emr_name']
    emr_version = kwargs.get('emr_version')
    script_args = ''
    source_system = (kwargs.get('source_system_name') or 'all').lower()
    emr_full_name = f"ta-individual-findw-{env}-ondemand-emr-{source_system}-{emr_name}-{project}-"
    cluster_id = check_emr_cluster(None, emr_full_name, 'create')
    if not cluster_id:
        args = {'emr_name': emr_name, 'source_system_name': source_system, 'emr_version': emr_version, 'task_instance': kwargs['task_instance']}
        emr_actions(**args)
        cluster_id = check_emr_cluster(None, emr_full_name, 'create')
    try:
        step_name = kwargs['step_name']
    except Exception:
        step_name = command.replace('sh /application/financedw/', '')
    try:
        script_uri, script_args = command.split(".sh", 1)
        script_uri = f"{script_uri}.sh"
        if script_uri.startswith('sh '):
            script_uri = script_uri.replace('sh ', '')
    except Exception:
        raise AirflowException("Script Can only be Shell Scripts")
    args = {'script_type': 'bash', 'action_on_failure': 'CONTINUE', 'emr_cluster_id': cluster_id, 'step_name': step_name,
            'script_uri': script_uri, 'script_args': script_args, 'task_instance': kwargs['task_instance']}
    step_submit(**args)

# create cluster using ssm command
def create_cluster(**kwargs):
    status, output = ssm_send_command(**kwargs)
    try:
        cluster_id = output['StandardOutputContent'].split('Created cluster. ')[1].split('\n')[0]
        print("***********The EMR cluster is created***********")
    except Exception:
        try:
            cluster_id = output['StandardOutputContent'].split('EMR already exists for given configuration: ')[1].split('\n')[0]
            print("***********The EMR cluster exists already***********")
        except Exception:
            cluster_id = None
            print("***********Could not determine cluster id from ssm output***********")
    print(f"***********The cluster id is: {cluster_id}***********")
    if kwargs.get('task_instance') and cluster_id:
        kwargs['task_instance'].xcom_push(key='cluster_id', value=cluster_id)
    return status

# terminate cluster using ssm command
def terminate_cluster(**kwargs):
    status, output = ssm_send_command(**kwargs)
    print(f"***********The cluster is terminated: {status}***********")
    return status

# fetch params based on environment passed for findw dags
def get_finwd_var(ds, **kwargs):
    try:
        env_var = kwargs['dag_run'].conf['env']
    except Exception:
        env_var = kwargs.get('env')
    kwargs['dag_run'].conf['env'] = env_var
    try:
        project = kwargs['dag_run'].conf['project']
    except Exception:
        project = kwargs.get('project')
    kwargs['dag_run'].conf['project'] = project
    try:
        emr_name = kwargs['dag_run'].conf['emr_name']
        if emr_name == 'gdq':
            emr = 'BDE'
        else:
            emr = emr_name
        kwargs['task_instance'].xcom_push(key='emr', value=emr)
    except Exception:
        pass
    file_path = kwargs['config_file_path']
    config.read(file_path)
    project_override = ''
    if project != 'financedw':
        project_override = '-' + project
    env_dict = {'dev': 'dev', 'tst': 'test', 'mdl': 'model', 'prd': 'prod'}
    ec2_user = f"sptactfin" + env_dict.get(env_var, env_var)
    config['env'] = {'env_name': env_var, 'prefix': 'ta-individual-findw-' + env_var, 'project': project, 'project_override': project_override}
    # iterate through all config passed in airflow cli and push them to xcom
    for k, v in kwargs['dag_run'].conf.items():
        kwargs['task_instance'].xcom_push(key=k, value=v)
    kwargs['task_instance'].xcom_push(key='project_override', value=project_override)
    kwargs['task_instance'].xcom_push(key='ssm_params',
                                     value=eval(config.get('ssm_params', 'ssm_params')))
    kwargs['task_instance'].xcom_push(key='redshift_params',
                                     value=eval(config.get('redshift_params', 'redshift_params')))
    kwargs['task_instance'].xcom_push(key='ec2_user', value=ec2_user)
    kwargs['task_instance'].xcom_push(key='ec2_instance_name', value=eval(config.get('ec2_params', 'instance_name')))

# create python operator
def create_python_operator(dag, task_name, op_kwargs, python_callable, trigger_rule, retries=1):
    task = PythonOperator(
        task_id=task_name,
        python_callable=python_callable,
        op_kwargs=op_kwargs,
        trigger_rule=trigger_rule,
        retries=retries,
        dag=dag
    )
    return task

# Function to fetch Redshift credentials from secret manager
def get_secret(secret_name, region):
    print("************secret string name: ", secret_name, "************")
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region
    )
    # Fetch secret value
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except Exception as ex:
        print("************The following exception occurred when retrieving credentials:************\n", ex)
        raise ex
    else:
        if 'SecretString' in get_secret_value_response:
            secret = json.loads(get_secret_value_response['SecretString'])
        else:
            secret = base64.b64decode(get_secret_value_response['SecretBinary'])
        return secret

def create_python_func(hop_name, load_type, task_name, dag, sp_dict, domain=None, source_system=None, batch_frequency=None, trigger_rule='all_success', op_kwargs=None):
    if not op_kwargs:
        op_kwargs = {}
    if domain:
        op_kwargs["domain"] = domain
    if source_system:
        op_kwargs["source_system"] = source_system
    if batch_frequency:
        op_kwargs["batch_frequency"] = batch_frequency
    op_kwargs["sql"] = "call " + sp_dict[load_type][hop_name]
    op_kwargs["action"] = "exec_sql"
    task = create_python_operator_retries(
        dag=dag,
        task_name=task_name,
        op_kwargs=op_kwargs,
        python_callable=python_callable_for_redshift_actions,
        trigger_rule=trigger_rule,
    )
    return task

# create python operator with retries
def create_python_operator_retries(dag, task_name, op_kwargs, python_callable, trigger_rule, retries=None,
                                   retry_delay=timedelta(seconds=30), retry_exponential_backoff=True, max_retry_delay=timedelta(seconds=120)):
    if not retries:
        if python_callable == python_callable_for_redshift_actions:
            retries = int(Variable.get("redshift_retries", default_var=2))
        else:
            retries = int(Variable.get("other_retries", default_var=2))
    task = PythonOperator(
        task_id=task_name,
        python_callable=python_callable,
        op_kwargs=op_kwargs,
        trigger_rule=trigger_rule,
        retries=retries,
        retry_delay=retry_delay,
        retry_exponential_backoff=retry_exponential_backoff,
        max_retry_delay=max_retry_delay,
        dag=dag
    )
    return task

# Fetch Redshift Credentials
def fetch_creds(fetch_idl=False, **kwargs):
    redshift_params = kwargs['task_instance'].xcom_pull(task_ids='start', key='redshift_params')
    project = kwargs['task_instance'].xcom_pull(task_ids='start', key='project')
    env = kwargs['task_instance'].xcom_pull(task_ids='start', key='env')
    secret_name = redshift_params['secret']
    region = redshift_params['region']
    secret = get_secret(secret_name, region)
    host = secret['redshift']['data_warehouse']['master_svc_account']['hostname']
    dbname = project.replace('financedw', 'findw')
    username = secret['redshift']['data_warehouse']['master_svc_account']['username']
    password = secret['redshift']['data_warehouse']['master_svc_account']['password']
    port = secret['redshift']['data_warehouse']['master_svc_account']['port']
    if env == 'prd':
        try:
            dml_user = kwargs.get('dml_user')
            username = secret['redshift']['data_warehouse']['master_svc_account_dml']['username']
            password = secret['redshift']['data_warehouse']['master_svc_account_dml']['password']
        except Exception:
            pass
    if fetch_idl:
        postgres_host = secret['rds']['database']['master_svc_account']['hostname']
        postgres_dbname = secret['rds']['database']['master_svc_account']['dbname']
        postgres_username = secret['rds']['database']['master_svc_account']['username']
        postgres_password = secret['rds']['database']['master_svc_account']['password']
        postgres_port = secret['rds']['database']['master_svc_account']['port']
        return host, username, password, port, dbname, postgres_host, postgres_dbname, \
               postgres_username, postgres_password, postgres_port
    else:
        return host, username, password, port, dbname

def python_callable_for_redshift_actions(**kwargs):
    try:
        # Call get_secret function and assign required credentials to variables
        host, username, password, port, dbname = fetch_creds(False, **kwargs)
        # Create connection to redshift
        conn = psycopg.connect(dbname=dbname, host=host, port=port, user=username, password=password)
        conn.autocommit = True
        cur = conn.cursor()
        print(f"************User {username} established Connection to Redshift database {dbname}************")
        try:
            return_locally = kwargs['return_locally']
        except Exception:
            return_locally = False
        try:
            return_locally_with_cols = kwargs['return_locally_with_cols']
        except Exception:
            return_locally_with_cols = False
        action = kwargs['action']
        return_val = {}
        if action in ['exec_sql', 'exec_custom_sql']:
            sql = kwargs['sql']
            if action == 'exec_sql':
                # Attempt to substitute placeholders from kwargs or xcom
                try:
                    source_system = kwargs['source_system']
                except Exception:
                    try:
                        source_system = kwargs['task_instance'].xcom_pull(task_ids='start', key='source_system')
                    except Exception:
                        source_system = None
                try:
                    if source_system:
                        sql = sql.replace('{source_system}', source_system)
                except Exception:
                    pass
                try:
                    domain = kwargs['domain']
                except Exception:
                    try:
                        domain = kwargs['task_instance'].xcom_pull(task_ids='start', key='domain')
                    except Exception:
                        domain = None
                try:
                    if domain:
                        sql = sql.replace('{domain}', domain)
                except Exception:
                    pass
                try:
                    batch_frequency = kwargs['batch_frequency']
                except Exception:
                    try:
                        batch_frequency = kwargs['task_instance'].xcom_pull(task_ids='start', key='batch_frequency')
                    except Exception:
                        batch_frequency = None
                try:
                    if batch_frequency:
                        sql = sql.replace('{batch_frequency}', batch_frequency)
                except Exception:
                    pass
                try:
                    hop_name = kwargs['hop_name']
                except Exception:
                    try:
                        hop_name = kwargs['task_instance'].xcom_pull(task_ids='start', key='hop_name')
                    except Exception:
                        hop_name = None
                try:
                    if hop_name:
                        sql = sql.replace('{hop_name}', hop_name)
                except Exception:
                    pass
            # Call stored procedure / execute SQL
            try:
                print("************Calling stored procedure:", sql, " **************")
                cur.execute("%s" % sql)
                print("************ " + sql + " executed successfully**************")
                try:
                    if return_locally is True:
                        return cur.fetchall()
                    elif return_locally_with_cols is True:
                        return cur.fetchall(), cur.description
                    else:
                        return_val['result'] = cur.fetchall()
                        kwargs['task_instance'].xcom_push(key="return_val", value=return_val)
                except Exception as e:
                    print("************The following exception occurred when fetching the results: *************\n", e)
            except Exception as e:
                print("************The following exception occurred when calling a stored procedure: *************\n", e)
                raise e
        else:
            # other actions not implemented here
            pass
    except Exception as e:
        print("************The following exception occurred *************\n", e)
        raise e
    finally:
        try:
            print("************Closing the connection************")
            conn.close()
        except Exception:
            pass

def check_value(val):
    sort_order = ["failed", "upstream_failed", "shutdown", "skipped", "success"]
    if val in sort_order:
        return sort_order.index(val)
    else:
        return int(5)

def custom_send_email(dom, envron, ss_name, btfreq, title, email_body, notify_category, notify_type='all', attachment_list=None, **kwargs):
    if dom != 'CustomDML':
        if kwargs.get('application') and kwargs['application'] != 'finance':
            dom = kwargs['application']
        else:
            dom = 'FINMOD'
    query = f"select top 1 to_email_list, cc_email_list from financedworchestration.notification where \
    domain_name = '{dom}' and source_system_name = '{ss_name}' and batch_frequency = '{btfreq}' \
    and environment_code = '{envron}' and notification_category = '{notify_category}' and notification_type = '{notify_type}' "
    email_list = python_callable_for_redshift_actions(sql=query, action='exec_custom_sql', return_locally=True, **kwargs)
    alert_email_list = None
    alert_email_list_cc = None
    if bool(email_list):
        if email_list[0][0] is not None:
            alert_email_list = email_list[0][0]
        if email_list[0][1] is not None:
            alert_email_list_cc = email_list[0][1]
    print(f"Email List for Notification Type {notify_type} is {alert_email_list} {alert_email_list_cc}")
    if alert_email_list is not None:
        send_email_smtp(alert_email_list, title, email_body, files=attachment_list, cc=alert_email_list_cc)
    else:
        print("Skipped E-Mail")

def notify_email(**kwargs):
    session = settings.Session()
    domain_name = kwargs['domain_name']
    environment = kwargs['environment']
    source_system_name = kwargs['source_system_name']
    batch_frequency = kwargs['batch_frequency']
    try:
        cycle_date = kwargs['odate']
        cycle_date = datetime.strptime(cycle_date, '%Y%m%d').strftime('%Y-%m-%d')
    except Exception:
        cst = timezone('US/Central')
        cycle_date = datetime.now(cst).strftime('%Y-%m-%d')
    try:
        application = kwargs['application']
    except Exception:
        application = 'finance'
    kwargs['application'] = application
    try:
        job_name = kwargs['job_name']
    except Exception:
        job_name = 'NOT DEFINED'
    input_params = {
        'source_system_name': source_system_name,
        'domain_name': domain_name,
        'batch_frequency': batch_frequency,
        'cycle_date': cycle_date,
        'environment': environment,
        'control_m_job': job_name
    }
    affected_apps = 'Finance DataWarehouse - Finance'
    poc = 'tatechdataengineering-dwdev@transamerica.com;tatechdataengineering-dwops@transamerica.com'
    current_dag = kwargs['dag_run'].dag_id
    dag_key = current_dag.split('_')[-1]
    today = date.today()
    tdate = today.strftime("%m-%d-%Y")
    title = str(environment).upper() + " Airflow Failure alert: " + current_dag + " -> Domain: " + domain_name + ", SourceSystem: " + source_system_name + \
            ", CycleDate: " + cycle_date + ", ControlMJob: " + job_name
    email_task_row = """
        <tr>
            <td style="border: 1px solid white; padding: 5px; font-family: Arial, sans-serif; font-size: 16px; line-height: 20px; width: 10.3001%; background-color: #E0E2E5; vertical-align: middle; text-align: left;">
                <span style="font-family: Verdana,Geneva,sans-serif;"><span style="font-size: 14px;">{TASK_NAME}</span></span></td>
            <td style="border: 1px solid white; padding: 5px; font-family: Arial, sans-serif; font-size: 16px; line-height: 20px; width: 9.4843%; background-color: {TASK_STATUS_COLOR}; vertical-align: middle; text-align: left;">
                <span style="font-family: Verdana,Geneva,sans-serif;"><span style="font-size: 14px;">{TASK_STATUS}</span></span></td>
            <td style="border: 1px solid white; width: 13.4615%; background-color: #E0E2E5; vertical-align: middle; text-align: left;"><span style="font-family: Verdana,Geneva,sans-serif;"><span style="font-size: 14px;"><span style="color: #000000;">&nbsp;{TASK_SEV}</span></span></span></td>
            <td style="border: 1px solid white; width: 13.4615%; background-color: #E0E2E5; vertical-align: middle; text-align: center;">
                <span style="font-family: Verdana,Geneva,sans-serif;"><span style="color: #54acd2;">&nbsp; &nbsp; &nbsp;</span></span></td>
            <td style="border: 1px solid white; width: 13.7675%; background-color: #E0E2E5; vertical-align: middle; text-align: left;"><span style="font-family: Verdana,Geneva,sans-serif;"><span style="font-size: 14px;">{TASK_START_TIME}</span></span></td>
            <td style="border: 1px solid white; width: 13.4615%; background-color: #E0E2E5; vertical-align: middle; text-align: left;">
                <span style="font-family: Verdana,Geneva,sans-serif;"><span style="font-size: 14px;">{TASK_END_TIME}</span></span></td>
            <td style="border: 1px solid white; width: 13.8695%; background-color: {TASK_SLA_COLOR}; vertical-align: middle; text-align: left;">
                <span style="font-family: Verdana,Geneva,sans-serif;"><span style="font-size: 14px;">{TASK_EXE_TIME}</span></span></td>
        </tr>
    """
    email_body = """    </tr>
    </tbody>
    </table>
    <p>
    <br>
    </p>
    <p>****** Note: All DateTime are in UTC ******</p>
    <p>****** This is an auto generated email. Do Not Reply to this email ******</p>
    <p>&nbsp;</p>
    <p>Regards,</p>
    <p>Airflow Admin .</p>
    <p>
    <br>
    </p>
    """
    entries_initial = session.query(TaskInstance).filter(TaskInstance.dag_id == current_dag,
                                                        TaskInstance.execution_date == kwargs['dag_run'].execution_date).all()
    entries = sorted(entries_initial, key=lambda val: check_value(val.state))
    task_alert_ind = False
    sla_alert_ind = False
    alert_ind = False
    dag_url = ''
    email_task_rows = ''
    for tsk in entries:
        task_name = str(tsk.task_id)
        if task_name != 'TASK_Send_Email':
            task_status = str(tsk.state)
            task_start_time = str(tsk.start_date)
            task_end_time = str(tsk.end_date)
            if tsk.duration is not None:
                task_duration = str(round(float(tsk.duration) / 60, 2))
            else:
                task_duration = '-1'
            task_url = str(tsk.log_url)
            dag_url = task_url
            if task_status == 'success':
                task_status_color = '#41a85f'
            elif task_status in ('failed', 'upstream_failed', 'shutdown'):
                task_status_color = '#b8312f'
                task_alert_ind = True
            else:
                task_status_color = '#ff9900'
            sla_alert_ind = True
            task_sla_color = '#b8312f'
            email_task_rows = email_task_rows + email_task_row.replace('{TASK_NAME}', task_name).\
                replace('{TASK_STATUS}', task_status).\
                replace('{TASK_STATUS_COLOR}', task_status_color).\
                replace('{TASK_SEV}', 'high').\
                replace('{LOG_URL}', task_url).\
                replace('{TASK_START_TIME}', task_start_time).\
                replace('{TASK_END_TIME}', task_end_time).\
                replace('{TASK_EXE_TIME}', task_duration)
    entries = session.query(DagRun).filter(DagRun.dag_id == current_dag,
                                          DagRun.execution_date == kwargs['dag_run'].execution_date).all()
    for tsk in entries:
        dag_start_time = str(tsk.start_date)
        dag_end_time = str(tsk.end_date)
        dag_status = str(tsk.state)
        dag_duration = round((datetime.utcnow() - tsk.start_date.replace(tzinfo=None)).total_seconds() / 60, 2)
        if dag_status == 'running' and task_alert_ind is False and sla_alert_ind is False:
            dag_status = 'success'
            dag_color = 'rgb(65, 168, 95)'
        else:
            dag_status = 'failed'
            alert_ind = True
            dag_color = 'rgb(184, 49, 47)'
        dag_url = dag_url[:dag_url.index(current_dag)]
        dag_url = re.sub("/log", "/graph", dag_url + current_dag).replace('{DAG_Start_Time}', dag_start_time).\
            replace('{DAG_End_Time}', dag_end_time).\
            replace('{DAG_STATUS_COLOR}', dag_color).\
            replace('{DAG_STATUS}', dag_status).\
            replace('{ENV}', environment).\
            replace('{DAG_Run_Time}', str(dag_duration)).\
            replace('{CYCLEDATE}', cycle_date).\
            replace('{Contact}', poc).\
            replace('{APPS}', affected_apps).\
            replace('{DAG_Link}', dag_url).replace('{EMAIL_TASK_ROW}', email_task_rows)
        break
    incident_msg = "Will be created by ControlM Server"
    if alert_ind is True:
        email_body = email_body.replace("{INCIDENT}", incident_msg)
    if alert_ind is False:
        title = str(environment).upper() + " Airflow Success Info: " + current_dag + " -> Domain: " + domain_name + ", SourceSystem: " + source_system_name + \
                ", CycleDate: " + cycle_date
        email_body = email_body.replace("{INCIDENT}", "NA")
    if dag_status == 'all' or domain_name == 'CustomDML':
        custom_send_email(domain_name, environment, source_system_name, batch_frequency, title, email_body, 'job_status', 'all', **kwargs)
    elif dag_status == 'failed':
        custom_send_email(domain_name, environment, source_system_name, batch_frequency, title, email_body, 'job_status', dag_status, **kwargs)
    else:
        print(f"Skipped EMail. DAG Status was {dag_status}")
    print(" Sending SNS notification")
    prepare_publish_airflow_dag_overall_status(current_dag, source_system_name, domain_name, dag_url, dag_status, environment, cycle_date,
                                               application, job_name)

def prepare_publish_airflow_dag_overall_status(airflow_dag_name, source_system, domain_name, airflow_dag_url, status, environment, cycle_date,
                                              application, job_name):
    """
    Prepares a json message which will represent the overall status of each dag run. publish this message to sns topic
    :param airflow_dag_name: The airflow dag name which is publishing this message.
    :param source_system: The source system name.
    :param airflow_dag_url: The airflow dag url for specific dag which is publishing the message.
    :param status: The status of the airflow dag run. completed or scheduled
    :param control_m_job: The Control M Job Name
    """
    try:
        acctno = env_acct_dict[environment]
        topic_arn = f"arn:aws:sns:us-east-1:{acctno}:ta-individual-findw-{environment}"
        print("~~topic_arn : ", topic_arn)
        status_json_message = {
            "application": application,
            "airflow_dag_name": airflow_dag_name,
            "domain_name": domain_name,
            "source_system": source_system,
            "airflow_dag_url": airflow_dag_url,
            "control_m_job": job_name,
            "timestamp": str(datetime.now(timezone('US/Central'))),
            "status": status
        }
        print("~~status_json_message : ", status_json_message)
        status_msg = {'application': application, 'domain_name': domain_name, 'source_system_name': source_system, 'cycle_date': cycle_date, 'dag_name': airflow_dag_name}
        status_str_message = json.dumps(status_msg)
        endpoint_message = {
            "default": status_str_message,
            "email": status_str_message,
            "lambda": json.dumps(status_json_message)
        }
        subject = environment.upper() + " FINDW Finance Airflow " + status.upper() + " " + status.replace('failed', 'Alert').replace('success', 'Info') + " - " + domain_name + " - " + source_system + " for cycle date: " + cycle_date
        # publish message
        publish_json_message_to_sns(topic_arn, subject, endpoint_message)
    except (Exception, ClientError) as e:
        print(f"Exception in prepare_publish_airflow_dag_overall_status task : {e}")

def publish_json_message_to_sns(topic_arn, subject, message):
    """
    Publishes a json format message to a SNS Topic. The message will contain the sourcesystem, cycle_date and status.
    :param topic_arn: The ARN of the sns topic to which message need to be published.
    :param subject: Optional parameter to be used as the "Subject" line when the message is delivered
                   to email endpoints/subscriptions.
    :param json_message: The message to be published to sns topic.
    """
    try:
        # publish message
        sns_client.publish(TopicArn=topic_arn,
                           MessageStructure='json',
                           Message=json.dumps(message),
                           Subject=subject)
        print(f"~~SNS msg published to : {topic_arn}")
    except ClientError as e:
        print(f"Couldn't publish message to topic {topic_arn} due to {e}")

def defineDependencies(dag, segment_dependencies, start, task_email_notify, final_status, start_task, end_task):
    ##The function takes start task, end task and segment_dependencies as input parameter.
    ##Based on dependencies defined set dependency between different workflows.
    ##Define dependencies between segments based on dependencies defined in segment_dependencies
    for workflowname in range(0, len(segment_dependencies)):
        parent_wf_name = list(segment_dependencies[workflowname][0].keys())[0]
        child_wf_name = list(segment_dependencies[workflowname][0].values())[0]
        parent_wf_taskid = dag.task_group.get_child_by_label(parent_wf_name)
        child_wf_taskid = dag.task_group.get_child_by_label(child_wf_name)
        parent_wf_taskid >> child_wf_taskid
    ##Get start task and end task and set dependencies for start and end task
    starttask = dag.task_group.get_child_by_label(start_task[0][0])
    endtask = dag.task_group.get_child_by_label(end_task[0][0])
    start >> starttask
    endtask >> task_email_notify >> final_status

def createMappings(source_system, segment_names, mapping_dependencies, config_dict, dag, emr_name):
    ##The function takes workflow names and mapping_dependencies as input parameter.
    ##Creates mapping within and set dependencies within.
    ##Initialize variables/configurations from config in python environment
    lst_taskgrp = []
    def getSegments(workflowname, mapping_dependencies):
        ##The function takes workflow name and mapping dependencies as input parameter.
        ##From mapping dependencies get the list of mapping for a given workflow.
        ##Return the mapping list for given workflow.
        lst_task = None
        tasklist = [task_dict if workflowname in task_dict else '' for task_dict in mapping_dependencies]
        for task in tasklist:
            if len(task) < 1:
                pass
            else:
                lst_task = list(task.values())[0]
        return lst_task
    def createMappingTasks(list_task_list, gid, config_dict, source_system, emr_name):
        ##The function takes list of tasks and its corresponding task group id as input parameter.
        ##Create mapping task for the all task in the list of tasks
        ##Return the task created in a list called workflow_tasks.
        workflow_tasks = []
        for task_list in list_task_list:
            lst_tasks_ll = []
            for task in task_list:
                taskname = list(task.keys())[0]
                if 'alm' in source_system or 'vantageone' in source_system:
                    if 'prelogging' in taskname:
                        tasknm = taskname.replace('prelogging_', 'wf_')
                    elif 'postlogging' in taskname:
                        tasknm = taskname.replace('postlogging_', 'wf_')
                    else:
                        tasknm = taskname
                else:
                    if 'prelogging' in taskname:
                        tasknm = taskname.replace('prelogging_', f"wf_{source_system}")
                    elif 'postlogging' in taskname:
                        tasknm = taskname.replace('postlogging_', f"wf_{source_system}")
                    else:
                        tasknm = taskname
                for i, j in config_dict.items():
                    globals()[f'i{j}'] = eval(j)
                t_command = list(task.values())[0]
                command = eval(t_command + "_command")
                params = eval(t_command + "_params") if (t_command + "_params") in globals() else {}
                taskid = gid + "_" + str(taskname)
                step_name = f"GDQ {source_system} {tasknm}"
                task = create_python_operator(dag=dag, task_name=taskid, op_kwargs={"emr_name": emr_name, "step_name": step_name, "command": command}, python_callable=emr_step_submit, trigger_rule="all_success")
                lst_tasks_ll.append(task)
            workflow_tasks.append(lst_tasks_ll)
        return workflow_tasks
    ##Iterate through all workflows create task group for each workflow and set dependencies.
    for workflowlist in segment_names:
        wf_lst = []
        for workflowname in workflowlist:
            list_task_list = getSegments(workflowname, mapping_dependencies)
            gid = str(workflowname)
            with TaskGroup(group_id=gid) as tgl:
                workflow_tasks = createMappingTasks(list_task_list, gid, config_dict, source_system, emr_name)
                for i in range(1, len(workflow_tasks)):
                    for j in range(0, len(workflow_tasks[i])):
                        workflow_tasks[i-1] >> workflow_tasks[i][j]
            wf_lst.append(tgl)
        lst_taskgrp.append(wf_lst)
    return lst_taskgrp

def getTaskDependencies(segment_names, task_details):
    mapping_dependencies = []
    for i in segment_names:
        listoflist = []
        curr_task_order = 1
        prev_task_order = 1
        tasklist = []
        for j in task_details:
            if i != j[3]:
                continue
            curr_task_order = j[9]
            task_name = j[6]
            task_type = j[7]
            if curr_task_order != prev_task_order:
                if len(tasklist) > 0:
                    listoflist.append(tasklist)
                tasklist = []
            tasklist.append({task_name: task_type})
            prev_task_order = curr_task_order
        listoflist.append(tasklist)
        mapping_dependencies.append({i: listoflist})
    return mapping_dependencies

def getDagCreateConfig(df):
    t_segment_names = df[(~df['parent_segment_name'].isnull()) & (df['segment_order'] != -1)][['segment_name', 'segment_order']].drop_duplicates() \
                        .groupby('segment_order')['segment_name'].apply("','".join).reset_index().values.tolist()
    [i.pop(0) for i in t_segment_names]
    segment_names = []
    [segment_names.append(i[0].split("','")) for i in t_segment_names]
    ## segment dependencies
    segment_dependencies = []
    t_segment_dependencies = df[['segment_name', 'parent_segment_name']].drop_duplicates().groupby(['segment_name', 'parent_segment_name']) \
                            .apply(','.join).reset_index()[['segment_name', 'parent_segment_name']].to_dict('records')
    [segment_dependencies.append({i['parent_segment_name']: i['segment_name']}) for i in t_segment_dependencies]
    ## mapping dependencies
    m_segment_names = list(set(df[(~(df['parent_segment_name'].isnull())) & (df['segment_order'] != -1)]['segment_name'].values.tolist()))
    m_task_details = df[(~(df['parent_segment_name'].isnull())) & (df['segment_order'] != -1)].values.tolist()
    st_segment_names = list(set(df[((df['parent_segment_name'].isnull()) & ((df['segment_order'] == 1)))]['segment_name'].values.tolist()))
    st_task_details = df[(df['parent_segment_name'].isnull()) & ((df['segment_order'] == 1))].values.tolist()
    et_segment_names = list(set(df[((df['parent_segment_name'].isnull()) & ((df['segment_order'] == -1)))]['segment_name'].values.tolist()))
    et_task_details = df[(df['parent_segment_name'].isnull()) & ((df['segment_order'] == -1))].values.tolist()
    temp_mapping_dependencies = getTaskDependencies(m_segment_names, m_task_details)
    mapping_dependencies = []
    for i in temp_mapping_dependencies:
        for k, v in i.items():
            temp_dict = {}
            lol = []
            for z in v:
                ll = [dict(t) for t in (tuple(d.items()) for d in z)]
                lol.append(ll)
            temp_dict[k] = lol
            mapping_dependencies.append(temp_dict)
    ## start_task, start_task_dependencies, end_task, end_task_dependencies
    start_task_dependencies = getTaskDependencies(st_segment_names, st_task_details)
    end_task_dependencies = getTaskDependencies(et_segment_names, et_task_details)
    start_task = [st_segment_names]
    end_task = [et_segment_names]
    return segment_names, segment_dependencies, mapping_dependencies, start_task, start_task_dependencies, end_task, end_task_dependencies

def getDagConfig(source_system, hop_name, batch_frequency, project_name):
    ## Get all configs to create Dag
    base_dir = '/usr/local/airflow'
    metadata = base_dir + '/dags/finance/' + project_name + '/configs/metadata.csv'
    dfcollist = ['source_system', 'hop_name', 'batch_frequency', 'segment_name', 'parent_segment_name', 'segment_order', 'task_name', 'task_type', 'parent_task_name', 'task_order']
    natural_key = ['source_system', 'hop_name', 'batch_frequency', 'segment_name', 'parent_segment_name', 'task_name']
    df = pd.read_csv(metadata, sep='|', index_col=False, names=dfcollist)
    df = df.drop_duplicates(subset=natural_key, keep='first').reset_index(drop=True)
    df = df.query(f"source_system == '{source_system}' & batch_frequency == '{batch_frequency}' & hop_name == '{hop_name}'").reset_index(drop=True)
    segment_names, segment_dependencies, mapping_dependencies, start_task, start_task_dependencies, \
        end_task, end_task_dependencies = getDagCreateConfig(df)
    ## Get variables and parameters of task inside Dag
    configname = source_system + "_" + hop_name + "_" + batch_frequency + "_config"
    config_dict = eval(configname)
    return config_dict, segment_names, segment_dependencies, mapping_dependencies, start_task, \
           start_task_dependencies, end_task, end_task_dependencies

# Checks for controls threshold error and warnings and inform through mail
def check_controls_threshold_error_warning(**kwargs):
    environment = kwargs['environment']
    source_system_name = kwargs['source_system_name']
    domain = kwargs['domain']
    hop_name = kwargs['hop_name']
    cycle_date = kwargs['odate']
    cycle_date = datetime.strptime(cycle_date, '%y%m%d').strftime('%Y-%m-%d')
    email_list = kwargs['email_list']
    cc_email_list = kwargs['cc_email_list']
    if source_system_name == 'all':
        title = f"{environment.upper()} {domain.upper()} {hop_name.upper()} Threshold Check Discrepancies"
        error_email_description = f"Errors found in threshold range check for {domain.upper()} - {hop_name.upper()}. This also caused sweep job to fail. The errors "
        warning_email_description = f"Warnings found in threshold range check for {domain.upper()} - {hop_name.upper()}. These warnings didn't cause a failure to sweep"
        redshift_query_threshold_error = f"select control_results_id, batch_id, sys_nm, domain, measure_table, measure_name, cast(src_measure_value as varchar) as src_measure_value from control_results where domain = '{domain}'"
        redshift_query_threshold_warning = f"select control_results_id, batch_id, sys_nm, domain, measure_table, measure_name, cast(src_measure_value as varchar) as src_measure_value from control_results where domain = '{domain}'"
    else:
        title = f"{environment.upper()} {source_system_name.upper()} {hop_name.upper()} Threshold Check Discrepancies"
        error_email_description = f"Errors found in threshold range check for {source_system_name.upper()} - {hop_name.upper()}. This also caused sweep job to fail. The errors "
        warning_email_description = f"Warnings found in threshold range check for {source_system_name.upper()} - {hop_name.upper()}. These warnings didn't cause a failure to sweep"
        redshift_query_threshold_error = f"select control_results_id, batch_id, sys_nm, domain, measure_table, measure_name, cast(src_measure_value as varchar) as src_measure_value from control_results where sys_nm = '{source_system_name}' and domain = '{domain}'"
        redshift_query_threshold_warning = f"select control_results_id, batch_id, sys_nm, domain, measure_table, measure_name, cast(src_measure_value as varchar) as src_measure_value from control_results where sys_nm = '{source_system_name}' and domain = '{domain}'"
    # make dict
    threshold_error_query_data, columns = python_callable_for_redshift_actions(sql=redshift_query_threshold_error,
                                                                              action='exec_custom_sql', return_locally_with_cols=True, **kwargs)
    columns = list(columns)
    results_threshold_error = []
    for row in threshold_error_query_data:
        row_dict = {}
        for i, col in enumerate(columns):
            row_dict[col.name] = row[i]
        results_threshold_error.append(row_dict)
    df_threshold_error = pd.DataFrame.from_dict(results_threshold_error)
    threshold_warning_query_data, columns = python_callable_for_redshift_actions(sql=redshift_query_threshold_warning,
                                                                                 action='exec_custom_sql', return_locally_with_cols=True, **kwargs)
    columns = list(columns)
    results_threshold_warning = []
    for row in threshold_warning_query_data:
        row_dict = {}
        for i, col in enumerate(columns):
            row_dict[col.name] = row[i]
        results_threshold_warning.append(row_dict)
    df_threshold_warning = pd.DataFrame.from_dict(results_threshold_warning)
    if df_threshold_error.shape[0] > 0 and df_threshold_warning.shape[0] > 0:
        html = """
        <html>
        <head></head>
        <body>
        Hi Team, <br>
        <br>
        {2} <br>
        <br>
        {0} <br>
        <br>
        {3} <br>
        <br>
        {1} <br>
        <br>
        Thanks, <br>
        Finance Datawarehouse Team <br>
        </body>
        </html>""".format(df_threshold_error.to_html(index=False), df_threshold_warning.to_html(index=False), error_email_description, warning_email_description)
        send_email_smtp(email_list, title, html, cc=cc_email_list)
    elif df_threshold_error.shape[0] > 0:
        html = """
        <html>
        <head></head>
        <body>
        Hi Team, <br>
        <br>
        {1} <br>
        <br>
        {0} <br>
        <br>
        Thanks, <br>
        Finance Datawarehouse Team <br>
        </body>
        </html>""".format(df_threshold_error.to_html(index=False), error_email_description)
        send_email_smtp(email_list, title, html, cc=cc_email_list)
    elif df_threshold_warning.shape[0] > 0:
        html = """
        <html>
        <head></head>
        <body>
        Hi Team, <br>
        <br>
        {1} <br>
        <br>
        {0} <br>
        <br>
        Thanks, <br>
        Finance Datawarehouse Team <br>
        </body>
        </html>""".format(df_threshold_warning.to_html(index=False), warning_email_description)
        send_email_smtp(email_list, title, html, cc=cc_email_list)
    else:
        print("No threshold check discrepancies found")

# Read S3
def read_s3(s3_bucket, s3_querypath):
    """Reads the S3 File in a Location"""
    data = s3_client.get_object(Bucket=s3_bucket, Key=s3_querypath)
    code = data['Body'].read()
    return code.decode("utf-8")

# Refresh Materialized View
def refresh_materialized_view(view_name, **kwargs):
    print(f"Refreshing Materialized View dw_materialized_views.{view_name}")
    query = f"""refresh materialized view dw_materialized_views.{view_name}"""
    environment = kwargs['dag_run'].conf.get('env')
    project = kwargs['dag_run'].conf.get('project')
    s3_code_bucket = f"ta-individual-findw-{environment}-codedeployment"
    try:
        python_callable_for_redshift_actions(sql=query, action='exec_custom_sql', return_locally=False, **kwargs)
    except Exception as e:
        if "not found" in str(e).lower():
            try:
                print(f"Materialized view not found. Attempting to create from S3 definition.")
                view_path = f"{project}/redshift/finance/views/dw_materialized_views/{view_name}.sql"
                query = read_s3(s3_code_bucket, view_path)
                python_callable_for_redshift_actions(sql=query, action='exec_custom_sql', return_locally=False, **kwargs)
                print(f"Successfully created materialized view {view_name}")
            except Exception as ex:
                raise AirflowException(f"Failed to create materialized view {view_name} from S3: {str(ex)}")
        else:
            raise AirflowException(f"Failed to refresh materialized view {view_name}: {str(e)}")

# Load CSV File From S3 to Redshift
def load_s3_to_redshift(bucket_name, file_key, table_name, delete_existing=True, delimiter='|', column_string=None, **kwargs):
    try:
        host, username, password, port, dbname = fetch_creds(False, **kwargs)
        s3_response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        min_rows_expected = sum(1 for _ in s3_response['Body'].iter_lines()) - 1
        env = kwargs['task_instance'].xcom_pull(task_ids='start', key='env')
        acctno = env_acct_dict[env]
        conn = psycopg.connect(
            dbname=dbname,
            host=host,
            port=port,
            user=username,
            password=password,
            connect_timeout=10
        )
        conn.autocommit = False
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        initial_count = cursor.fetchone()[0]
        print(f"Initial row count: {initial_count}")
        if delete_existing and delete_existing is True:
            print(f"Deleting table {table_name}")
            cursor.execute(f"delete from {table_name}")
        column_string_ = ''
        if column_string:
            column_string_ = f"({column_string})"
        copy_cmd = f"""
        COPY {table_name} {column_string_}
        FROM 's3://{bucket_name}/{file_key}'
        IAM_ROLE 'arn:aws:iam::{acctno}:role/ta-individual-findw-{env}-redshift'
        DELIMITER '{delimiter}'
        NULL AS 'NULL'
        REMOVEQUOTES
        ESCAPE
        IGNOREHEADER 1
        EMPTYASNULL
        IGNOREBLANKLINES
        BLANKSASNULL
        TIMEFORMAT 'auto'
        """
        cursor.execute(copy_cmd)
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        final_count = cursor.fetchone()[0]
        rows_loaded = final_count - (0 if delete_existing else initial_count)
        if rows_loaded < min_rows_expected:
            raise AirflowException(
                f"Load validation failed: Expected at least {min_rows_expected} rows, but got {rows_loaded}")
        else:
            print(f"Load validation passed: Expected at least {min_rows_expected} rows, and got {rows_loaded}")
        conn.commit()
    except Exception as e:
        print(f"Error: {str(e)}")
        if 'conn' in locals() and conn and not conn.closed:
            try:
                print("Starting rollback...")
                conn.rollback()
                print("Rollback completed")
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                current_count = cursor.fetchone()[0]
                print(f"Current row count after rollback: {current_count}")
                if current_count != initial_count:
                    print("WARNING: Row count mismatch after rollback!")
            except Exception as rollback_error:
                print(f"Error during rollback or verification: {str(rollback_error)}")
                raise AirflowException(f"Error loading data into Table {table_name}: {str(e)}")
    finally:
        if 'cursor' in locals() and cursor and not cursor.closed:
            cursor.close()
        if 'conn' in locals() and conn and not conn.closed:
            conn.close()
            print("Connection and cursor closed")

# Create Excel
def create_excel(data_pd, excel_filename):
    with tempfile.NamedTemporaryFile(mode='wb+', delete=False, suffix=".xlsx") as fp:
        temp_file_path = fp.name
    with pd.ExcelWriter(temp_file_path, engine='xlsxwriter') as writer:
        data_pd.to_excel(writer, sheet_name='Data', index=False)
        workbook = writer.book
        worksheet = writer.sheets['Data']
        for column in data_pd:
            column_length = max(data_pd[column].astype(str).map(len).max(), len(column))
            col_idx = data_pd.columns.get_loc(column)
            worksheet.set_column(col_idx, col_idx, column_length)
    new_file_path = f"/tmp/{excel_filename}.xlsx"
    os.rename(temp_file_path, new_file_path)
    return new_file_path

# Check for Changes in Currency Rate for MKDB and Notify
def forex_check_cdc(**kwargs):
    domain_name = kwargs['domain_name']
    environment = kwargs['environment']
    source_system_name = kwargs['source_system_name']
    batch_frequency = kwargs['batch_frequency']
    application = kwargs['application']
    notification_category = kwargs['notification_category']
    wrk_table_name = kwargs['wrk_table_name']
    cycle_date = kwargs['odate']
    cycle_date = datetime.strptime(cycle_date, '%y%m%d').strftime('%Y-%m-%d')
    excel_filename = f"{domain_name}_{source_system_name}_forex_change_{cycle_date}"
    query = f"select generalledgertransactioncurrencycode, generalledgerreportingcurrencycode, generalledgerreportingcurrencyconversionratetype, generalledgerreportingcurrencyexchangerate, generalledgereffectiveperiodstartdate, generalledgereffectiveperiodenddate, generalledgerreportingcurrencyexchangerate from erpdworking.{wrk_table_name} where actiontype = 'UPDATE' and sourcetable = 'Target' order by 1, 2, 3, 4"
    data, columns = python_callable_for_redshift_actions(sql=query, action='exec_custom_sql', return_locally_with_cols=True, **kwargs)
    columns = list(columns)
    data_list = []
    for row in data:
        row_dict = {}
        for i, col in enumerate(columns):
            row_dict[col.name] = row[i]
        data_list.append(row_dict)
    data = pd.DataFrame.from_dict(data_list)
    query_smry = f"select generalledgertransactioncurrencycode, generalledgerreportingcurrencycode from erpdworking.{wrk_table_name} where actiontype = 'UPDATE' and sourcetable = 'Target' group by generalledgertransactioncurrencycode, generalledgerreportingcurrencycode order by generalledgertransactioncurrencycode, generalledgerreportingcurrencycode"
    data_smry, columns = python_callable_for_redshift_actions(sql=query_smry, action='exec_custom_sql', return_locally_with_cols=True, **kwargs)
    columns = list(columns)
    data_smry_list = []
    for row in data_smry:
        row_dict = {}
        for i, col in enumerate(columns):
            row_dict[col.name] = row[i]
        data_smry_list.append(row_dict)
    data_smry = pd.DataFrame.from_dict(data_smry_list)
    attachment = create_excel(data, excel_filename)
    attachment_list = [attachment]
    if data.shape[0] > 0:
        title = f"{environment.upper()} Info: There is Change in Currency Exchange Rate"
        email_body = """
        <html>
        <head></head>
        <body>
        Hi, <br>
        <br>
        There are changes in Currency Exchange Rate. Please check Attachment for More Details. <br>
        Cycle Date Processed: {1} <br>
        <br>
        Currency impacted by change in Exchange Rate as below. <br>
        <br>
        {0} <br>
        <br>
        Thanks, <br>
        Finance Datawarehouse Team <br>
        </body>
        </html>""".format(data_smry.to_html(index=False), cycle_date)
        custom_send_email(domain_name, environment, source_system_name, batch_frequency, title, email_body, notification_category, 'all', attachment_list, **kwargs)
        os.remove(attachment)
    else:
        print("There is no change in Currency Exchange Rate")

# Check for Missing Batches in FINDW/ERPDW
def check_for_missing_batches(**kwargs):
    environment = kwargs['environment']
    application = kwargs['application']
    domain_name = kwargs['domain_name']
    cst = timezone('US/Central')
    today_cst = datetime.now(cst)
    days_back = (today_cst.weekday() + 7) if today_cst.weekday() != 6 else 6
    prior_monday_cst = today_cst - timedelta(days=days_back)
    prior_monday_yyyy_mm_dd = prior_monday_cst.strftime('%Y-%m-%d')
    application_in_email = application.upper()
    application_in_query = application
    if application == 'finance':
        application_in_email = 'FINDW'
        application_in_query = 'financedw'
    query = f"""select weekbegindate, weekenddate, application, source_system_name, batch_frequency, expected_runs_per_week, no_runs as actual_run_cnt, cnt_domain as expected_total_domains_cnt, cnt_domain_calc as actual_completed_domains_cnt from financedworchestration.datawarehouse_missing_loads_in_week where application = '{application_in_query}' and weekbegindate = '{prior_monday_yyyy_mm_dd}'"""
    data, columns = python_callable_for_redshift_actions(sql=query, action='exec_custom_sql', return_locally_with_cols=True, **kwargs)
    columns = list(columns)
    data_list = []
    for row in data:
        row_dict = {}
        for i, col in enumerate(columns):
            row_dict[col.name] = row[i]
        data_list.append(row_dict)
    data = pd.DataFrame.from_dict(data_list)
    if data.shape[0] > 0:
        title = f"{environment.upper()} WARN: There Are Missing Batches From Prior Week for {application_in_email}"
        email_body = """
        <html>
        <head></head>
        <body>
        Hi, <br>
        <br>
        There Are Missing Batches From Prior Week Starting. Details can be found below. <br>
        <br>
        {0} <br>
        <br>
        Thanks, <br>
        Finance Datawarehouse Team <br>
        </body>
        </html>""".format(data.to_html(index=False), prior_monday_yyyy_mm_dd)
        custom_send_email(domain_name, environment, 'N/A', 'N/A', title, email_body, 'job_status', 'all', **kwargs)
    else:
        print(f"There are no Missing Batches from Prior Week Starting - {prior_monday_yyyy_mm_dd} (Monday)")

# Email Notification on successful completion of ingestion
def email_notify_ingestion_complete(**kwargs):
    domain_name = kwargs['domain_name']
    environment = kwargs['environment']
    source_system_name = kwargs['source_system_name']
    print(f"source system name: {source_system_name}")
    batch_frequency = kwargs['batch_frequency']
    application = kwargs['application']
    cycle_date = kwargs['odate']
    cycle_date = datetime.strptime(cycle_date, '%y%m%d').strftime('%Y-%m-%d')
    query = f"select source_system_name from financedworchestration.notification where \
    domain_name = '{domain_name}' and source_system_name = '{source_system_name}' and batch_frequency = '{batch_frequency}' \
    and environment_code = '{environment}' and notification_category = 'ingestion complete' and notification_type = 'all'"
    notification_opt = python_callable_for_redshift_actions(sql=query, action='exec_custom_sql', return_locally=True, **kwargs)
    notification_result = None
    if bool(notification_opt):
        if notification_opt[0] is not None:
            notification_result = notification_opt[0]
    if notification_result is None:
        raise AirflowSkipException(f"No entry found in financedworchestration.notification table for source_system_name: {source_system_name} , notification_category: ingestion complete and notification_type: all, skipping ingestion success email trigger.")
    else:
        if source_system_name == 'alm':
            email_ovrrd = 'Fundmapping'
        else:
            email_ovrrd = source_system_name
        title = f"{environment.upper()} Info: Ingestion Loads for {email_ovrrd} Completed Successfully"
        email_body = """
        <html>
        <head></head>
        <body>
        Hi, <br>
        <br>
        The {0} ingestion load completed successfully for order date: {1}. <br>
        <br>
        Thanks, <br>
        Finance Datawarehouse Team <br>
        </body>
        </html>""".format(email_ovrrd, cycle_date)
        custom_send_email(domain_name, environment, source_system_name, batch_frequency, title, email_body, 'ingestion complete', 'all', **kwargs)