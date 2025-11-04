# File: emr_serverless.py
"""
Purpose: Common Functions for EMR Serverless Actions to Run Spark Programs from Airflow
Usage: from emr_serverless import *
03/25/2024 - Prathyush Premachandran - Finance DW framework development
"""
import time
import boto3
from datetime import datetime, timedelta
from botocore.exceptions import ClientError
from botocore.client import Config
from airflow.exceptions import AirflowException
region = 'us-east-1'
config = Config(retries={'max_attempts': 15, 'mode': 'adaptive'})
acct_no_dict = {'dev': 209497537643, 'tst': 209497537643, 'mdl': 510200178882, 'prd': 492931852779}
emrsvls = boto3.client('emr-serverless', region_name=region, config=config)
application_valid_states = ['CREATING', 'STARTING', 'STARTED', 'CREATED']
application_invalid_states = ['STOPPING', 'STOPPED', 'TERMINATED']
jobRun_valid_states = ['SUBMITTED', 'PENDING', 'SCHEDULED', 'RUNNING']
jobRun_invalid_states = ['FAILED', 'CANCELLING', 'CANCELLED']
# Application States
# State               Description
# Creating            The application is being prepared and isn’t ready to use yet.
# Created             The application has been created but hasn’t provisioned capacity yet.
#                     You can modify the application to change its initial capacity configuration.
# Starting            The application is starting and is provisioning capacity.
# Started             The application is ready to accept new jobs. The application only accepts jobs when it’s in this state.
# Stopping            All jobs have completed and the application is releasing its capacity.
# Stopped             The application is stopped and no resources are running on the application.
#                     You can modify the application to change its initial capacity configuration.
# Terminated          The application has been terminated and doesn’t appear on your application list.
# Job Run States
# State               Description
# Submitted           The initial job state when you submit a job run to EMR Serverless. The job waits to be scheduled for the application.
# Pending             EMR Serverless begins to prioritize and schedule the job run.
# Scheduled           The scheduler is evaluating the job run to prioritize and schedule the run for the application.
# Running             EMR Serverless has scheduled the job run for the application, and is allocating resources to execute the job.
#                     EMR Serverless has allocated the resources that the job initially needs, and the job is running in the application.
#                     In Spark applications, this means that the Spark driver process is in the running state.
# Failed              EMR Serverless failed to submit the job run to the application, or it completed unsuccessfully.
# Success             See StateDetails for additional information about this job failure.
# Cancelling          The CancelJobRun API has requested job run cancellation, or the job run has timed out. E
# Cancelled           MR Serverless is trying to cancel the job in the application and release the resources.
#                     The job run was cancelled successfully, and the resources that it used have been released.
def delete_application(applicationId: str) -> None:
    try:
        emrsvls.delete_application(applicationId=applicationId)
        print(f'Application {applicationId} is Deleted')
    except ClientError as e:
        if e.response['Error']['Code'] in ['ResourceNotFoundException', 'ValidationException']:
            print(f'Exception Caught in delete_application. But Skipping. Exception Details: {e}')
            pass
        else:
            raise AirflowException(f'Exception Caught in delete_application. Exception Details: {e}')
def start_application(applicationId: str) -> None:
    try:
        emrsvls.start_application(applicationId=applicationId)
        print(f'Application {applicationId} is Started')
    except ClientError as e:
        if e.response['Error']['Code'] in ['ResourceNotFoundException', 'ValidationException']:
            print(f'Exception Caught in start_application. But Skipping. Exception Details: {e}')
            pass
        else:
            raise AirflowException(f'Exception Caught in start_application. Exception Details: {e}')
def stop_application(applicationId: str) -> None:
    try:
        emrsvls.stop_application(applicationId=applicationId)
        print(f'Application {applicationId} is Stopped')
    except ClientError as e:
        if e.response['Error']['Code'] in ['ResourceNotFoundException', 'ValidationException']:
            print(f'Exception Caught in stop_application. But Skipping. Exception Details: {e}')
            pass
        else:
            raise AirflowException(f'Exception Caught in stop_application. Exception Details: {e}')
def get_application(applicationId: str) -> dict:
    try:
        response = emrsvls.get_application(applicationId=applicationId)
        return response
    except Exception as e:
        raise AirflowException(f'Exception Caught in get_application. Exception Details: {e}')
def page_list_applications(appName: str, check_states: list) -> tuple:
    try:
        paginator = emrsvls.get_paginator("list_applications")
        pages = paginator.paginate(states=check_states)
        i = 0
        application_ids_lst = []
        application_state_lst = []
        for page in pages:
            applications = page['applications']
            while i < len(applications):
                if appName in applications[i]['name']:
                    application_ids_lst.append(applications[i]['id'])
                    application_state_lst.append(applications[i]['state'])
                i = i + 1
        return application_ids_lst, application_state_lst
    except Exception as e:
        raise AirflowException(f'Exception Caught in page_list_applications. Exception Details: {e}')
def list_applications(appName: str, checkStopped: bool = False) -> tuple:
    try:
        appid, state = None, None
        application_ids_lst, application_state_lst = page_list_applications(appName, ['CREATING', 'CREATED', 'STARTING', 'STARTED'])
        if application_ids_lst and len(application_ids_lst) > 1:
            for applicationId in application_ids_lst:
                stop_application(applicationId)
            raise AirflowException(f'Multiple Applications Active and all were Stopped - {application_ids_lst}')
        elif application_ids_lst and len(application_ids_lst) == 1:
            appid = application_ids_lst[0]
            state = application_state_lst[0]
        elif checkStopped:
            application_ids_lst, application_state_lst = page_list_applications(appName, ['STOPPED'])
            if application_ids_lst:
                application_ids_lst.sort()
                appid, state = application_ids_lst[0], 'STOPPED'
        if not appid:
            print(f'No Application Like - {appName}')
        else:
            print(f'Application ID for {appName} - {appid}, its state is {state}')
        return appid, state
    except Exception as e:
        raise AirflowException(f'Exception Caught in list_applications. Exception Details: {e}')
def check_and_delete_application(applicationId: str) -> None:
    try:
        response = get_application(applicationId)
        state = response['application']['state']
        if state in application_valid_states:
            stop_application(applicationId)
            delete_application(applicationId)
        else:
            print(f'Application State is already {state} so not deleted.')
    except ClientError as e:
        if e.response['Error']['Code'] in ['ResourceNotFoundException', 'ValidationException']:
            print(f'Exception Caught in check_and_delete_application. But Skipping. Exception Details: {e}')
            pass
        else:
            raise AirflowException(f'Exception Caught in check_and_delete_application. Exception Details: {e}')
def create_application(kwargs: dict, appName: str) -> str:
    try:
        applicationId, state = list_applications(appName, True)
        if not applicationId:
            response = emrsvls.create_application(
                name=appName,
                releaseLabel=kwargs['releaseLabel'],
                type=kwargs['type'],
                initialCapacity=kwargs['initialCapacity'],
                maximumCapacity=kwargs['maximumCapacity'],
                tags=kwargs['tags'],
                autoStartConfiguration=kwargs['autoStartConfiguration'],
                autoStopConfiguration=kwargs['autoStopConfiguration'],
                networkConfiguration=kwargs['networkConfiguration'],
                architecture=kwargs['architecture'],
                monitoringConfiguration=kwargs['monitoringConfiguration'],
                runtimeConfiguration=kwargs['runtimeConfiguration'])
            applicationId = response['applicationId']
            print(f'Application ID for {appName} Created : {applicationId}')
            get_application_and_wait(applicationId)
        else:
            if state == 'STOPPED':
                update_application(kwargs, appName, applicationId)
                start_application(applicationId)
                print(f'Application {applicationId} already existed for {appName}')
        list_applications(appName)
        return applicationId
    except Exception as e:
        raise AirflowException(f'Exception Caught in create_application. Exception Details: {e}')
# File: emr_serverless.py
def update_application(kwargs: dict, appName: str = None, applicationId: str = None) -> None:
    try:
        if not applicationId and appName:
            applicationId, ignore = list_applications(appName)
        if applicationId:
            emrsvls.update_application(
                applicationId=applicationId,
                releaseLabel=kwargs['releaseLabel'],
                initialCapacity=kwargs['initialCapacity'],
                maximumCapacity=kwargs['maximumCapacity'],
                autoStartConfiguration=kwargs['autoStartConfiguration'],
                autoStopConfiguration=kwargs['autoStopConfiguration'],
                networkConfiguration=kwargs['networkConfiguration'],
                architecture=kwargs['architecture'],
                monitoringConfiguration=kwargs['monitoringConfiguration'],
                runtimeConfiguration=kwargs['runtimeConfiguration'])
            print(f'Application ID for {appName} : {applicationId} is now Updated with {kwargs}')
        else:
            raise AirflowException(f'Application Name {appName} is not Existing')
    except Exception as e:
        raise AirflowException(f'Exception Caught in update_application. Exception Details: {e}')
def get_application_and_wait(applicationId: str) -> None:
    try:
        state = emrsvls.get_application(applicationId=applicationId)['application']['state']
        while state not in application_valid_states:
            response = get_application(applicationId)
            state = response['application']['state']
            if state in application_invalid_states:
                raise AirflowException(f'Application {applicationId} in Invalid Status - {state}')
            time.sleep(60)
        print(f'Application {applicationId} Final State is {state}')
    except Exception as e:
        raise AirflowException(f'Exception Caught in get_application_and_wait. Exception Details: {e}')
def start_job_run(env: str, applicationType: str, applicationId: str, jobName: str, tags: str, programFile: str, parameters: str,
                  sparkArg_or_HiveInitFile: str = None, executionTimeoutMinutes: int = 600, timeoutHours: int = 6) -> None:
    try:
        submittedAt = datetime.now()
        if applicationType == 'Spark':
            response = get_application(applicationId)
            releaseLabel = response['application']['releaseLabel'].replace('.', '-')
            parameters = '--conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory ' \
                         f'--conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python ' \
                         f'--conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python ' \
                         f'--conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python ' + parameters
            #environment --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python
            if not sparkArg_or_HiveInitFile:
                entryPointArguments = []
            else:
                entryPointArguments = sparkArg_or_HiveInitFile.split()
            jobDriver = {'sparkSubmit': {
                'entryPoint': programFile,
                'entryPointArguments': entryPointArguments,
                'sparkSubmitParameters': parameters}}
        else:
            initQueryFile = None
            if sparkArg_or_HiveInitFile:
                initQueryFile = sparkArg_or_HiveInitFile
            jobDriver = {'hive': {
                'query': programFile,
                'initQueryFile': initQueryFile,
                'parameters': parameters}}
            if not initQueryFile:
                jobDriver['hive'].pop('initQueryFile')
            if not parameters:
                jobDriver['hive'].pop('parameters')
        response = emrsvls.start_job_run(
            applicationId=applicationId,
            name=jobName,
            executionRoleArn=f'arn:aws:iam::{acct_no_dict[env]}:role/ta-individual-findw-{env}-emr-serverless',
            jobDriver=jobDriver,
            tags=tags,
            executionTimeoutMinutes=executionTimeoutMinutes)
        jobRunId = response['jobRunId']
        print(f'Job Run ID for Job {jobName} is {jobRunId}, submitted at {submittedAt} and will Timeout in {executionTimeoutMinutes} minutes')
        get_job_run_and_wait(applicationType, applicationId, jobRunId, env, submittedAt, timeoutHours)
    except Exception as e:
        raise AirflowException(f'Exception Caught in start_job_run. Exception Details: {e}')
def get_job_run(applicationId: str, jobRunId: str) -> dict:
    try:
        response = emrsvls.get_job_run(
            applicationId=applicationId,
            jobRunId=jobRunId)
        return response
    except Exception as e:
        raise AirflowException(f'Exception Caught in get_job_run. Exception Details: {e}')
def get_job_run_and_wait(applicationType: str, applicationId: str, jobRunId: str, env: str, submittedAt: datetime, timeoutHours: int = 6) -> None:
    try:
        common_text = f'Job {jobRunId} Created in Application {applicationId}'
        state = ''
        i = 0
        while state != 'SUCCESS':
            i += 1
            time.sleep(60)
            response = get_job_run(applicationId, jobRunId)
            state = response['jobRun']['state']
            if i == 1:
                print(f"{common_text} at {response['jobRun']['createdAt']}")
            if state in jobRun_valid_states:
                print(f"{common_text} Current JobRun State is {state}")
                if i == 1 and state == 'RUNNING':
                    get_dashboard_for_job_run(applicationId, jobRunId)
                    i = 0
                if i % 60 == 0 and i/60 == timeoutHours:
                    raise AirflowException(f'{common_text} is in {state} for around {timeoutHours} hours. Please check')
                if i == 60 and state in ['SUBMITTED', 'PENDING', 'SCHEDULED']:
                    raise AirflowException(f'{common_text} is in {state} for around 1 hour. Please check')
                continue
            elif state in jobRun_invalid_states:
                list_job_runs(applicationType, applicationId, jobRunId, submittedAt, jobRun_invalid_states, env)
                get_dashboard_for_job_run(applicationId, jobRunId)
                raise AirflowException(f'{common_text} is in Invalid State - {state}')
        if state == 'SUCCESS':
            get_dashboard_for_job_run(applicationId, jobRunId)
            print(f'{common_text} Final JobRun State is {state}')
    except Exception as e:
        raise AirflowException(f'Exception Caught in get_job_run_and_wait. Exception Details: {e}')
def list_job_runs(applicationType, applicationId: str, jobRunId: str, submittedAt: datetime, state: list, env: str) -> None:
    try:
        paginator = emrsvls.get_paginator("list_job_runs")
        pages = paginator.paginate(applicationId=applicationId, states=state, createdAfter=submittedAt,
                                   createdBefore=submittedAt + timedelta(hours=8))
        if applicationType == 'Spark':
            ovrd_path = 'SPARK_DRIVER'
        else:
            ovrd_path = 'HIVE_DRIVER'
        i = 0
        for page in pages:
            jobRuns = page['jobRuns']
            while i < len(jobRuns):
                if jobRuns[i]['id'] == jobRunId:
                    stateDetails = jobRuns[i]['stateDetails']
                    print("###########################################################")
                    print(f'SysOut Details for Job {jobRunId} Created in Application {applicationId} - {stateDetails}')
                    s3_log = f's3://ta-individual-findw-{env}-logs/emr_serverless/applications/{applicationId}/jobs/{jobRunId}/{ovrd_path}/stdout.gz'
                    s3_err = f's3://ta-individual-findw-{env}-logs/emr_serverless/applications/{applicationId}/jobs/{jobRunId}/{ovrd_path}/stderr.gz'
                    print(f'{applicationType} Driver Log File is {s3_log} and Error file is {s3_err}')
                    print("###########################################################")
                i = i + 1
    except Exception as e:
        raise AirflowException(f'Exception Caught in list_job_runs. Exception Details: {e}')
def cancel_job_run(applicationId: str, jobRunId: str) -> dict:
    try:
        response = emrsvls.cancel_job_run(
            applicationId=applicationId,
            jobRunId=jobRunId)
        print(f'Job ID {jobRunId} Cancelled in Application {applicationId}')
        return response
    except Exception as e:
        raise AirflowException(f'Exception Caught in cancel_job_run. Exception Details: {e}')
def cancel_all_job_runs_and_stop_application(applicationId: str) -> None:
    try:
        paginator = emrsvls.get_paginator("list_job_runs")
        pages = paginator.paginate(applicationId=applicationId, states=jobRun_valid_states)
        i = 0
        for page in pages:
            jobRuns = page['jobRuns']
            while i < len(jobRuns):
                cancel_job_run(applicationId, jobRuns[i]['id'])
                i = i + 1
        #time.sleep(60)
        emrsvls.stop_application(applicationId=applicationId)
        print(f'Application {applicationId} is Stopped')
    except Exception as e:
        raise AirflowException(f'Exception Caught in cancel_all_job_runs_and_stop_application at {datetime.now()}... Retrying.. Exception Details: {e}')
# File: emr_serverless.py
def get_dashboard_for_job_run(applicationId: str, jobRunId: str) -> None:
    try:
        response = emrsvls.get_dashboard_for_job_run(
            applicationId=applicationId,
            jobRunId=jobRunId)
        print("###########################################################")
        print(f'Application UI for JobRun {jobRunId} in Application {applicationId} is {response["url"]}')
        print("###########################################################")
    except Exception as e:
        print(f'Exception Caught in get_dashboard_for_job_run. Exception Details:, But Skipping: {e}')
        pass
def emr_serverless_action(**kwargs):
    from finance.{project_name}.configs.emr_serverless_commands import spark_commands
    from finance.{project_name}.configs.emr_serverless_params import params
    mandatory_kwargs = ['action', 'emr_source_system', 'batch_frequency', 'hop_name']
    optional_kwargs = ['domain', 'curated_source', 'emr_size', 'script_name']
    domain = None
    hop_name = None
    script_name = None
    ctrlM_job_name = None
    action = 'run'
    applicationType = 'Spark'
    emr_source_system = 'all'
    batch_frequency = 'DAILY'
    source_system = None
    env = kwargs['task_instance'].xcom_pull(task_ids='start', key='env')
    project = kwargs['task_instance'].xcom_pull(task_ids='start', key='project')
    odate = kwargs['task_instance'].xcom_pull(task_ids='start', key='date')
    cycle_date = datetime.strptime(str(odate), '%Y%m%d').strftime('%Y-%m-%d')
    try:
        ctrlM_job_name = kwargs['task_instance'].xcom_pull(task_ids='start', key='job_name')
    except Exception as e:
        pass
    redshift_db = project.replace('financedw', 'findw')
    for key, value in kwargs.items():
        if key in mandatory_kwargs + optional_kwargs:
            globals()[f"{key}"] = value
    if kwargs.get('action'):
        action = kwargs['action']
    if kwargs.get('batch_frequency'):
        batch_frequency = kwargs['batch_frequency']
    if kwargs.get('emr_source_system'):
        emr_source_system = kwargs['emr_source_system']
    if not source_system and kwargs.get('source_system'):
        source_system = kwargs['source_system']
    else:
        source_system = 'UNKNOWN'
    if kwargs.get('domain'):
        domain = kwargs['domain']
    if kwargs.get('hop_name'):
        hop_name = kwargs['hop_name']
    if kwargs.get('script_name'):
        script_name = kwargs['script_name']
    if not kwargs.get('emr_size'):
        emr_sizes_dict = {'prd': 'large', 'mdl': 'large', 'tst': 'small', 'dev': 'tiny'}
        kwargs['emr_size'] = emr_sizes_dict[env]
    if not kwargs.get('executionTimeoutMinutes'):
        kwargs['executionTimeoutMinutes'] = 600
    if not kwargs.get('timeoutHours'):
        kwargs['timeoutHours'] = 6
    appname = f'ta-individual-findw-{env}-emr-{applicationType.lower()}-{emr_source_system}-{project}'
    appName = f'{appname}-{cycle_date}'
    emr_size = kwargs['emr_size']
    print(f"*************** {appName} ***************")
    print(f"It's a {applicationType} Application ***************")
    emr_network_details = params['emr_network_details']
    networkConfiguration = emr_network_details[env]
    emr_sizes = params['emr_sizes']
    initialCapacity = emr_sizes[emr_size]['initialCapacity']
    maximumCapacity = emr_sizes[emr_size]['maximumCapacity']
    tags = eval(params['tags'])
    emr_timeouts = params['emr_timeouts']
    idleTimeoutMinutes = emr_timeouts[env]
    emr_config = eval(params['emr_config'])
    if action == 'create':
        applicationId = create_application(emr_config, appName)
    if action == 'start':
        applicationId, ignore = list_applications(appName, True)
        if applicationId:
            start_application(applicationId)
    elif action == 'stop':
        applicationId, ignore = list_applications(appName)
        stop_application(applicationId)
    elif action == 'update':
        applicationId, ignore = list_applications(appName, True)
        if applicationId:
            stop_application(applicationId)
            time.sleep(30)
            update_application(emr_config, appName, applicationId=applicationId)
    elif action == 'cancel':
        applicationId = kwargs['task_instance'].xcom_pull(task_ids='start', key='applicationId')
        jobRunId = kwargs['task_instance'].xcom_pull(task_ids='start', key='jobRunId')
        cancel_job_run(applicationId, jobRunId)
    #elif action == 'kill':
    #    applicationId, ignore = list_applications(appName)
    #    cancel_all_job_runs_and_stop_application(appName)
    elif action == 'run':
        sparkSubmitParameters = emr_sizes[emr_size]['sparkSubmitParameters']
        sparkSubmitParameters = f"{sparkSubmitParameters} --py-files s3://ta-individual-findw-{env}-codedepolyment/{project}/scripts/{hop_name}/scripts.zip"
        programFile = eval(f"spark_commands['spark_scripts']['{script_name}']['entryPoint']")
        programFile = f"s3://ta-individual-findw-{env}-codedepolyment/{project}/scripts/{hop_name}/{programFile}"
        script_args = eval(f"spark_commands['hop_names']['{hop_name}']['spark_scripts']['{script_name}']['entryPointArguments']")
        script_args = eval(script_args)
        parameters = eval(f"spark_commands['common_vars'].get('spark_params')")
        s3_jar_location = eval(params['jar_location'])
        s3_jars = [s3_jar_location + i for i in params['jars']]
        s3_jars = ','.join(s3_jars)
        if not parameters:
            parameters = sparkSubmitParameters
        else:
            parameters = f"{sparkSubmitParameters} {parameters}"
            parameters = parameters + ' ' + f"--jars {s3_jars}"
        if not kwargs.get('jobName'):
            kwargs['jobName'] = f"{hop_name.upper()} Layer Run {source_system} - Script {script_name}"
        if domain:
            kwargs['jobName'] = f"{hop_name.upper()} Layer Run {domain}({source_system}) - Script {script_name}"
        applicationId = create_application(emr_config, appName)
        start_job_run(env, applicationType, applicationId, kwargs['jobName'], tags, programFile, parameters,
                      script_args, kwargs['executionTimeoutMinutes'], kwargs['timeoutHours'])
    else:
        raise AirflowException(f"Invalid Action Type {kwargs['action']} Provided. Should be in ['create', 'update', 'start', 'stop', 'run', 'cancel']")

