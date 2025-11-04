#!/usr/bin/python
# -*- coding: utf-8 -*-

####################################################################################################
#Purpose: Airflow code to orchestrate a GDQ source system's workflow.
#         Based on dependencies defined for workflows and tasks within workflows in config file for a given source system,
#         the script creates workflow execution pipeline named-
#
#config JSON = {"env": "{project_environment_name}", "project": "{project_name}", "odate": "YYYYMMDD", "job_name": "XXYY"}
####################################################################################################

####################################### DEVELOPMENT LOG ############################################
#12/12/2022 - Sandeep Jagurti/Atul Vishal  - ALMfundmapping Orchestration framework development
#Version - Initial Release
####################################################################################################

from airflow.models import DAG
from finance.{project_name}.common_utils import *

source_system='alm'
hop_name='gdq'
batch_frequency='quarterly'
project_name='{project_name}'
env_name='{project_environment_name}'
emr_name='curated'

config_file_path, dag_id, dag_default_args, schedule_interval, tags, is_paused_upon_creation, sp_dict, default_region, \
baseline_domain_list_findw, derived_domain_list = \
getDagParams(project_name, "tags", source_system, batch_frequency)

dag_id = dag_id.upper() + "_" + source_system.upper() + "_" + hop_name.upper() + "_" + batch_frequency.upper()
tags = list(map(lambda x: x.replace("ALM", "ALM"), tags))
tags.append('gdq')

config_dict, segment_names, segment_dependencies, mapping_dependencies, start_tasks, \
start_task_dependencies, end_task, end_task_dependencies = \
getGdqConfig(source_system, hop_name, batch_frequency, project_name)

####################################### DAG DEFINITION ############################################
with DAG(
    dag_id=dag_id,
    default_args=dag_default_args,
    schedule=schedule_interval,
    is_paused_upon_creation=is_paused_upon_creation,
    tags=tags
) as dag:

    ##Create start task- get dag variables and push to xcom
    start = create_python_operator(
        dag=dag,
        task_name='start',
        op_kwargs={
            'config_file_path': config_file_path,
            'source_system': source_system
        },
        python_callable=get_findw_var,
        trigger_rule='all_success',
        retries=0
    )

    ##trigger email notification
    task_email_notify = create_python_operator(
        dag=dag,
        task_name='TASK_Send_Email',
        op_kwargs={
            'source_system_name': 'N/A',
            'domain_name': 'FINDW',
            'batch_frequency': 'N/A',
            'environment': '{{ task_instance.xcom_pull(task_ids="start", key="env") }}',
            'job_name': '{{ task_instance.xcom_pull(task_ids="start", key="job_name") }}',
            'odate': '{{ task_instance.xcom_pull(task_ids="start", key="odate") }}'
        },
        python_callable=notify_email,
        trigger_rule='one_failed',
        retries=0
    )

    final_status = PythonOperator(
        task_id='finally',
        dag=dag,
        python_callable=final_status,
        trigger_rule='all_done',
        retries=0
    )

    create_curated_cluster = create_python_operator_retries(
        dag=dag,
        task_name='create_' + emr_name + '_cluster',
        op_kwargs={
            'emr_name': emr_name,
            'action': 'create'
        },
        python_callable=emr_actions,
        trigger_rule='all_success'
    )

    start >> create_curated_cluster

    ##create start task e.g. currentbatchid for p65
    lst_se_task = createMappings(source_system, start_tasks, start_task_dependencies, config_dict, dag, emr_name)

    ##create end task e.g. completedbatchid for p65
    lst_se_task = createMappings(source_system, end_task, end_task_dependencies, config_dict, dag, emr_name)

    ##create taskgroup for workflow and mappings inside workflow
    lst_taskgrp = createMappings(source_system, segment_names, mapping_dependencies, config_dict, dag, emr_name)

    ##Define dependencies between workflows
    defineDependencies(dag, segment_dependencies, create_curated_cluster, task_email_notify, final_status, start_tasks, end_task)
