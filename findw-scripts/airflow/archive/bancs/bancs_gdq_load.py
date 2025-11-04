# Purpose: Airflow code to orchestrate orchestrate GDQ BANCS Load.
# Config JSON = {"env": "(project_environment_name)", "project": "{project_name}", "odate": "YYYYMMDD", "job_name": "XXYY", "rerun_flag": "Y/N"}

# -------------------- DEVELOPMENT LOG --------------------
# 01/25/2023 - Prathush Premachandran - Initial Release -> BANCS Move to Redshift

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from finance.{project_name}.common_utils import *

source_system = 'BANCS'
domain_name = 'finwd'
project_name = '{project_name}'
batch_frequency = 'daily'
emr_name = 'curated'
kc2_source = ['Activity', 'Contract', 'ContractOption']
kc2_query = ['gdq_source', 'gdq_target']

config_file_path, dag_id, dag_default_args, schedule_interval, tags, is_paused_upon_creation, sp_dict, default_region, \
baseline_domain_list_finwd, derived_domain_list = \
getDagParams(project_name, "tags", source_system, batch_frequency, domain_name)

dag_name = dag_id.upper() + '.' + source_system.upper() + '-GDQ-' + batch_frequency.upper()
tags.append('gdq')

with DAG(
    dag_id=dag_name,
    default_args=dag_default_args,
    schedule_interval=schedule_interval,
    is_paused_upon_creation=is_paused_upon_creation,
    tags=tags
) as dag:

    start = create_python_operator(
        dag=dag,
        task_name="start",
        op_kwargs={
            'config_file_path': config_file_path,
            'source_system': source_system
        },
        python_callable=get_finwd_var,
        trigger_rule='all_success',
        retries=0
    )

    end = DummyOperator(
        task_id='end',
        dag=dag,
        trigger_rule='all_success'
    )

    task_email_notify = create_python_operator(
        dag=dag,
        task_name="TASK_Send_Email",
        op_kwargs={
            'source_system_name': "N/A",
            'domain_name': "FINWD",
            'batch_frequency': "N/A",
            'environment': "{{ task_instance.xcom_pull(task_ids='start', key='env') }}",
            'job_name': "{{ task_instance.xcom_pull(task_ids='start', key='job_name') }}",
            'odate': "{{ task_instance.xcom_pull(task_ids='start', key='odate') }}"
        },
        python_callable=notify_email,
        trigger_rule='all_done',
        retries=0
    )

    final_status = PythonOperator(
        task_id='finally',
        dag=dag,
        provide_context=True,
        python_callable=final_status,
        trigger_rule='all_done',
        retries=0
    )

    load_current_batch_gdq = create_python_operator(
        dag=dag,
        task_name="load_current_batch_gdq",
        op_kwargs={
            "emr_name": emr_name,
            "step_name": f"GDQ Current Batch {source_system}",
            "command": "sh /application/financedw/financemasterdatastore/scripts/di_gdq/BaNCS/run_currentbatchid.sh"
        },
        python_callable=emr_step_submit,
        trigger_rule='all_success'
    )

    load_completed_batch_gdq = create_python_operator(
        dag=dag,
        task_name="load_completed_batch_gdq",
        op_kwargs={
            "emr_name": emr_name,
            "step_name": f"GDQ Completed Batch {source_system}",
            "command": "sh /application/financedw/financemasterdatastore/scripts/di_gdq/BaNCS/run_gdqcomplete.sh",
            "job_name": "{{ task_instance.xcom_pull(task_ids='start', key='job_name') }}"
        },
        python_callable=emr_step_submit,
        trigger_rule='all_success'
    )

    trigger_sweep_job = TriggerDagRunOperator(
        task_id='trigger_sweep_job',
        trigger_dag_id=f'TA_INDIVIDUAL-FINWD-{project_environment_name}-{project_name}-SHEEPJOB-DAILY'.upper(),
        conf={
            "env": "{{ task_instance.xcom_pull(task_ids='start', key='env') }}",
            "odate": "{{ task_instance.xcom_pull(task_ids='start', key='odate') }}",
            "job_name": "{{ task_instance.xcom_pull(task_ids='start', key='job_name') }}",
            "variable": 1,
            "hop_name": "gdq",
            "source_system": "bancs",
            "project": "{project_name}"
        },
        wait_for_completion=True,
        retries=0
    )

    with TaskGroup(group_id="gdq") as gdq_tg:
        for domain in baseline_domain_list_finwd:
            load_gdq_domain = create_python_operator(
                dag=dag,
                task_name="load_gdq_" + domain,
                op_kwargs={
                    "emr_name": emr_name,
                    "step_name": f"GDQ Load {source_system} {domain}",
                    "command": f"sh /application/financedw/financemasterdatastore/scripts/di_gdq/BaNCS/run_gdqscript.sh {domain} "
                               "{{ task_instance.xcom_pull(task_ids='start', key='job_name') }} "
                               "{{ task_instance.xcom_pull(task_ids='start', key='rerun_flag') }}"
                },
                python_callable=emr_step_submit,
                trigger_rule='all_success',
                retries=0
            )

    with TaskGroup(group_id="kc2") as gdq_kc2_tg:
        for source in kc2_source:
            for query in kc2_query:
                load_gdq_kc2 = create_python_operator(
                    dag=dag,
                    task_name="kc2_" + source + '_' + query,
                    op_kwargs={
                        "emr_name": emr_name,
                        "step_name": f"GDQ KC2 {source_system} {source} {query}",
                        "command": f"sh /application/financedw/financecontrols/scripts/KC2_INSERT_RUN.sh {source} {query} "
                                   "{{ task_instance.xcom_pull(task_ids='start', key='env') }}"
                    },
                    python_callable=emr_step_submit,
                    trigger_rule='all_success'
                )

    start >> load_current_batch_gdq >> gdq_tg >> load_completed_batch_gdq >> gdq_kc2_tg >> end >> task_email_notify >> final_status
    load_completed_batch_gdq >> trigger_sweep_job >> end
