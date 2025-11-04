# Purpose: Airflow code to orchestrate Ingestion BANCS Load.
# Config JSON = {"env": "(project_environment_name)", "project": "{project_name}", "odate": "YYYYMMDD", "job_name": "XXYY", "load_type": "delta/full"}

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
run_landing = ['wf_Bancs_Daily_Split', 'wf_Bancs_KC6_Controlfile_to_Landing', 'wf_Bancs_Source_Controls_Daily_Split']
new_fw = ['Daily_Split', 'Sourcecontrol_Split']
kc2_source = ['Activity', 'Contract', 'ContractOption']

config_file_path, dag_id, dag_default_args, schedule_interval, tags, is_paused_upon_creation, sp_dict, default_region, \
baseline_domain_list_finwd, derived_domain_list = \
getDagParams(project_name, "tags", source_system, batch_frequency, domain_name)

dag_name = dag_id.upper() + '.' + source_system.upper() + '-INGESTION-' + batch_frequency.upper()
tags.append('ingestion')

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

    create_curated_cluster = create_python_operator_retries(
        dag=dag,
        task_name="create_" + emr_name + "_cluster",
        op_kwargs={
            "emr_name": emr_name,
            "action": "create"
        },
        python_callable=emr_actions,
        trigger_rule='all_success'
    )

    kc6 = create_python_operator(
        dag=dag,
        task_name="kc6",
        op_kwargs={
            "emr_name": emr_name,
            "step_name": f"fKC6 Ingestion {source_system}",
            "command": "sh /application/financedw/financecontrols/scripts/kc6_run.sh"
        },
        python_callable=emr_step_submit,
        trigger_rule='all_success'
    )

    dc_completed_batch = create_python_operator(
        dag=dag,
        task_name="dc_completed_batch",
        op_kwargs={
            "emr_name": emr_name,
            "step_name": f"fDatastage Completed Batch {source_system}",
            "command": f"sh /application/financedw/financemasterdatastore/scripts/run_dccompletedbatchidinfo.sh -s {source_system.lower()} -b {batch_frequency.upper()} "
                       "-c {{ task_instance.xcom_pull(task_ids='start', key='job_name') }}"
        },
        python_callable=emr_step_submit,
        trigger_rule='all_success'
    )

    dc_Daily_Split_Controls = create_python_operator(
        dag=dag,
        task_name="dc_Daily_Split_Control",
        op_kwargs={
            "emr_name": emr_name,
            "step_name": f"fDatastage New Framework {source_system} Controls",
            "command": f"sh /application/financedw/financedatastage/scripts/newfw_edc_control.sh -s {source_system.lower()} "
                       "-c {{ task_instance.xcom_pull(task_ids='start', key='load_type') }}"
        },
        python_callable=emr_step_submit,
        trigger_rule='all_success'
    )

    trigger_sweep_job = TriggerDagRunOperator(
        task_id='trigger_sweep_job',
        trigger_dag_id=f'TA-INDIVIDUAL-FINWD-{project_environment_name}-{project_name}-SHEEPJOB-DAILY'.upper(),
        conf={
            "env": "{{ task_instance.xcom_pull(task_ids='start', key='env') }}",
            "odate": "{{ task_instance.xcom_pull(task_ids='start', key='odate') }}",
            "job_name": "{{ task_instance.xcom_pull(task_ids='start', key='job_name') }}",
            "variable": "1",
            "hop_name": "edc",
            "source_system": "bancs",
            "project": "{project_name}"
        },
        wait_for_completion=True,
        retries=0
    )

    for run in run_landing:
        run_landing = create_python_operator(
            dag=dag,
            task_name="run_landing_" + run,
            op_kwargs={
                "emr_name": emr_name,
                "step_name": f"fDatastage Run Landing {source_system} {run}",
                "command": f"sh /application/financedw/financedatastage/scripts/run_landing_controls.sh -w {run} -s {source_system.lower()} "
                           "-c {{ task_instance.xcom_pull(task_ids='start', key='job_name') }}"
            },
            python_callable=emr_step_submit,
            trigger_rule='all_success'
        )

    start >> create_curated_cluster >> run_landing >> kc6

    for fw in new_fw:
        dc_new_fw = create_python_operator(
            dag=dag,
            task_name="dc_new_fw_" + fw,
            op_kwargs={
                "emr_name": emr_name,
                "step_name": f"fDatastage New Framework {source_system} {fw}",
                "command": f"sh /application/financedw/financedatastage/scripts/newfw_runtemplate.sh -s {fw} -o {source_system.lower()} "
                           "-p {{ task_instance.xcom_pull(task_ids='start', key='load_type') }}"
            },
            python_callable=emr_step_submit,
            trigger_rule='all_success'
        )

        kc6 >> dc_new_fw
        if fw == 'Daily_Split':
            dc_new_fw >> dc_Daily_Split_Controls
        else:
            dc_new_fw >> dc_completed_batch

    for kc2 in kc2_source:
        kc2_ingestion = create_python_operator(
            dag=dag,
            task_name="kc2_" + kc2,
            op_kwargs={
                "emr_name": emr_name,
                "step_name": f"fDatastage KC2 {source_system} {kc2}",
                "command": f"sh /application/financedw/financecontrols/scripts/KC2_INSERT_RUN.sh {kc2} landing.rs "
                           "{{ task_instance.xcom_pull(task_ids='start', key='env') }}"
            },
            python_callable=emr_step_submit,
            trigger_rule='all_success'
        )

        kc6 >> kc2_ingestion >> end

    dc_Daily_Split_Controls >> dc_completed_batch >> trigger_sweep_job >> end >> task_email_notify >> final_status