# Purpose: Airflow code to run Initial Load Job to load Finance Curated tables from Financedwdgd tables for Vantage, LTCG & LTCGHYBRID Systems.
# Creates pipeline named TA-INDIVIDUAL-FINWD-{project_name}-INITIAL-LOAD-CURATED
# Configuration JSON: {"env": "(project_environment_name)", "project": "{project_name}", "odate": "YYYYMMDD"}

# -------------------- DEVELOPMENT LOG --------------------
# 03/12/2024 - Sagar Sawant - Initial Development

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from finance.{project_name}.common_utils import *

base_dir, config_file_path, dag_id, dag_default_args, schedule_interval, default_region = common_var()

project_name = '{project_name}'
emr_name = 'annuities'
is_paused_upon_creation = True

if schedule_interval.lower() == 'none':
    schedule_interval = None

dict_ = {'annuities': 'load_curated_from_gdgdatastorage.sh', 'maintenance': 'load_curated_from_financedw.sh'}
emrs = list(dict_.keys())

with DAG(
    dag_id=dag_id.upper() + '-INITIAL-LOAD-CURATED',
    default_args=dag_default_args,
    schedule_interval=schedule_interval,
    is_paused_upon_creation=is_paused_upon_creation,
    tags=['maintenance', 'finance', '{project_name}']
) as dag:

    start = create_python_operator(
        dag=dag,
        task_name="start",
        op_kwargs={
            'config_file_path': config_file_path
        },
        python_callable=get_findw_var,
        trigger_rule='all_success',
        retries=0
    )

    task_email_notify = create_python_operator(
        dag=dag,
        task_name="TASK_Send_Email",
        op_kwargs={
            'source_system_name': "N/A",
            'domain_name': "FINWD",
            'batch_frequency': "N/A",
            'environment': "{{ task_instance.xcom_pull(task_ids='start', key='env') }}",
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

    for emr_name in emrs:
        srcipt = dict_[emr_name]
        create_cluster = create_python_operator_retries(
            dag=dag,
            task_name=f"create_{emr_name}_cluster",
            op_kwargs={
                "emr_name": emr_name,
                "action": "create"
            },
            python_callable=emr_actions,
            trigger_rule='all_success'
        )

        terminate_cluster = create_python_operator(
            dag=dag,
            task_name=f"terminate_{emr_name}_cluster",
            op_kwargs={
                "emr_name": emr_name,
                "action": "terminate"
            },
            python_callable=emr_actions,
            trigger_rule='all_success'
        )

        initial_load_curated = create_python_operator(
            dag=dag,
            task_name=f"initial_load_curated_in_{emr_name}",
            op_kwargs={
                "emr_name": emr_name,
                "step_name": "Initial Load Curated",
                "command": f"sh /application/financedw/curated/scripts/utilities/{srcipt}"
            },
            python_callable=emr_step_submit,
            trigger_rule='all_success'
        )

        start >> create_cluster >> initial_load_curated >> terminate_cluster >> task_email_notify >> final_status