# Purpose: Airflow code to Load Controls Backup (from Oracle, Sweep Job Data) into Redshift.
# Config JSON = {"env": "(project_environment_name)", "project": "{project_name}", "odate": "YYYYMMDD"}

# -------------------- DEVELOPMENT LOG --------------------
# 04/30/2025 - Prathush Premachandran - Initial Release

from airflow.models import DAG
from finance.{project_name}.common_utils import *

base_dir, config_file_path, dag_id, dag_default_args, schedule_interval, default_region = common_var()

domain_name = 'findw'
project_name = '{project_name}'
tables_to_load = ['batch_tracking', 'dw_control_catalog', 'source_system_master', 'control_definitions', 'control_results', 'control_measures']

is_paused_upon_creation = True

if schedule_interval.lower() == 'none':
    schedule_interval = None

dag_name = dag_id.upper() + '-LOAD-CONTROLS-BACKUP'

with DAG(
    dag_id=dag_name,
    default_args=dag_default_args,
    schedule_interval=schedule_interval,
    is_paused_upon_creation=is_paused_upon_creation,
    tags=['finance', '{project_name}', 'maintenance']
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
            'domain_name': "CustoDML",
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

    for table in tables_to_load:
        load_table = create_python_operator(
            dag=dag,
            task_name=f"load_table_{table}",
            op_kwargs={
                'bucket_name': "ta-individual-findw-fileshare-{{ task_instance.xcom_pull(task_ids='start', key='env') }}",
                'file_key': f"dw_logging/{table.upper()}.txt",
                'table_name': f"dw_logging_bkup.{table}"
            },
            python_callable=load_s3_to_redshift,
            trigger_rule='all_success'
        )

    start >> load_table >> task_email_notify >> final_status