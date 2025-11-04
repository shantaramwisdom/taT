# Purpose: Airflow code to run S3 Parquet Merge Process.
# Creates pipeline named TA-INDIVIDUAL-FINWD-{project_name}-NYDFS-LTCG-EXPIREDCONTRACTS-REFRESH

# Configuration JSON: {"env": "(project_environment_name)", "project": "{project_name}", "odate": "YYYYMMDD"}

# -------------------- DEVELOPMENT LOG --------------------
# 05/09/2024 - Sandeep Thalla - FINANCEDW framework development

import os
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from finance.{project_name}.common_utils import *

base_dir, config_file_path, dag_id, dag_default_args, schedule_interval, default_region = common_var()

project_name = '{project_name}'
emr_name = 'maintenance'
is_paused_upon_creation = True
if project_name == 'financedw':
    is_paused_upon_creation = False

if schedule_interval.lower() == 'none':
    schedule_interval = None

with DAG(
    dag_id=dag_id.upper() + "-NYDFS-LTCG-EXPIREDCONTRACTS-REFRESH",
    default_args=dag_default_args,
    schedule_interval=schedule_interval,
    is_paused_upon_creation=is_paused_upon_creation,
    tags=['maintenance', 'ltcg', 'finance', '{project_name}']
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

    create_maintenance_cluster = create_python_operator_retries(
        dag=dag,
        task_name="create_maintenance_cluster",
        op_kwargs={
            "emr_name": emr_name,
            "action": "create"
        },
        python_callable=emr_actions,
        trigger_rule='all_success'
    )

    terminate_maintenance_cluster = create_python_operator(
        dag=dag,
        task_name="terminate_maintenance_cluster",
        op_kwargs={
            "emr_name": emr_name,
            "action": "terminate"
        },
        python_callable=emr_actions,
        trigger_rule='all_success'
    )

    final_status = PythonOperator(
        task_id='finally',
        dag=dag,
        provide_context=True,
        python_callable=final_status,
        trigger_rule='all_done',
        retries=0
    )

    ltcg_nydfs_onetime_load = create_python_operator(
        dag=dag,
        task_name="run_ltcg_nydfs_onetime_load",
        op_kwargs={
            "emr_name": emr_name,
            "step_name": "ltcg_nydfs_onetime_load",
            "command": "sh /application/financedw/curated/scripts/nydfs_ltcg_refresh.sh "
                       "{{ task_instance.xcom_pull(task_ids='start', key='env') }} "
                       "{{ task_instance.xcom_pull(task_ids='start', key='odate') }}"
        },
        python_callable=emr_step_submit,
        trigger_rule='all_success',
        retries=0
    )

    start >> create_maintenance_cluster >> ltcg_nydfs_onetime_load >> terminate_maintenance_cluster >> final_status