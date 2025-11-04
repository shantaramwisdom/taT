# Purpose: Airflow code to orchestrate KC2 Bancs Quarterly Process.
# Config JSON = {"env": "(project_environment_name)", "project": "{project_name}", "odate": "YYYYMMDD", "job_name": "XXYY"}

# -------------------- DEVELOPMENT LOG --------------------
# 09/20/2023 - Prathush Premachandran - Initial Release -> BANCS Move to Redshift

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from finance.{project_name}.common_utils import *

source_system = 'BANCS'
domain_name = 'finwd'
project_name = '{project_name}'
batch_frequency = 'quarterly'
emr_name = 'curated'

config_file_path, dag_id, dag_default_args, schedule_interval, tags, is_paused_upon_creation, sp_dict, default_region, \
baseline_domain_list_finwd, derived_domain_list = \
getDagParams(project_name, "tags", source_system, batch_frequency, domain_name)

dag_name = dag_id.upper() + '.' + source_system.upper() + '-KC2-' + batch_frequency.upper()

tags.append('kc2')

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

    actuarial_kc2 = create_python_operator(
        dag=dag,
        task_name="actuarial_kc2",
        op_kwargs={
            "emr_name": emr_name,
            "step_name": f"fKC2 Quarterly Actuarial {source_system}",
            "command": f"sh /application/financedw/financecontrols/scripts/actuarial_kc2.sh start {batch_frequency} "
                       "{{ task_instance.xcom_pull(task_ids='start', key='odate') }}"
        },
        python_callable=emr_step_submit,
        trigger_rule='all_success'
    )

    start >> create_curated_cluster >> actuarial_kc2 >> end

    if '{project_environment_name}' in ['prd']:
        end >> task_email_notify >> final_status
    else:
        trigger_emr_terminator = TriggerDagRunOperator(
            task_id='trigger_curated_emr_terminator',
            trigger_dag_id=f'TA-INDIVIDUAL-FINWD-{project_environment_name}-{project_name}-EMR-TERMINATOR'.upper(),
            conf={
                "env": "{{ task_instance.xcom_pull(task_ids='start', key='env') }}",
                "project": "{{ task_instance.xcom_pull(task_ids='start', key='project') }}",
                "odate": "{{ task_instance.xcom_pull(task_ids='start', key='odate') }}",
                "emr_name": emr_name
            }
        )

        end >> trigger_emr_terminator >> task_email_notify >> final_status