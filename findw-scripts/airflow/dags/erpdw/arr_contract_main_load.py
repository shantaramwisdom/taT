"""
Purpose: Airflow code to orchestrate Curated and FINDW process. Creates EMR and submits steps to it to process data in Curated layer.
Run FINDW Load Procedures in Redshift
Config JSON = {"env": "{project_environment_name}", "project": "{project_name}", "odate": "YYMMDD", "job_name": "XXYY"}
----------------DEVELOPMENT LOG----------------
08/28 - Md Jabbar - DAG development for ARR load
"""

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from finance.{project_name}.common_utils import *

domain_name = 'erpdw'
dag_ss_name = 'arr_contract'
source_system = 'arr'
project_name = '{project_name}'
batch_frequency = 'monthly'
emr_name = 'curated'
emr_source_system = domain_name
application = domain_name
arr_domain = 'contract_information'

config_file_path, dag_id, dag_default_args, schedule_interval, tags, is_paused_upon_creation, sp_dict, default_region, \
baseline_domain_list_findw, derived_domain_list, shared_emr_list = \
getDagParams(project_name, "tags", dag_ss_name, batch_frequency, domain_name, application)

dag_name = dag_id.upper() + '.' + source_system.upper() + '-MAIN-' + batch_frequency.upper()
tags.append('main')
tags.append('ERP')
tags.append(source_system)
tags.remove(dag_ss_name)

with DAG(
    dag_id=dag_name,
    default_args=dag_default_args,
    schedule=schedule_interval,
    is_paused_upon_creation=is_paused_upon_creation,
    tags=tags
) as dag:

    start = create_python_operator(
        dag=dag,
        task_name="start",
        op_kwargs={
            'config_file_path': config_file_path,
            'source_system': source_system,
        },
        python_callable=get_findw_var,
        trigger_rule="all_success",
        retries=0
    )

    end_curated = EmptyOperator(
        task_id='end_curated',
        dag=dag,
        trigger_rule='all_success'
    )

    end_findw = EmptyOperator(
        task_id='end_findw',
        dag=dag,
        trigger_rule='none_failed'
    )

    task_email_notify = create_python_operator(
        dag=dag,
        task_name="TASK_Send_Email",
        op_kwargs={
            'source_system_name': "N/A",
            'domain_name': "FINDW",
            'batch_frequency': "N/A",
            'application': application,
            'environment': "{{ task_instance.xcom_pull(task_ids='start', key='env') }}",
            'job_name': "{{ task_instance.xcom_pull(task_ids='start', key='job_name') }}",
            'odate': "{{ task_instance.xcom_pull(task_ids='start', key='odate') }}"
        },
        python_callable=notify_email,
        trigger_rule="one_failed",
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
        task_name="create_" + emr_name + "_cluster",
        op_kwargs={
            "emr_name": emr_name,
            "source_system_name": emr_source_system,
            "action": "create"
        },
        python_callable=emr_actions,
        trigger_rule="all_success"
    )

    trigger_emr_terminator = TriggerDagRunOperator(
        task_id='trigger_curated_emr_terminator',
        trigger_dag_id='TA-INDIVIDUAL-FINDW-{project_environment_name}-{project_name}-ERP-DW-EMR-TERMINATOR'.upper(),
        conf={
            "env": "{{ task_instance.xcom_pull(task_ids='start', key='env') }}",
            "project": "{{ task_instance.xcom_pull(task_ids='start', key='project') }}",
            "odate": "{{ task_instance.xcom_pull(task_ids='start', key='odate') }}"
        },
        trigger_rule="all_success"
    )

    branch_object_check_task = BranchPythonOperator(
        task_id='check_arr_batches',
        python_callable=check_new_arr_batches,
        op_kwargs={
            "source_system_name": source_system,
            "batch_frequency": batch_frequency.upper(),
            "arr_domain": arr_domain,
            "domain_list": baseline_domain_list_findw
        }
    )

    notify_skipped_load = create_python_operator(
        dag=dag,
        task_name="notify_skipped_load",
        op_kwargs={
            "source_system_name": source_system,
            "domain_name": "FINDW",
            "batch_frequency": "N/A",
            "application": application,
            "environment": "{{ task_instance.xcom_pull(task_ids='start', key='env') }}",
            "odate": "{{ task_instance.xcom_pull(task_ids='start', key='odate') }}"
        },
        python_callable=notify_skipped_load,
        trigger_rule="all_success",
        retries=0
    )

    def generate_task(source_system, domain, emr_name, is_derived=False):
        ''' returns python operator for curated domain and stored procedure task group for base/derived domains'''
        if not is_derived:
            with TaskGroup(group_id="curated_" + domain) as curated_tg:
                curated_currentbatch = create_python_operator(
                    dag=dag,
                    task_name='curated_currentbatch_' + source_system + '_' + domain,
                    op_kwargs={
                        "emr_name": emr_name,
                        "step_name": f"Curated Current Batch {source_system} {domain}",
                        "source_system_name": emr_source_system,
                        "command": "sh /application/financedw/curated/scripts/load_curated.sh "
                                   f"-s {source_system} -d currentbatch({domain}) -f {batch_frequency.upper()} -g A "
                                   "-j {{ task_instance.xcom_pull(task_ids='start', key='job_name') }}"
                    },
                    python_callable=emr_step_submit,
                    trigger_rule="all_success"
                )

                curated = create_python_operator(
                    dag=dag,
                    task_name='curated_' + source_system + '_' + domain,
                    op_kwargs={
                        "emr_name": emr_name,
                        "step_name": f"Curated Load {source_system} {domain}",
                        "source_system_name": emr_source_system,
                        "command": "sh /application/financedw/curated/scripts/load_curated.sh "
                                   f"-s {source_system} -d {domain} -f {batch_frequency.upper()} -g A "
                                   "-j {{ task_instance.xcom_pull(task_ids='start', key='job_name') }}"
                    },
                    python_callable=emr_step_submit,
                    trigger_rule="all_success"
                )

                curated_completedbatch = create_python_operator(
                    dag=dag,
                    task_name='curated_completedbatch_' + source_system + '_' + domain,
                    op_kwargs={
                        "emr_name": emr_name,
                        "step_name": f"Curated Completed Batch {source_system} {domain}",
                        "source_system_name": emr_source_system,
                        "command": "sh /application/financedw/curated/scripts/load_curated.sh "
                                   f"-s {source_system} -d completedbatch({domain}) -f {batch_frequency.upper()} -g A "
                                   "-j {{ task_instance.xcom_pull(task_ids='start', key='job_name') }}"
                    },
                    python_callable=emr_step_submit,
                    trigger_rule="all_success"
                )

                curated_currentbatch >> curated >> curated_completedbatch
            with TaskGroup(group_id="findw_" + domain) as findw_sp_tg:
                currentbatch_sp = create_python_func(
                    hop_name='currentbatch',
                    batch_frequency=batch_frequency.upper(),
                    load_type='baseline',
                    domain=domain,
                    task_name='currentbatch_' + source_system + '_' + domain,
                    dag=dag,
                    sp_dict=sp_dict,
                    source_system=source_system
                )

                staging_sp = create_python_func(
                    hop_name='load_staging',
                    load_type='baseline',
                    domain=domain,
                    task_name='staging_' + source_system + '_' + domain,
                    dag=dag,
                    sp_dict=sp_dict,
                    source_system=source_system
                )

                working_sp = create_python_func(
                    hop_name='working',
                    load_type='baseline',
                    domain=domain,
                    task_name='working_' + source_system + '_' + domain,
                    dag=dag,
                    sp_dict=sp_dict,
                    source_system=source_system
                )

                findw_sp = create_python_func(
                    hop_name='findw',
                    load_type='baseline',
                    domain=domain,
                    task_name='findw_' + source_system + '_' + domain,
                    dag=dag,
                    sp_dict=sp_dict,
                    source_system=source_system
                )

                currentbatch_sp >> staging_sp >> working_sp >> findw_sp
            return curated_tg, findw_sp_tg

    end_curated >> trigger_emr_terminator >> end_findw >> task_email_notify >> final_status
    for domain in baseline_domain_list_findw:
        if domain == 'contractplancodeetails':
            curated_plancode_tg, findw_plancode_tg = generate_task(source_system, domain, emr_name)
        elif domain == 'contractreinsurance treatycompidassignment':
            curated_compid_tg, findw_compid_tg = generate_task(source_system, domain, emr_name)

    start >> branch_object_check_task >> [create_curated_cluster, notify_skipped_load]
    notify_skipped_load >> task_email_notify
    create_curated_cluster >> curated_plancode_tg >> [curated_compid_tg, findw_plancode_tg]
    [curated_compid_tg, curated_plancode_tg] >> end_curated
    [findw_plancode_tg, findw_compid_tg] >> end_findw