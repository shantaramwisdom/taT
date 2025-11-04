"""
Purpose: Airflow code to orchestrate Curated and FINDW process. Creates EMR and submits steps to it to process data in Curated layer.
Run FINDW Load Procedures in Redshift
Config JSON - {"env": "{project_environment_name}", "project": "{project_name}", "odate": "YYMMDD", "job_name": "XXYY"}
----------------DEVELOPMENT LOG----------------
06/12/2023 - Sangram Patil - Curated framework development
"""
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from finance.{project_name}.common_utils import *
source_system = 'Bestow'
domain_name = 'findw'
project_name = '{project_name}'
batch_frequency = 'daily'
emr_name = 'curated'
emr_source_system = source_system.lower()
config_file_path, dag_id, dag_default_args, schedule_interval, tags, is_paused_upon_creation, sp_dict,\
default_region, baseline_domain_list_findw, derived_domain_list = \
getDagParams(project_name, "tags", source_system, batch_frequency, domain_name)
dag_name = dag_id.upper() + '-' + source_system.upper() + '-MAIN-' + batch_frequency.upper()
tags.append('main')
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
            'source_system': source_system
        },
        python_callable=get_findw_var,
        trigger_rule="all_success",
        retries=0
    )
    end_curated = EmptyOperator(
        task_id='end_curated',
        dag=dag,
        trigger_rule="all_success"
    )
    end_findw = EmptyOperator(
        task_id='end_findw',
        dag=dag,
        trigger_rule="none_failed"
    )
    task_email_notify = create_python_operator(
        dag=dag,
        task_name="TASK_Send_Email",
        op_kwargs={
            'source_system_name': 'N/A',
            'domain_name': 'FINMOD',
            'batch_frequency': 'N/A',
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
        trigger_rule="all_done",
        retries=0
    )
    create_curated_cluster = create_python_operator_retries(
        dag=dag,
        task_name="create_" + emr_name + "_cluster",
        op_kwargs={
            'emr_name': emr_name,
            'source_system_name': emr_source_system,
            'action': 'create'
        },
        python_callable=emr_actions,
        trigger_rule="all_success"
    )
    terminate_curated_cluster = create_python_operator(
        dag=dag,
        task_name="terminate_" + emr_name + "_cluster",
        op_kwargs={
            'emr_name': emr_name,
            'source_system_name': emr_source_system,
            'action': 'terminate'
        },
        python_callable=emr_actions,
        trigger_rule="all_success"
    )
    def generate_task(source_system, domain, emr_name, is_derived=False):
        '''returns python operator for curated domain and stored procedure task group for base/derived domains'''
        if not is_derived:
            with TaskGroup(group_id="curated_" + domain) as curated_tg:
                curated_currentbatch = create_python_operator(
                    dag=dag,
                    task_name="curated_currentbatch_" + source_system + "_" + domain,
                    op_kwargs={
                        'emr_name': emr_name,
                        'step_name': f"Curated Current Batch {source_system} {domain}",
                        'source_system_name': emr_source_system,
                        'command': "sh /application/financedw/curated/scripts/load_curated.sh "
                                   f"-s {source_system} -d currentbatch{domain} -f {batch_frequency.upper()} -g I "
                                   f"-j {{ task_instance.xcom_pull(task_ids='start', key='job_name') }}"
                    },
                    python_callable=emr_step_submit,
                    trigger_rule="all_success"
                )
                curated = create_python_operator(
                    dag=dag,
                    task_name="curated_" + source_system + "_" + domain,
                    op_kwargs={
                        'emr_name': emr_name,
                        'step_name': f"Curated Load {source_system} {domain}",
                        'source_system_name': emr_source_system,
                        'command': "sh /application/financedw/curated/scripts/load_curated.sh "
                                   f"-s {source_system} -d {domain} -f {batch_frequency.upper()} -g I "
                                   f"-j {{ task_instance.xcom_pull(task_ids='start', key='job_name') }}"
                    },
                    python_callable=emr_step_submit,
                    trigger_rule="all_success"
                )
                curated_completedbatch = create_python_operator(
                    dag=dag,
                    task_name="curated_completedbatch_" + source_system + "_" + domain,
                    op_kwargs={
                        'emr_name': emr_name,
                        'step_name': f"Curated Completed Batch {source_system} {domain}",
                        'source_system_name': emr_source_system,
                        'command': "sh /application/financedw/curated/scripts/load_curated.sh "
                                   f"-s {source_system} -d completedbatch{domain} -f {batch_frequency.upper()} -g I "
                                   f"-j {{ task_instance.xcom_pull(task_ids='start', key='job_name') }}"
                    },
                    python_callable=emr_step_submit,
                    trigger_rule="all_success"
                )
                start >> create_curated_cluster >> curated_currentbatch >> curated >> curated_completedbatch
        ovrd_work_hop_name = "working"
        ovrd_findw_hop_name = "findw"
        if domain == "financeactivity":
            ovrd_work_hop_name = "working_append"
            ovrd_findw_hop_name = "findw_append"
        with TaskGroup(group_id="findw_" + domain) as findw_sp_tg:
            currentbatch_sp = create_python_func(
                hop_name="currentbatch",
                batch_frequency=batch_frequency.upper(),
                load_type="baseline",
                domain=domain,
                task_name="currentbatch_" + source_system + "_" + domain,
                dag=dag,
                sp_dict=sp_dict,
                source_system=source_system
            )
            staging_sp = create_python_func(
                hop_name="load_staging",
                load_type="baseline",
                domain=domain,
                task_name="staging_" + source_system + "_" + domain,
                dag=dag,
                sp_dict=sp_dict,
                source_system=source_system
            )
                working_sp = create_python_func(
        hop_name=ovrd_work_hop_name,
        load_type='baseline',
        domain=domain,
        task_name='working_' + source_system + '_' + domain,
        dag=dag,
        sp_dict=sp_dict,
        source_system=source_system
    )
    findw_sp = create_python_func(
        hop_name=ovrd_findw_hop_name,
        load_type='baseline',
        domain=domain,
        task_name='findw_' + source_system + '_' + domain,
        dag=dag,
        sp_dict=sp_dict,
        source_system=source_system
    )
    currentbatch_sp >> staging_sp >> working_sp >> findw_sp
    if is_derived:
        return findw_sp_tg
    return curated_tg, findw_sp_tg
if ('{project_environment_name}') in ['tst'] and ('{project_name}') in ['financedwt5', 'financedwt6']) or ('{project_environment_name}') in ['mdl']:
    end_curated_findw = EmptyOperator(
        task_id='end_curated_findw',
        dag=dag,
        trigger_rule='all_success'
    )
    trigger_erpdw = TriggerDagRunOperator(
        task_id='trigger_erpdw',
        trigger_dag_id=f'TA-INDIVIDUAL-FINDW-({project_environment_name})-({project_name})-ERPDW-({source_system})-MAIN-DAILY'.upper(),
        conf={"env": "{{ task_instance.xcom_pull(task_ids='start', key='env') }}",
              "project": "{{ task_instance.xcom_pull(task_ids='start', key='project') }}",
              "odate": "{{ task_instance.xcom_pull(task_ids='start', key='odate') }}",
              "job_name": "{{ task_instance.xcom_pull(task_ids='start', key='job_name') }}",
              "rerun_flag": "N"},
        wait_for_completion=True,
        retries=0
    )
    end_findw >> task_email_notify
else:
    end_curated >> terminate_curated_cluster >> end_findw >> task_email_notify >> final_status
for domain in derived_domain_list:
    exec("{0}_deri_tg=generate_task(source_system, domain, emr_name, True)".format(str(domain)))
for domain in baseline_domain_list_findw:
    curated_tg, findw_base_tg = generate_task(source_system, domain, emr_name)
    if ('{project_environment_name}') in ['tst'] and ('{project_name}') in ['financedwt5', 'financedwt6']) or ('{project_environment_name}') in ['mdl']:
        if domain in ['contract', 'party', 'activity', 'partycontractrelationship']:
            findw_base_tg >> trigger_erpdw >> end_curated >> terminate_curated_cluster >> task_email_notify >> final_status
        create_curated_cluster >> (end_curated_findw, findw_base_tg)
        end_curated_findw >> end_curated
    else:
        create_curated_cluster >> curated_tg >> (end_curated, findw_base_tg)
        if domain in ['contract', 'party', 'partycontractrelationship']:
            findw_base_tg >> financecontract_deri_tg >> end_findw
        else:
            findw_base_tg >> end_findw
