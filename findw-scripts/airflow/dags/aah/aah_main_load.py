# Purpose: Airflow code to orchestrate findw procedures.
# Conf: Name of the env in a dict(pass only when triggering manually) - {'env' : 'dev', ''},
# possible values are 'dev', 'tst0', 'mdl', 'prd'

# Config JSON = {"env": "{project_environment_name}","project": "{project_name}","odate":"YYYYMMDD"}

# -------------------------DEVELOPMENT LOG-------------------------
# 01/24/2022  Md Jabbar - Feedback Loop framework development
# 02/08/2022  Md Jabbar - Optimizations
# 03/28/2022  Md Jabbar - Added steps for Airflow email notifications
# ----------------------------------------------------------------

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from finance.{project_name}.common_utils import *

source_system = 'aah'
domain_name = 'aahjournalentrydetaileddomain'
sec_domain_name = 'findw'
project_name = '{project_name}'
batch_frequency = 'daily'

config_file_path, dag_id, dag_default_args, schedule_interval, tags, is_paused_upon_creation, sp_dict, default_region, \
baseline_domain_list_findw, derived_domain_list = \
getDagParams(project_name, "tags", source_system, batch_frequency, sec_domain_name)

tags.append('main')

with DAG(
    dag_id=dag_id.upper() + '_' + source_system.upper() + '-MAIN-' + batch_frequency.upper(),
    default_args=dag_default_args,
    schedule=schedule_interval,
    is_paused_upon_creation=is_paused_upon_creation,
    tags=tags
) as dag:
    start = create_python_operator(
        task_id='start',
        op_kwargs={
            'config_file_path': config_file_path,
            'source_system': source_system
        },
        python_callable=get_findw_var,
        retries=0
    )

    end = EmptyOperator(
        task_id='end_of_baseline_loads',
        dag=dag,
        trigger_rule='all_success'
    )

    task_email_notify = create_python_operator(
        dag=dag,
        task_name='TASK_SEND_EMAIL',
        op_kwargs={
            'source_system_name': 'N/A',
            'domain_name': 'FINDW',
            'batch_frequency': 'N/A',
            'environment': '{{ task_instance.xcom_pull(task_ids="start", key="env") }}',
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

    for domain in baseline_domain_list_findw:
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
            hop_name='working_append',
            load_type='baseline',
            domain=domain,
            task_name='working_' + source_system + '_' + domain,
            dag=dag,
            sp_dict=sp_dict,
            source_system=source_system
        )

        findw_sp = create_python_func(
            hop_name='findw_append',
            load_type='baseline',
            domain=domain,
            task_name='findw_' + source_system + '_' + domain,
            dag=dag,
            sp_dict=sp_dict,
            source_system=source_system
        )

        start >> currentbatch_sp >> staging_sp >> working_sp >> findw_sp >> end

    for domain in derived_domain_list:
        derived_currentbatch_sp = create_python_func(
            hop_name='currentbatch',
            batch_frequency=batch_frequency.upper(),
            load_type='baseline',
            domain=domain,
            task_name='currentbatch_' + source_system + '_' + domain,
            dag=dag,
            sp_dict=sp_dict,
            source_system=source_system
        )

        derived_staging_sp = create_python_func(
            hop_name='load_staging',
            load_type='baseline',
            domain=domain,
            task_name='staging_' + source_system + '_' + domain,
            dag=dag,
            sp_dict=sp_dict,
            source_system=source_system
        )

        derived_working_sp = create_python_func(
            hop_name='working_append',
            load_type='baseline',
            domain=domain,
            task_name='working_' + source_system + '_' + domain,
            dag=dag,
            sp_dict=sp_dict,
            source_system=source_system
        )

        derived_findw_sp = create_python_func(
            hop_name='findw_append',
            load_type='baseline',
            domain=domain,
            task_name='findw_' + source_system + '_' + domain,
            dag=dag,
            sp_dict=sp_dict,
            source_system=source_system
        )

        end >> derived_currentbatch_sp >> derived_staging_sp >> derived_working_sp >> derived_findw_sp >> task_email_notify >> final_status