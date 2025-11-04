# Purpose: Airflow code to orchestrate findw procedures for BANCS Redshift Loads.
# Config JSON = {"env": "(project_environment_name)", "project": "{project_name}", "odate": "YYYYMMDD"}

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
groups = []

config_file_path, dag_id, dag_default_args, schedule_interval, tags, is_paused_upon_creation, sp_dict, default_region, \
baseline_domain_list_finwd, derived_domain_list = \
getDagParams(project_name, "tags", source_system, batch_frequency, domain_name)

dag_name = dag_id.upper() + '.' + source_system.upper() + '-MAIN-' + batch_frequency.upper()

tags.append('main')

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

    finance_activity_controls_load_ingestion = create_python_operator(
        dag=dag,
        task_name="ingestion_controls_load",
        op_kwargs={
            "emr_name": emr_name,
            "step_name": "BaNCS Ingestion Controls Activity",
            "command": "sh /application/financedw/financecontrols/scripts/BaNCS_INGESTION_CONTROLS_LOAD.sh activity"
        },
        python_callable=emr_step_submit,
        trigger_rule='all_success'
    )

    finance_activity_controls_load_consumption = create_python_operator(
        dag=dag,
        task_name="consumption_controls_load",
        op_kwargs={
            "emr_name": emr_name,
            "step_name": "BaNCS Final Controls Activity",
            "command": "sh /application/financedw/financecontrols/scripts/BaNCS_CV_and_AUDIT_CONTROLS_LOAD.sh activity"
        },
        python_callable=emr_step_submit,
        trigger_rule='all_success'
    )

    start >> create_curated_cluster >> finance_activity_controls_load_ingestion >> finance_activity_controls_load_consumption >> end

    with TaskGroup(group_id="kc2") as findw_kc2_tg:
        kc2_controls = create_python_operator(
            dag=dag,
            task_name="kc2_controls",
            op_kwargs={
                "emr_name": emr_name,
                "step_name": "BaNCS Kc2 Controls Final",
                "command": "sh /application/financedw/financecontrols/scripts/bancs_kc2_controls.sh"
            },
            python_callable=emr_step_submit,
            trigger_rule='all_success'
        )

        kc2_recon = create_python_operator(
            dag=dag,
            task_name="kc2_recon",
            op_kwargs={
                "emr_name": emr_name,
                "step_name": "BaNCS Kc2 Recon Final",
                "command": "sh /application/financedw/financecontrols/scripts/KC2_BANCS_RECON_RUN.sh "
                           "{{ task_instance.xcom_pull(task_ids='start', key='env') }}"
            },
            python_callable=emr_step_submit,
            trigger_rule='all_success'
        )

        kc2_alerts = create_python_operator(
            dag=dag,
            task_name="kc2_alerts",
            op_kwargs={
                "emr_name": emr_name,
                "step_name": "BaNCS Kc2 Alerts Final",
                "command": "sh /application/financedw/financecontrols/scripts/BANCSKC2Alerts_Daily.sh "
                           "{{ task_instance.xcom_pull(task_ids='start', key='env') }}"
            },
            python_callable=emr_step_submit,
            retries=0,
            trigger_rule='all_success'
        )

        for kc2 in kc2_source:
            kc2_findw = create_python_operator(
                dag=dag,
                task_name="kc2_" + kc2,
                op_kwargs={
                    "emr_name": emr_name,
                    "step_name": f"BaNCS Daily Kc2 Final {kc2}",
                    "command": f"sh /application/financedw/financecontrols/scripts/KC2_INSERT_RUN.sh {kc2} consumption_gt "
                               "{{ task_instance.xcom_pull(task_ids='start', key='env') }}"
                },
                python_callable=emr_step_submit,
                trigger_rule='all_success'
            )

            kc2_controls >> kc2_findw >> kc2_recon

        kc2_recon >> kc2_alerts >> end
        create_curated_cluster >> findw_kc2_tg

    for domain in derived_domain_list:
        ovrd_findw_hop_name = 'working'
        ovrd_work_hop_name = ''
        if domain in 'financeactivity':
            ovrd_work_hop_name = 'working_append'
            ovrd_findw_hop_name = 'findw_append'

        with TaskGroup(group_id="findw_derived_" + domain) as findw_derived_tg:
            derived_currentbatch_sp = create_python_func(
                hop_name='currentbatch',
                batch_frequency=batch_frequency.upper(),
                load_type='baseline',
                domain=domain,
                task_name='currentbatch_sp_for_' + source_system + '_' + domain,
                dag=dag,
                sp_dict=sp_dict,
                source_system=source_system
            )

        derived_staging_sp = create_python_func(
            hop_name='load_staging',
            load_type='baseline',
            domain=domain,
            task_name='staging_sp_for_' + source_system + '_' + domain,
            dag=dag,
            sp_dict=sp_dict,
            source_system=source_system
        )

        derived_working_sp = create_python_func(
            hop_name=ovrd_work_hop_name,
            load_type='baseline',
            domain=domain,
            task_name='working_sp_for_' + source_system + '_' + domain,
            dag=dag,
            sp_dict=sp_dict,
            source_system=source_system
        )

        derived_findw_sp = create_python_func(
            hop_name=ovrd_findw_hop_name,
            load_type='baseline',
            domain=domain,
            task_name='findw_sp_for_' + source_system + '_' + domain,
            dag=dag,
            sp_dict=sp_dict,
            source_system=source_system
        )

        groups.append(findw_derived_tg)
        derived_currentbatch_sp >> derived_staging_sp >> derived_working_sp >> derived_findw_sp
        if domain == 'financeactivity':
            derived_findw_sp >> (findw_kc2_tg, finance_activity_controls_load_ingestion)
        else:
            derived_findw_sp >> end

    for domain in baseline_domain_list_finwd:
        ovrd_work_hop_name = 'working'
        ovrd_findw_hop_name = 'findw'

        with TaskGroup(group_id="findw_" + domain) as findw_base_tg:
            currentbatch_sp = create_python_func(
                hop_name='currentbatch',
                batch_frequency=batch_frequency.upper(),
                load_type='baseline',
                domain=domain,
                task_name='currentbatch_sp_for_' + source_system + '_' + domain,
                dag=dag,
                sp_dict=sp_dict,
                source_system=source_system
            )

            transpose_sp = create_python_func(
                hop_name='transpose',
                load_type='baseline',
                domain=domain,
                task_name='transpose_sp_for_' + source_system + '_' + domain,
                dag=dag,
                sp_dict=sp_dict,
                source_system=source_system
            )

            staging_sp = create_python_func(
                hop_name='staging',
                load_type='baseline',
                domain=domain,
                task_name='staging_sp_for_' + source_system + '_' + domain,
                dag=dag,
                sp_dict=sp_dict,
                source_system=source_system
            )

            working_sp = create_python_func(
                hop_name=ovrd_work_hop_name,
                load_type='baseline',
                domain=domain,
                task_name='working_sp_for_' + source_system + '_' + domain,
                dag=dag,
                sp_dict=sp_dict,
                source_system=source_system
            )

            findw_sp = create_python_func(
                hop_name=ovrd_findw_hop_name,
                load_type='baseline',
                domain=domain,
                task_name='findw_sp_for_' + source_system + '_' + domain,
                dag=dag,
                sp_dict=sp_dict,
                source_system=source_system
            )

            currentbatch_sp >> transpose_sp >> staging_sp >> working_sp >> findw_sp

        if domain in ['contract', 'contractoption']:
            findw_base_tg >> findw_kc2_tg

        if domain not in ['contract', 'party', 'activity', 'contractoption']:
            findw_base_tg >> end
        elif domain == 'contract':
            findw_base_tg >> [groups[0], groups[2]]
        elif domain == 'activity':
            findw_base_tg >> groups[1]
        elif domain == 'party':
            findw_base_tg >> [groups[0], groups[2]]

    start >> findw_base_tg

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