p65_gdq_daily_config=\
{
    'pre_flag': '"pre"',
    'post_flag': '"post"',
    'job_name': '"{{ task_instance.xcom_pull(task_ids=\'start\', key=\'job_name\') }}"',
    'prelogging_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdq_controllogging.sh "',
    'prelogging_params': '"-a {source_system} -w {tasknm} -f {pre_flag} -j {job_name}"',
    'postlogging_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdq_controllogging.sh "',
    'postlogging_params': '"-a {source_system} -w {tasknm} -f {post_flag} -j {job_name}"',
    'mapping_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdqdatainsert.sh "',
    'mapping_params': '"-a {source_system} -s {taskname} -j {job_name}"'
}

p65_gdq_monthly_config=\
{
    'pre_flag': '"pre"',
    'post_flag': '"post"',
    'job_name': '"{{ task_instance.xcom_pull(task_ids=\'start\', key=\'job_name\') }}"',
    'prelogging_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdq_controllogging.sh "',
    'prelogging_params': '"-a {source_system} -w {tasknm} -f {pre_flag} -j {job_name}"',
    'postlogging_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdq_controllogging.sh "',
    'postlogging_params': '"-a {source_system} -w {tasknm} -f {post_flag} -j {job_name}"',
    'mapping_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdqdatainsert.sh "',
    'mapping_params': '"-a {source_system} -s {taskname} -j {job_name}"'
}

vantageone_gdq_daily_config= \
{
    'pre_flag': '"pre"',
    'post_flag': '"post"',
    'job_name': '"{{ task_instance.xcom_pull(task_ids=\'start\', key=\'job_name\') }}"',
    'prelogging_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdq_controllogging.sh "',
    'prelogging_params': '"-a {source_system} -w {tasknm} -f {pre_flag} -j {job_name}"',
    'postlogging_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdq_controllogging.sh "',
    'postlogging_params': '"-a {source_system} -w {tasknm} -f {post_flag} -j {job_name}"',
    'ifrs_oracle_write_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdq_controllogging.sh "',
    'mapping_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdqdatainsert.sh "',
    'mapping_params': '"-a {source_system} -s {taskname} -j {job_name}"'
}

alm_gdq_quarterly_config=\
{
    'pre_flag': '"pre"',
    'post_flag': '"post"',
    'job_name': '"{{ task_instance.xcom_pull(task_ids=\'start\', key=\'job_name\') }}"',
    'prelogging_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdq_controllogging.sh "',
    'prelogging_params': '"-a {source_system} -w {tasknm} -f {pre_flag} -j {job_name}"',
    'postlogging_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdq_controllogging.sh "',
    'postlogging_params': '"-a {source_system} -w {tasknm} -f {post_flag} -j {job_name}"',
    'mapping_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdqdatainsert.sh "',
    'mapping_params': '"-a {source_system} -s {taskname} -j {job_name}"'
}

p6_gdq_daily_config=\
{
    'pre_flag': '"pre"',
    'post_flag': '"post"',
    'job_name': '"{{ task_instance.xcom_pull(task_ids=\'start\', key=\'job_name\') }}"',
    'prelogging_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdq_controllogging.sh "',
    'prelogging_params': '"-a {source_system} -w {tasknm} -f {pre_flag} -j {job_name}"',
    'postlogging_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdq_controllogging.sh "',
    'postlogging_params': '"-a {source_system} -w {tasknm} -f {post_flag} -j {job_name}"',
    'mapping_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdqdatainsert.sh "',
    'mapping_params': '"-a {source_system} -s {taskname} -j {job_name}"'
}

p6_gdq_monthly_config=\
{
    'pre_flag': '"pre"',
    'post_flag': '"post"',
    'job_name': '"{{ task_instance.xcom_pull(task_ids=\'start\', key=\'job_name\') }}"',
    'prelogging_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdq_controllogging.sh "',
    'prelogging_params': '"-a {source_system} -w {tasknm} -f {pre_flag} -j {job_name}"',
    'postlogging_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdq_controllogging.sh "',
    'postlogging_params': '"-a {source_system} -w {tasknm} -f {post_flag} -j {job_name}"',
    'mapping_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdqdatainsert.sh "',
    'mapping_params': '"-a {source_system} -s {taskname} -j {job_name}"'
}

sp1_gdq_daily_config=\
{
    'pre_flag': '"pre"',
    'post_flag': '"post"',
    'job_name': '"{{ task_instance.xcom_pull(task_ids=\'start\', key=\'job_name\') }}"',
    'prelogging_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdq_controllogging.sh "',
    'prelogging_params': '"-a {source_system} -w {tasknm} -f {pre_flag} -j {job_name}"',
    'postlogging_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdq_controllogging.sh "',
    'postlogging_params': '"-a {source_system} -w {tasknm} -f {post_flag} -j {job_name}"',
    'mapping_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdqdatainsert.sh "',
    'mapping_params': '"-a {source_system} -s {taskname} -j {job_name}"'
}

sp1_gdq_monthly_config=\
{
    'pre_flag': '"pre"',
    'post_flag': '"post"',
    'job_name': '"{{ task_instance.xcom_pull(task_ids=\'start\', key=\'job_name\') }}"',
    'prelogging_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdq_controllogging.sh "',
    'prelogging_params': '"-a {source_system} -w {tasknm} -f {pre_flag} -j {job_name}"',
    'postlogging_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdq_controllogging.sh "',
    'postlogging_params': '"-a {source_system} -w {tasknm} -f {post_flag} -j {job_name}"',
    'mapping_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdqdatainsert.sh "',
    'mapping_params': '"-a {source_system} -s {taskname} -j {job_name}"'
}

p5_gdq_daily_config=\
{
    'pre_flag': '"pre"',
    'post_flag': '"post"',
    'job_name': '"{{ task_instance.xcom_pull(task_ids=\'start\', key=\'job_name\') }}"',
    'prelogging_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdq_controllogging.sh "',
    'prelogging_params': '"-a {source_system} -w {tasknm} -f {pre_flag} -j {job_name}"',
    'postlogging_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdq_controllogging.sh "',
    'postlogging_params': '"-a {source_system} -w {tasknm} -f {post_flag} -j {job_name}"',
    'mapping_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdqdatainsert.sh "',
    'mapping_params': '"-a {source_system} -s {taskname} -j {job_name}"'
}

p5_gdq_monthly_config=\
{
    'pre_flag': '"pre"',
    'post_flag': '"post"',
    'job_name': '"{{ task_instance.xcom_pull(task_ids=\'start\', key=\'job_name\') }}"',
    'prelogging_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdq_controllogging.sh "',
    'prelogging_params': '"-a {source_system} -w {tasknm} -f {pre_flag} -j {job_name}"',
    'postlogging_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdq_controllogging.sh "',
    'postlogging_params': '"-a {source_system} -w {tasknm} -f {post_flag} -j {job_name}"',
    'mapping_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdqdatainsert.sh "',
    'mapping_params': '"-a {source_system} -s {taskname} -j {job_name}"'
}

p75_gdq_daily_config=\
{
    'pre_flag': '"pre"',
    'post_flag': '"post"',
    'job_name': '"{{ task_instance.xcom_pull(task_ids=\'start\', key=\'job_name\') }}"',
    'prelogging_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdq_controllogging.sh "',
    'prelogging_params': '"-a {source_system} -w {tasknm} -f {pre_flag} -j {job_name}"',
    'postlogging_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdq_controllogging.sh "',
    'postlogging_params': '"-a {source_system} -w {tasknm} -f {post_flag} -j {job_name}"',
    'mapping_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdqdatainsert.sh "',
    'mapping_params': '"-a {source_system} -s {taskname} -j {job_name}"'
}

p75_gdq_monthly_config=\
{
    'pre_flag': '"pre"',
    'post_flag': '"post"',
    'job_name': '"{{ task_instance.xcom_pull(task_ids=\'start\', key=\'job_name\') }}"',
    'prelogging_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdq_controllogging.sh "',
    'prelogging_params': '"-a {source_system} -w {tasknm} -f {pre_flag} -j {job_name}"',
    'postlogging_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdq_controllogging.sh "',
    'postlogging_params': '"-a {source_system} -w {tasknm} -f {post_flag} -j {job_name}"',
    'mapping_command': '"sh /application/financedw/financemasterdatastore/scripts/di_gdq/Vantage/DataInsert/gdqdatainsert.sh "',
    'mapping_params': '"-a {source_system} -s {taskname} -j {job_name}"'
}
