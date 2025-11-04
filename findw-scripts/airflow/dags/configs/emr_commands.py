spark_commands = \
{
    'common_vars': {
        'spark_submit': '--deploy-mode client --executor-memory 45g --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.maxExecutors=30 --executor-cores 5 --driver-memory 30g --conf spark.port.maxRetries=50',
        'spark_submit_lower': '--deploy-mode client --master yarn --num-executors 20 --executor-cores 5 --executor-memory 20g --driver-memory 18g --conf spark.sql.broadcastTimeout=2000 --conf spark.sql.autoBroadcastJoinThreshold=50M --conf spark.executor.memoryOverhead=4g'
    },
    'curated': {
        'spark_scripts': {
            'current_batch': {
                'script_uri': '/application/financedw/curated/scripts/load_curated.py',
                'script_args': '"-f {source_system} -d {domain} -f {batch_frequency} -j {job_name} -g {curated_source}"'
            },
            'curated_load': {
                'script_uri': '/application/financedw/curated/scripts/load_curated.py',
                'script_args': '"-f {source_system} -d {domain} -f {batch_frequency} -j {job_name} -g {curated_source}"'
            },
            'completed_batch': {
                'script_uri': '/application/financedw/curated/scripts/load_curated.py',
                'script_args': '"-f {source_system} -d {domain} -f {batch_frequency} -j {job_name} -g {curated_source}"'
            }
        },
        'py_files': '"f:s3://ta-individual-findw-{env}-codedeployment/{project_name}/scripts/curated/scripts.zip"',
        'spark_submit': '--deploy-mode client --executor-memory 45g --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.maxExecutors=30 --executor-cores 5 --driver-memory 30g --conf spark.port.maxRetries=50',
        'spark_submit_lower': '--deploy-mode client --master yarn --num-executors 20 --executor-cores 5 --executor-memory 20g --driver-memory 18g --conf spark.sql.broadcastTimeout=2000 --conf spark.sql.autoBroadcastJoinThreshold=50M --conf spark.executor.memoryOverhead=4g'
    },
    'shell_scripts': {
        'current_batch': {
            'script_uri': '/application/financedw/curated/scripts/load_curated.sh',
            'script_args': '"-f -e {env} -p {project} -s {source_system} -d {domain} -f {batch_frequency} -j {job_name} -sf {curated_source}"'
        },
        'curated_load': {
            'script_uri': '/application/financedw/curated/scripts/load_curated.sh',
            'script_args': '"-f -e {env} -p {project} -s {source_system} -d {domain} -f {batch_frequency} -j {job_name} -sf {curated_source}"'
        },
        'completed_batch': {
            'script_uri': '/application/financedw/curated/scripts/load_curated.sh',
            'script_args': '"-f -e {env} -p {project} -s {source_system} -d {domain} -f {batch_frequency} -j {job_name} -sf {curated_source}"'
        }
    }
}
