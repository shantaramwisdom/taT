spark_commands = \
{
    'common_vars': {
        'spark_params': '--conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.maxExecutors=30',
    },
    'curated': {
        'sparkSubmitParametersOvrd': None,
        'spark_scripts': {
            'current_batch': {
                'entryPoint': 'load_curated.py',
                'entryPointArguments': '"""-f -s {source_system} -d currentbatch_{domain} -f {batch_frequency} -j {ctrlM_job_name} -sf {kwargs[\'src_flag\']} -rs Y -r N"""'
            },
            'curated_load': {
                'entryPoint': 'load_curated.py',
                'entryPointArguments': '"""-f -s {source_system} -d {domain} -f {batch_frequency} -j {ctrlM_job_name} -sf {kwargs[\'rerun_flag\']}"""'
            },
            'completed_batch': {
                'entryPoint': 'load_curated.py',
                'entryPointArguments': '"""-f -s {source_system} -d completedbatch_{domain} -f {batch_frequency} -j {ctrlM_job_name} -sf {kwargs[\'src_flag\']} -rs Y -r N"""'
            }
        }
    }
}
