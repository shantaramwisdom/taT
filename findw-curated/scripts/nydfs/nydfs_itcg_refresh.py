from pyspark.sql import SparkSession
from datetime import datetime
from dateutil.relativedelta import relativedelta
import sys
import subprocess
# Initialize Spark session
spark = SparkSession.builder\
    .appName("Refresh expiredcontract hive table")\
    .enableHiveSupport()\
    .getOrCreate()

env = (sys.argv[1])
cycle_date = (sys.argv[2])
cycle_date = datetime.strptime(cycle_date, "%Y-%m-%d").strftime("%Y%m%d")
formatted_cycledate = str(cycle_date).replace('-', '')
two_year_from_cycle_date = datetime.strptime(datetime.strptime(cycle_date, "%Y-%m-%d").replace(month=1, day=1) - relativedelta(years=2).strftime("%Y-%m-%d"), "%Y%m%d").strftime("%Y-%m-%d")
seven_year_from_cycle_date = datetime.strptime(datetime.strptime(cycle_date, "%Y-%m-%d").replace(month=1, day=1) - relativedelta(years=7).strftime("%Y-%m-%d"), "%Y%m%d").strftime("%Y-%m-%d")
curated_database = f'ta_individual_find_{env}.financecurated'
validation_query = f"""select source_system, domain_name, cycle_date, count(*) from {curated_database}.expiredcontract group by source_system, domain_name, cycle_date"""
print("----------------Count of complete records before the refresh grouped by sourcesystem, domain and cycledate----------------")
df = spark.sql(validation_query)
df.show(n = df.count(), truncate=False)
source_path = f's3://ta-individual-find-{env}-finance-curated/finances/orchestration/expiredcontract/'
destination_path = f's3://ta-individual-find-{env}-extracts/finances/orchestration/expiredcontract/'
command = f'aws s3 sync --only-show-errors --delete {source_path} {destination_path}'
try:
    result = subprocess.run(command, shell = True, capture_output = True, text = True)
    print(f"Backup completed successfully from {source_path} to {destination_path}")
    df = spark.read.parquet(f"s3a://ta-individual-find-{env}-extracts/finances/orchestration/expiredcontract/")
    df.createOrReplaceTempView("expired_backup")
    if result.stdout:
        print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"Error occurred during sync")
except Exception as s:
    print(s)
    sys.exit(1)

try:
    for sourcesystem in ['itga', 'itgcbyheard']:
        print(f"Performing refresh for {sourcesystem}")
        source_database = f'ta_individual_datalake_{env}.{sourcesystem}_curated'
        print(f"Source Database is {source_database}")
        valid_expired_policies_query = f"""
        with expired as (
            select policy_no, policy_id
            from {source_database}.policy_effective_history_daily_formatted_cycledate
            where cast('{cycle_date}' as date) between cast(view_row_create_day as date)
            and cast(view_row_obsolete_day as date)
            and cast('{cycle_date}' as date) between cast(view_row_effective_day as date)
            and cast(view_row_expiration_day as date)
            and (
                cast(policy_status_dt as date) < cast('{two_year_from_cycle_date}' as date)
                and (
                    policy_status_cd = '185'
                    and cast(coverage_expiration_dt as date) < cast('{seven_year_from_cycle_date}' as date)
                or
                    policy_status_cd = '186'
                    and cast(original_effective_dt as date) < cast('{seven_year_from_cycle_date}' as date)
                )
            )
        ),
        not_expired as (
            select policy_no, policy_id
            from {source_database}.policy_effective_history_daily_formatted_cycledate
            where cast('{cycle_date}' as date) between cast(view_row_create_day as date)
            and cast(view_row_obsolete_day as date)
            and cast('{cycle_date}' as date) between cast(view_row_effective_day as date)
            and cast(view_row_expiration_day as date)
            and (
                policy_status_cd in ('164')
                or (
                    policy_status_cd = '185'
                    and cast(policy_status_dt as date) >= cast('{two_year_from_cycle_date}' as date)
                )
                or (
                    policy_status_cd = '186'
                    and cast(policy_status_dt as date) >= cast('{two_year_from_cycle_date}' as date)
                )
            )
        ),
        linkage as (
            select prim_policy_id,
                   sec_policy_id
            from {source_database}.policy_linkage_effective_history_daily_formatted_cycledate
            where cast('{cycle_date}' as date) between cast(view_row_create_day as date)
            and cast(view_row_obsolete_day as date)
            and cast('{cycle_date}' as date) between cast(view_row_effective_day as date)
            and cast(view_row_expiration_day as date)
            group by 1,2
        ),
        ignore_linkage as (
            select policy_no,
                   prim_policy_id as policy_id,
                   sec_policy_id as related_policy_id
            from linkage
            join not_expired b on prim_policy_id = policy_id
            union
            select policy_no,
                   sec_policy_id as policy_id,
                   prim_policy_id as related_policy_id
            from linkage
            join not_expired on sec_policy_id = policy_id
        )
        select *
        from expired a
        left anti join ignore_linkage b on a.policy_id = b.related_policy_id
        """

        expired_count = spark.sql(valid_expired_policies_query).count()
        print(f"Count of expired records for {sourcesystem} for {cycle_date} is {expired_count}")
        spark.sql(valid_expired_policies_query).createOrReplaceTempView("policy_blacklist")
        print("Identifying affected partitions...")
        expired_table_prev_count = spark.sql(f"select * from {curated_database}.expiredcontract where source_system = '{sourcesystem}'").count()
        print(f"Count of expiredcontract table before refresh is {expired_table_prev_count}")
        affected_partitions = spark.sql(f"SELECT distinct cycle_date, domain_name, source_system FROM {curated_database}.expiredcontract ec where source_system = "
                                        f"'{sourcesystem}'").collect()
        print(f"Identified {len(affected_partitions)} affected partitions.")
        print("Overwriting affected partitions...")
        for partition in affected_partitions:
            cycleDate = partition['cycle_date']
            domain = partition['domain_name']
            source_system = partition['source_system']
            query = f"""
            ALTER TABLE {curated_database}.expiredcontract
            DROP IF EXISTS PARTITION (cycle_date='{cycleDate}', domain_name='{domain}', source_system='{source_system}')
            """
            spark.sql(query)
            spark.sql(f"""
            INSERT OVERWRITE TABLE {curated_database}.expiredcontract PARTITION (cycle_date='{cycleDate}', domain_name='{domain}', source_system='{source_system}')
            SELECT recorded_timestamp, policy_no, policy_id, batch_id
            FROM expired_backup
            WHERE cycle_date = '{cycleDate}'
            AND domain_name = '{domain}'
            AND source_system = '{source_system}'
            AND policy_no IN (SELECT policy_no FROM policy_blacklist)
            """)
        print("Completed overwriting affected partitions.")
        expired_table_after_count = spark.sql(f"select * from {curated_database}.expiredcontract where source_system = '{sourcesystem}'").count()
        print(f"Count of expiredcontract table after refresh is {expired_table_after_count}")
except Exception as e:
    print("error:", e)
    command = f'aws s3 sync --only-show-errors --delete {destination_path} {source_path}'
    result = subprocess.run(command, shell = True, capture_output = True, text = True)
    if result.stdout:
        print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"Error occurred during restoring data")
        sys.exit(1)
print("----------------Count of complete records after the refresh grouped by sourcesystem, domain and cycledate----------------")
df = spark.sql(validation_query)
df.show(n = df.count(), truncate=False)
# Stop Spark session
spark.stop()
