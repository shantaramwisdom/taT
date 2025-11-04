####################################################################################################
# *****************************************  DEVELOPMENT LOG  ************************************* #
# DESCRIPTION : Script to load Finance Curated tables from financecw tables
# USAGE  : spark-submit /application/financecw/curated/scripts/nydfs/nydfs_cleanup.py
#         -e dev -p #financecw -s VantageP7? -z datastage -t dcpr?slprocessmtdb -c key_qualifier,company_code
# 04/08/2024 - Sagar Sawant - Initial Development
####################################################################################################

import argparse
import traceback
import logging
import os
import sys
import subprocess
import boto3
import math
import pytz
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
from botocore.client import Config

path = os.path.dirname(os.path.realpath(__file__))
print("Current Directory Path : ", path)
parent = os.path.abspath(os.path.join(path, os.pardir))
print("Parent Directory Path : ", parent)
sys.path.append(parent)

from common_utils import *

region_name = 'us-east-1'
boto3_config = Config(retries={'max_attempts': 15, 'mode': 'adaptive'})
s3_client = boto3.client('s3', region_name=region_name, config=boto3_config)
s3_paginator = s3_client.get_paginator('list_objects_v2')
rollback_skip_status = ['skipped, no further partitions to be dropped...', 'skipped, no impacted records found']
ChicagoTZ = pytz.timezone('America/Chicago')

def do_rollback():
    log.info("Rollback Started")
    table_existing_rec_count = sqlContext.read.parquet(table_location).count()
    extract_location = None
    rollback_df_cnt = 0
    rollback_df = None
    try:
        rollback_df = sqlContext.read.parquet(extract_location)
    except Exception as e:
        log.info(f"Rollback will be Skipped as {{extract_location}} is empty")
        pass
    if rollback_df:
        rollback_df_cnt = rollback_df.count()
        if not table_existing_rec_count:
            table_existing_rec_count = 0
        if rollback_df and rollback_df_cnt != table_existing_rec_count:
            rollback_df.write.partitionBy(partition_columns).mode('overwrite').parquet(table_location)
            nydfs_status_logging(table_name, status='rollback, successful', cut_off_date, cycle_date, hop_name, source_system_name, env_name, curated_database, start_tsmp, impact_count=None, source_count=rollback_df_cnt)
            log.info("Rollback Completed Successfully")
        elif rollback_df and rollback_df_cnt == table_existing_rec_count:
            nydfs_status_logging(table_name, status='rollback, skipped as the Counts are matching', cut_off_date, cycle_date, hop_name, source_system_name, env_name, curated_database, start_tsmp, impact_count=None, source_count=rollback_df_cnt)
            log.info("Rollback Skipped as the Counts are Matching")
        elif not rollback_df_cnt:
            nydfs_status_logging(table_name, status='rollback, skipped as Rollback Count was Zero', cut_off_date, cycle_date, hop_name, source_system_name, env_name, curated_database, start_tsmp, impact_count=None, source_count=rollback_df_cnt)
            log.info("Rollback Skipped as Rollback Count was Zero")

def write_and_check(src_cnt, imp_cnt, exp_cnt, tgt_cnt_after_insert, full_source_cnt, database_name, source_system_name, table_name, extract_location, partition_columns, cut_off_date, table_location, curated_database, start_tsmp):
    log.info(f"Source Impacted Record Count: {{src_cnt}}")
    log.info(f"Impacted Record Count: {{imp_cnt}}")
    if imp_cnt == 0:
        log.info("No Impacted Records Found..Skipping the Process.")
        nydfs_status_logging(table_name, status='skipped, no impacted records found', cut_off_date, cycle_date, hop_name, source_system_name, env_name, curated_database, start_tsmp, imp_cnt, src_cnt)
        sys.exit(0)

    del_cmd = f'aws s3 rm --recursive --only-show-errors {{extract_location}}'
    result = subprocess.run(del_cmd, shell=True, capture_output=True, text=True)
    if result.stdout:
        log.info(result.stdout)
    if not result.returncode == 0:
        log.error(result.stderr)

    log.info(f"Writing Source Data for {{source_system_name}} {{table_name}} to extract Location: {{extract_location}}")
    start_time = datetime.today()
    full_source_df = spark.read.options(mergeSchema=True).parquet(table_location)
    sch = full_source_df.schema

    full_source_cnt = full_source_df.count()
    log.info(f"Source Location Record Count: {{full_source_cnt}}")

    start_time = datetime.today()
    ext_loc_df = spark.read.options(mergeSchema=True).schema(sch).parquet(extract_location)
    ext_loc_cnt = ext_loc_df.count()
    log.info(f"Extract Location Record Count: {{ext_loc_cnt}}")

    if full_source_cnt == ext_loc_cnt:
        end_time = datetime.today()
        time_taken = calc_time_taken(start_time, end_time)
        log.info(f"Write Operation of Source Data to Extract Location completed at {{datetime.today().strftime('%Y-%m-%d %H:%M:%S')}}. Time Taken: {{time_taken}}(h:m:s)")
    else:
        raise Exception(f"Count Mismatches between Source Record Count and Extract Location Record Count. Failed to backup Source Data to extract Location successfully")

    exp_cnt = src_cnt - imp_cnt
    log.info(f"Expected Record Count: {{exp_cnt}}")

    if load_date_del_ind == 'Y':
        drop_partition(impdf, database_name, table_name, table_location, impact_partition=None, src_cnt=0, imp_cnt=0)
        final_qry = f"select * from {{database_name}}.{{table_name}} where TO_DATE(load_cycle_date, 'MM-dd-YYYY') < '{{cut_off_date}}'"
        final_qry_df = spark.sql(final_qry)
        tgt_cnt_before_insert = final_qry_df.count()
        log.info(f"Number of Records Expected to be Overwritten for {{database_name}}.{{table_name}} Table is: {{tgt_cnt_before_insert}}")
        if hop_name == 'curated' and table_name.endswith('_activity'):
            load_cycle_date_ovrd = 'cycle_date'
        else:
            load_cycle_date_ovrd = 'load_date'
        return
    else:
        load_cycle_date_ovrd = 'load_date'
        finaldf = impdf.cache()
        tgt_cnt_before_insert = finaldf.count()
        log.info(f"Number of Records Expected to be Overwritten for {{database_name}}.{{table_name}} Table is: {{tgt_cnt_before_insert}}")
        start_time = datetime.today()
        finaldf.repartition(2).write.mode('overwrite').insertInto('{0}.{1}'.format(database_name, table_name))
        end_time = datetime.today()
        time_taken = calc_time_taken(start_time, end_time)
        log.info(f"Written into {{database_name}}.{{table_name}} at {{datetime.today().strftime('%Y-%m-%d %H:%M:%S')}}. Time Taken: {{time_taken}}(h:m:s)")
        src_ins_qry = f"select load_date from {{curated_database}}.nydfs_driver where total_records = total_impacted_records and table_name = '{{table_name}}' and cycle_date = '{{cycle_date}}' and hop_name = '{{hop_name}}' and source_system = '{{source_system_name}}' group by load_date"
        src_ins_qry_df = spark.sql(src_ins_qry)
        if src_ins_qry_df.count():
            drop_partition(imp_part_qry_df, database_name, table_name, table_location, impact_partition=True)
        tgt_cnt_after_insert_qry = f"select count(*) from {{database_name}}.{{table_name}} where TO_DATE({{load_cycle_date_ovrd}}, 'MM-dd-YYYY') in (select distinct {{load_cycle_date_ovrd}} from driver_records)"
        result = spark.sql(tgt_cnt_after_insert_qry)
        tgt_cnt_after_insert = result.collect()[0][0]
        end_time = datetime.today()
        time_taken = calc_time_taken(start_time, end_time)
        log.info(f"Number of Records Overwritten for {{database_name}}.{{table_name}} Table is: {{tgt_cnt_after_insert}} at {{datetime.today().strftime('%Y-%m-%d %H:%M:%S')}}. Time Taken: {{time_taken}}(h:m:s)")
        tgt_qry = f"select * from {{database_name}}.{{table_name}}"
        tgt_df = spark.read.options(mergeSchema=True).schema(sch).parquet(table_location)
        except_failed = None
        try:
            src_tgt_compare_df = ext_loc_df.exceptAll(tgt_df)
        except Exception as ex:
            except_failed = True
            log.error(f"Failure with Except Query, Count src_tgt_compare_cnt Hardcoded to -99999. Exception is {{ex}}")
        if not except_failed:
            src_tgt_compare_cnt = src_tgt_compare_df.count()
            log.info(f"The Difference of count before NYDFS cleanup and after NYDFS cleanup is {{src_tgt_compare_cnt}}. The Impacted Count was {{imp_cnt}}. Both should match.")
        else:
            src_tgt_compare_cnt = -99999
        if exp_cnt != tgt_cnt_before_insert or exp_cnt != tgt_cnt_after_insert or imp_cnt != src_tgt_compare_cnt:
            msg = f"(Count Mismatch for {{source_system_name}} / {{table_name}} Expected Count: {{exp_cnt}} and Target Count: {{tgt_cnt_before_insert}} (Before Insert) / {{tgt_cnt_after_insert}} (After Insert), Impacted Count: {{imp_cnt}} and Source Target difference count: {{src_tgt_compare_cnt}}. Please Validate the Loads.)"
            log.error(msg)
            log.info(f"Restoring Source Table {{source_system_name}} {{table_name}}. Copying data from extract location: {{extract_location}} to {{table_location}}")
            start_time = datetime.today()
            df = spark.read.parquet(extract_location)
            df.repartition(2).write.mode('overwrite').insertInto('{0}.{1}'.format(database_name, table_name))
            tgt_cnt_after_restore_df = spark.sql(tgt_qry)
            src_cnt = src_qry_df.count()
            full_source_cnt = tgt_cnt_after_restore_df.count()
# --- continuation of write_and_check(...) ---

        end_time = datetime.today()
        time_taken = calc_time_taken(start_time, end_time)
        log.info(f"Source Data Record Count: {full_source_cnt} matches with Restored Record Count: {tgt_cnt_after_restore}. Source Table {source_system_name} {table_name} Restore Completed Successfully at "
                 f"{datetime.today().strftime('%Y-%m-%d %H:%M:%S')}. Time Taken: {time_taken}(h:m:s)")
    else:
        msg = f"#1. Count Mismatches between Source Record Count: {full_source_cnt} and Restored Record Count: {tgt_cnt_after_restore}. Failed to Restore Source Table from extracts location. 2. {msg}"
        log.error(msg)
        try:
            full_tgt_compare_df = full_source_df.exceptAll(tgt_cnt_after_restore_df)
            log.info(f"Count of Differences After Restore is {full_tgt_compare_df.count()}")
        except Exception as ex:
            msg = f"#3. Exception occurred while Comparing DF after Restore {ex}"
            log.error(f"Exception occurred while Comparing DF after Restore {ex}")
            raise Exception(msg)

    nydfs_status_logging(table_name, status='completed', cut_off_date, cycle_date, hop_name, source_system_name, env_name, curated_database, start_tsmp, imp_cnt, src_cnt, exp_cnt, tgt_cnt_after_insert, full_source_cnt)
    log.info(f"NYDFS Load Completed For {i}:".format(source_system_name, table_name) + ' at ' + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))

def nydfs_status_logging(table_name, status, cut_off_date, cycle_date, hop_name, source_system_name, env_name, curated_database, start_tsmp, imp_cnt, src_cnt, exp_cnt,
                         source_count=None, expected_count=None, target_count=None, full_source_cnt=None, failure_reason=None):
    if not impact_count:
        impact_count = 0
    if not source_count:
        source_count = 0
    if not expected_count:
        expected_count = 0
    if not target_count:
        target_count = 0
    if not full_source_cnt:
        full_source_cnt = 0
    if failure_reason:
        failure_reason = f"'{failure_reason}'"
    else:
        failure_reason = 'NULL'

    query = f"""select from_utc_timestamp(CURRENT_TIMESTAMP, 'US/Central') as recorded_timestamp,
cast('{start_tsmp}' as timestamp) as start_timestamp, '{table_name}' as table_name, '{status}' as status,
cast({source_count} as bigint) as source_count, cast({expected_count} as bigint) as expected_count,
cast({target_count} as bigint) as target_count, cast({full_source_cnt} as bigint) as backup_count, cast('{cut_off_date}' as date) as cut_off_date, {failure_reason} as failure_reason,
cast('{cycle_date}' as date) as cycle_date, '{hop_name}' as hop_name, '{source_system_name}' as source_system, '{env_name}' as env_name"""
    df = spark.sql(query)
    df.write.mode('append').insertInto(f"{curated_database}.nydfs_status")

def s3_delete_placeholder(bucket_name, prefix):  # prefix: 2 usages
    pages = s3_paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    allowed = 999
    for page in pages:
        if 'Contents' in page:
            objects = [{'Key': obj['Key']} for obj in page['Contents'] if obj['Key'].endswith('$')]
            i = 0
            counter = 0
            no_list = math.ceil(len(objects) / allowed)
            while i < no_list:
                objects_sub_list = objects[counter:counter + allowed]
                s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': objects_sub_list})
                counter += allowed
                i += 1


def s3_delete_placeholder(bucket_name, prefix):  # prefix: 2 usages
    pages = s3_paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    allowed = 999
    for page in pages:
        if 'Contents' in page:
            objects = [{'Key': obj['Key']} for obj in page['Contents'] if obj['Key'].endswith('$')]
            i = 0
            counter = 0
            no_list = math.ceil(len(objects) / allowed)
            while i < no_list:
                objects_sub_list = objects[counter:counter + allowed]
                s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': objects_sub_list})
                counter += allowed
                i += 1




if __name__ == "__main__":
    try:
        # Enable Logging
        log = Logging.getLogger('NydfsCopy')
        _h = Logging.StreamHandler()

        # Parse Command Line Arguments
        parser = argparse.ArgumentParser()
        parser.add_argument('-s', '--source_system_name', required=True, type=str, dest='source_system_name',
                            choices=['VantageP5', 'VantageP6', 'VantageP6SPL', 'VantageP7S', 'ltcg', 'ltcghybrid', 'eah', 'ALM', 'xdb', 'vantageone', 'refdata', 'refdataslm', 'VantageP6SFA'])
        parser.add_argument('-z', '--hop_name', required=True, type=str, dest='hop_name',
                            choices=['datastage', 'gqd', 'curated', 'annuities'])
        parser.add_argument('-t', '--table_name', required=True, type=str, dest='table_name')
        parser.add_argument('-e', '--env_name', required=True, type=str, dest='env_name')
        parser.add_argument('-c', '--column_list', required=False, type=str, dest='column_list')
        parser.add_argument('-d', '--ddl_path', required=False, type=str, dest='ddl_path')
        parser.add_argument('-l', '--load_date_del_ind', required=False, type=str, dest='load_date_del_ind',
                            choices=['Y', 'N'], default='N')
        parser.add_argument('-r', '--rollback', required=False, type=str, dest='rollback',
                            choices=['Y', 'N'], default='N')
        parser.add_argument('-y', '--cutoff_years', required=False, type=int, dest='cutoff_years', default=7)

        args = parser.parse_args()
        source_system_name = args.source_system_name
        hop_name = args.hop_name
        table_name = args.table_name
        env_name = args.env_name
        column_list = args.column_list
        ddl_path = args.ddl_path
        load_date_del_ind = args.load_date_del_ind
        cutoff_years = args.cutoff_years
        rollback = args.rollback

        _h.setFormatter(Logging.Formatter("**(%(asctime)s)** {hop_name} {table_name} %(levelname)s: %(msg)s", datefmt='%Y-%m-%d %H:%M:%S'))
        log.addHandler(_h)
        log.setLevel(Logging.INFO)

        log.info("Start : " + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
        start_tsmp = str(datetime.now(ChicagoTZ))[:-9]  # as seen cropped
        cut_off_date = datetime.strptime(datetime.strptime(cycle_date, "%Y%m%d").replace(month=1, day=1) - relativedelta(years=cutoff_years), '%Y%m%d').strftime("%Y-%m-%d")

        if table_name == 'ltcg_masterpool':
            start_of_quarter = datetime(
                year=datetime.strptime(cycle_date, "%Y%m%d").year,
                month=(math.floor((datetime.now().month - 1) / 3) + 1) * 3 + 2,
                day=1
            )
            cut_off_date = (start_of_quarter - relativedelta(months=6)).strftime("%Y-%m-%d")
            log.info(f"Current Quarter Start Date is: {start_of_quarter}. Cut off Date Override will be 6 months before this date")

        cut_off_date_short = datetime.strptime(cut_off_date, "%Y-%m-%d").strftime("%Y%m%d")
        cycle_date_short = datetime.strptime(cycle_date, "%Y%m%d").strftime("%Y-%m-%d")
        two_years_from_cycle_date = datetime.strptime(datetime.strptime(cycle_date, "%Y%m%d").replace(month=3, day=1) + relativedelta(years=2), '%Y%m%d').strftime("%Y-%m-%d")
        cycle_date = datetime.strptime(cycle_date, "%Y%m%d").strftime("%Y-%m-%d")

        imp_cnt, src_cnt, exp_cnt, tgt_cnt_after_insert, full_source_cnt = 0, 0, 0, 0, 0

        variables = common_vars(source_system_name)
        annuities_database = variables['annuities']
        datastage_database = variables['datastage']
        curated_database = variables['curated']
        gqd_database = variables['gqd']
        idl_database = variables['datalake_curated']

        admn_system_mapper = {'VantageP6SPL': 'spl', 'VantageP6': 'p6', 'VantageP6S': 'p6s', 'VantageP6SFA': 'p6sfa', 'VantageP7S': 'p7s'}
        admin_system_short = admn_system_mapper.get(source_system_name, source_system_name)

        extract_location = f"s3://ta-individual-findw-<<<UNREADABLE>>>-extracts/nydfs/{<<<UNREADABLE:project>>>}/{hop_name}/{source_system_name}/{table_name}/"

        # Initialize Spark
        appname = f"NYDFS Load {source_system_name} for {hop_name}: {table_name}"
        spark = SparkSession.builder.appName(appname).enableHiveSupport() \
            .config("hive.exec.dynamic.partition", "true") \
            .config("hive.exec.dynamic.partition.mode", "nonstrict").getOrCreate()
        sc = spark.sparkContext
        sqlContext = SQLContext(sc)
        sc.setLogLevel("ERROR")
        spark.conf.set("spark.serialization", "org.apache.spark.serializer.KryoSerializer")
        spark.conf.set("spark.sql.tungsten.enabled", "true")
        spark.conf.set("spark.sql.broadcastTimeout", "3600")
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", "true")
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        if hop_name == 'datastage':
            source_database = datastage_database
            load_cycle_date = 'load_date'
        elif hop_name == 'gqd':
            source_database = gqd_database
            load_cycle_date = 'load_date'
        elif hop_name == 'curated':
            source_database = curated_database
            load_cycle_date = 'cycle_date'
        elif hop_name == 'annuities':
            source_database = annuities_database
            load_cycle_date = 'cycle_date'

        log.info(f"Rollback Indicator is: {rollback}")
        log.info(f"Delete Indicator is: {load_date_del_ind}")
        log.info(f"Source Database is: {source_database}")
        log.info(f"IDL Database is: {idl_database}")
        log.info(f"Cycle Date is: {cycle_date}")
        log.info(f"Cut Off Number of years is: {cutoff_years}")
        log.info(f"Cut Off Date (Jan 1st Date) is: {cut_off_date}")
        log.info(f"Two Years from Cycle Date (Jan 1st Date) is: {two_years_from_cycle_date}")
        log.info(f"Extract Location is: {extract_location}")

        columns = spark.catalog.listColumns(f"{source_database}.{table_name}")
        partition_columns = [c.name for c in partition_columns]  # NOTE: screenshot shows a comprehension; adjust as needed
        table_location = glue_table_location(source_database, table_name, log)

        # Rollback logic
        if rollback == 'Y':
            query_status = f"""
            SELECT status FROM (
              SELECT *, row_number() over (partition by null order by recorded_timestamp desc) as rItr
                FROM {curated_database}.nydfs_status
               where source_system = '{source_system_name}'
                 and hop_name = '{hop_name}'
                 and table_name = '{table_name}'
                 and cycle_date = '{cycle_date}'
            ) A where rItr = 1
            """
            query_status_df = spark.sql(query_status)
            if query_status_df and query_status_df.count():
                query_status = query_status_df.collect()[0][0]
                if query_status not in rollback_skip_status:
                    do_rollback()
                else:
                    log.info(f"Rollback Skipped as Last status was {query_status} and is not in {rollback_skip_status}")
                    nydfs_status_logging(table_name, status='rollback, skipped as for skipped Rollback Status',
                                         cut_off_date=cut_off_date, cycle_date=cycle_date, hop_name=hop_name,
                                         source_system_name=source_system_name, env_name=env_name,
                                         curated_database=curated_database, start_tsmp=start_tsmp)
            else:
                raise Exception("Rollback Not Allowed as there was no run")
            sys.exit(0)

        log.info(f"Verifying Status Table if Data load is already complete for {source_system_name} and {table_name}")
        querystatuscount = f"""
          SELECT count(*), status FROM (
            SELECT *, row_number() over (partition by null order by recorded_timestamp desc) as rItr
              FROM {curated_database}.nydfs_status
             where source_system = '{source_system_name}'
               and hop_name = '{hop_name}'
               and table_name = '{table_name}'
               and cycle_date = '{cycle_date}'
               and env_name = '{env_name}'
          ) A where rItr = 1 group by status
        """
        querystatuscount_df = spark.sql(querystatuscount)
        count_st = 0
        latest_status = None
        if querystatuscount_df and querystatuscount_df.count():
            count_st = querystatuscount_df.collect()[0][0]
            latest_status = querystatuscount_df.collect()[0][1]
            log.info(f"Latest Status is {latest_status}")

        if not latest_status:
            log.info(f"No entry found in status table with status: complete. Continuing ahead with NYDFS Load for {source_system_name} and {table_name}")
        elif latest_status and latest_status.startswith('rollback'):
            log.info(f"Last Entry found was {latest_status}. Continuing ahead with NYDFS Load for {source_system_name} and {table_name}")
        elif latest_status and latest_status.startswith('failed'):
            raise Exception(f"Failing as rollback may be needed as the Last run for {source_system_name} and {table_name} was {latest_status}. Do a Rollback and Retry.")
        else:
            log.info(f"Data is already loaded for {source_system_name} and {table_name}. Skipping Data Load...")
            nydfs_status_logging(table_name, status='skipped, data is already loaded', cut_off_date=cut_off_date, cycle_date=cycle_date,
                                 hop_name=hop_name, source_system_name=source_system_name, env_name=env_name,
                                 curated_database=curated_database, start_tsmp=start_tsmp)
            sys.exit(0)

        col1, col2 = None, None
        ltcg_party = None

        if column_list and ',' in column_list:
            col1, col2 = column_list.replace(' ', '').split(',')
        elif column_list:
            col1 = column_list

        if col1 and col2:
            qyovwrd1 = f"{col1}, {col2}"
            qyovwrd2 = f"ir.{col1} as contractnumber, ir.{col2} as contractadministrationlocationcode"
            qyovwrd3 = f"fs.{col1} = d.contractnumber and s.{col2} = d.contractadministrationlocationcode"
        else:
            if col1:
                qyovwrd1 = f"{col1}"
                qyovwrd2 = f"ir.{col1} as contractnumber, NULL as contractadministrationlocationcode"
                qyovwrd3 = f"fs.{col1} = d.contractnumber"
            elif col2:
                raise Exception("Not Allowed to Delete basis on ContractAdminLocationCode Alone. Please Check the Input")
            if load_date_del_ind == 'N':
                raise Exception("Invalid Combination. Delete Indicator is N but Contract Number is Empty")

        if table_name.endswith(f"{source_system_name}_party") and 'ltcg' in source_system_name:
            ltcg_party = True
            qyovwrd1 = f"<<<UNREADABLE:partydocumentid>>>, contractnumber"
            qyovwrd2 = f"d.{<<<UNREADABLE>>>} as other_key_name, ir.{col1} as other_key_value"
            qyovwrd3 = f"fs.{col1} = d.other_key_value and d.other_key_name = <<<UNREADABLE:partydocumentid>>>"
            qyovwrd4 = f"NULL as contractnumber, NULL as contractadministrationlocationcode"

        log.info(f"Data Load Started for {table_name} at " + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))

        # (Many long SQL strings; captured as visible)
        if load_date_del_ind == 'Y':
            if hop_name == 'curated':
                source_qry = f"select {load_cycle_date} as load_date from {source_database}.{table_name} where cycle_date < '{cut_off_date}'"
            else:
                source_qry = f"select {load_cycle_date} as load_date from {source_database}.{table_name} where TO_DATE({load_cycle_date}, 'MM-dd-YYYY') < '{cut_off_date}'"
            source_qry_df = spark.sql(source_qry).cache()
            source_qry_df.createOrReplaceTempView('driver_records')
            src_cnt = source_qry_df.count()
            imp_cnt = src_cnt
            imp_part_qry_df = f"select load_date from driver_records group by load_date"
            imp_part_qry_df = spark.sql(imp_part_qry_df).cache()
            write_and_check(src_cnt, imp_cnt, exp_cnt, tgt_cnt_after_insert, full_source_cnt, source_database, source_system_name, table_name,
                            extract_location, partition_columns, cut_off_date, table_location, curated_database, imp_part_qry_df)
            sys.exit(0)

        elif load_date_del_ind == 'N':
            if hop_name == 'curated' and 'ltcg' in source_system_name:
                common_querypath = f"{parent}/query/common/"
                config_q_p = <<<UNREADABLE>>>  # config loader unreadable
                config['cycle_date'] = cycle_date
                config['cycledate1'] = cycle_date_short
                config['two_years_from_cycle_date'] = two_years_from_cycle_date
                config['seven_year_from_cycle_date'] = cut_off_date
                config['source_database'] = idl_database
                with open(f"{<<<UNREADABLE:common_querypath>>>}/ltcg_expired_contracts.sql") as code_flu:
                    code = code_flu.read()
                expired_qry_df = spark.sql(code.format(**config)).cache()
                expired_qry_df.createOrReplaceTempView('expired_contracts')

                if f"{source_system_name}_contract" in table_name:
                    imp_rec_qry = f"""
                        select {qyovwrd1}, {load_cycle_date} from {source_database}.{table_name} a
                        left semi join expired_contracts b on a.contractnumber = b.policy_no
                    """
                elif ltcg_party:
                    imp_rec_qry = f"""
                        select /*+ BROADCAST(d) */ {qyovwrd1}, {load_cycle_date}
                        from {source_database}.{table_name} a
                        left semi join (select /*+ BROADCAST(d, e) */ <<<UNREADABLE>>> ) d
                        <<<UNREADABLE long union of party tables and joins, as in images >>>
                    """
                else:
                    imp_rec_qry = f"""
                        select {qyovwrd1}, {load_cycle_date} from {source_database}.{table_name} a
                        left semi join (select * from {curated_database}.nydfs_driver
                                          where table_name = '{source_system_name}_contract'
                                            and cycle_date = '{cycle_date}'
                                            and hop_name = 'curated'
                                            and source_system = '{source_system_name}') d
                        on {qyovwrd3}
                    """

            else:
                # other branches shown in images for datastage/eah etc.
                imp_rec_qry = f"""
                  select {qyovwrd1}, {load_cycle_date} from {source_database}.{table_name} a
                  left semi join (select * from {curated_database}.nydfs_driver
                                    where table_name = '{<<<UNREADABLE:admin_system_short>>>}_contract'
                                      and cycle_date = '{cycle_date}'
                                      and hop_name = 'datastage'
                                      and source_system = '{source_system_name}') d
                  on {qyovwrd3}
                """

            # Build temp tables and driver insert
            imp_rec_qry_df = spark.sql(imp_rec_qry)
            imp_rec_qry_df.createOrReplaceTempView('imp_records')

            totalrec_qry = f"select count(*) as total_records, {load_cycle_date} from {source_database}.{table_name} group by {load_cycle_date}"
            totalrec_qry_df = spark.sql(totalrec_qry)
            totalrec_qry_df.createOrReplaceTempView('total_records')

            total_imp_rec_qry = f"select count(1) as total_impacted_records, {load_cycle_date} from imp_records group by {load_cycle_date}"
            total_imp_rec_df = spark.sql(total_imp_rec_qry)
            total_imp_rec_df.createOrReplaceTempView('total_impacted_records')

            start_time = datetime.today()
            driver_insert_qry = f"""
              select from_utc_timestamp(CURRENT_TIMESTAMP, 'US/Central') as recorded_timestamp, {qyovwrd1},
                     tr.total_records as total_records, tir.total_impacted_records as total_impacted_records,
                     TO_DATE(d.{load_cycle_date}, 'MM-dd-yyyy') as load_date, cast('{cut_off_date}' as date) as cut_off_date,
                     cast('{cycle_date}' as date) as cycle_date,
                     '{hop_name}' as hop_name, '{source_system_name}' as source_system, '{table_name}' as table_name
                from imp_records d
                inner join total_records tr on tr.{load_cycle_date} = d.{load_cycle_date}
                inner join total_impacted_records tir on tir.{load_cycle_date} = tr.{load_cycle_date}
            """
            driver_insert_qry_df = spark.sql(driver_insert_qry)
            driver_insert_qry_df.cache()
            driver_insert_qry_df.count()
            driver_insert_qry_df.repartition(2).write.mode('overwrite').insertInto('{0}.{1}'.format(curated_database, 'nydfs_driver'))
            end_time = datetime.today()
            time_taken = calc_time_taken(start_time, end_time)
            log.info(f"Driver Insert: Records written into nydfs driver table at {datetime.today().strftime('%Y-%m-%d %H:%M:%S')}. Time Taken: {time_taken}(h:m:s)")

            driver_qry = f"select * from {curated_database}.nydfs_driver where table_name = '{table_name}' and cycle_date = '{cycle_date}' and hop_name = '{hop_name}' and source_system = '{source_system_name}'"
            driver_qry_df = spark.sql(driver_qry)
            driver_qry_df.cache()
            imp_cnt = driver_qry_df.count()

            source_qry = f"""
              select * from {source_database}.{table_name}
               where TO_DATE({load_cycle_date}, 'MM-dd-YYYY') in (select distinct load_date from driver_records)
            """
            source_qry_df = spark.sql(source_qry)
            source_qry_df.cache()
            source_qry_df.createOrReplaceTempView('source_records')
            src_cnt = source_qry_df.count()

            final_qry = f"""
              select * from source_records s
               left anti join driver_records d
                 on ({qyovwrd3}) and TO_DATE(s.{load_cycle_date}, 'MM-dd-yyyy') = d.load_date
            """
            final_qry_df = spark.sql(final_qry)

            write_and_check(
                src_cnt, imp_cnt, exp_cnt, tgt_cnt_after_insert, full_source_cnt,
                source_database, source_system_name, table_name, extract_location,
                partition_columns, cut_off_date, table_location, curated_database, final_qry_df
            )

    except Exception as ex:
        traceback.print_exc()
        log.exception('Failing the script - Exception Details')
        err = str(ex).strip().replace('"', "'").replace('\n', ' ').replace('\r', ' ')
        nydfs_status_logging(
            table_name, status='failed', cut_off_date=cut_off_date, cycle_date=cycle_date, hop_name=hop_name,
            source_system_name=source_system_name, env_name=env_name, curated_database=curated_database,
            start_tsmp=start_tsmp, imp_cnt=imp_cnt, src_cnt=src_cnt, exp_cnt=exp_cnt,
            full_source_cnt=full_source_cnt, failure_reason=err
        )
        log.info("End : " + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
        exit(255)

    finally:
        log.info("Script completed")
        log.info("End : " + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))