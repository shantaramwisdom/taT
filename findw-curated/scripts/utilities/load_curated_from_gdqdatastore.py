########################################################################################################
# DEVELOPMENT LOG
# DESCRIPTION  : Script to load Finance Curated tables from gdqdatastore tables
# USAGE 1 : spark-submit /application/financedw/curated/scripts/utilities/load_curated_from_gdqdatastore.py \
#           -s ltcg -d activity
# USAGE 2 : spark-submit /application/financedw/curated/scripts/utilities/load_curated_from_gdqdatastore.py \
#           -s ltcg -d batchtracking
# USAGE 3 : spark-submit /application/financedw/curated/scripts/utilities/load_curated_from_gdqdatastore.py \
#           -s ltcg -d gdqdelete
# 12/07/2023 : Sagar Sawant - Initial Development
########################################################################################################

import argparse
import traceback
import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
from cryptography.fernet import Fernet

path = os.path.dirname(os.path.realpath(__file__))
print("Current Directory Path - ", path)
parent = os.path.abspath(os.path.join(path, os.pardir))
print("Parent Directory Path - ", parent)
sys.path.append(parent)
from common_utils import *

def write_and_check(source_system_name, domain_name, gdq_database, curated_database, curated_table, finaldf, checks='Y', final_count=0):
    log.info("Data Read Started at : " + datetime.now(est_tz).strftime("%Y-%m-%d %H:%M:%S"))
    finaldf = finaldf.cache()
    start_time = datetime.now(est_tz)

    # repartition default
    repartition = 2
    if domain_name in ['masterpool', 'contractexception'] and source_system_name == 'ltcg':
        repartition = 10

    if domain_name not in ['gdqdelete', 'batchtracking', 'masterpool']:
        if source_system_name == 'ltcg' and domain_name == 'activity':
            tablename = "gdqltcglossadjustment  gdqltcgclasspayment  gdqltcgactivitypremium"
        elif source_system_name == 'ltcghybrid' and domain_name == 'activity':
            tablename = "gdqltchybridclassadjustment  gdqltchybridclasspayment"
        else:
            tablename = f"gdq.{source_system_name}_{domain_name}"
        # sourcecount via SQL if needed (screenshot shows a commented select); using count as fallback
        sourcecount = finaldf.count()
    else:
        sourcecount = finaldf.count()

    end_time = datetime.now(est_tz)
    time_taken = calc_time_taken(start_time, end_time)
    log.info(f"Number of Records for {source_system_name} and {domain_name} in Source Table is: {sourcecount} at {datetime.now(est_tz).strftime('%Y-%m-%d %H:%M:%S')}. Time Taken: {time_taken}(h:m:s)")

    start_time = datetime.now(est_tz)
    finaldf.repartition(repartition).write.mode("overwrite").insertInto("{0}.{1}".format(curated_database, curated_table))
    end_time = datetime.now(est_tz)
    time_taken = calc_time_taken(start_time, end_time)
    log.info(f"Write into {curated_database}.{curated_table} at {datetime.now(est_tz).strftime('%Y-%m-%d %H:%M:%S')}. Time Taken: {time_taken}(h:m:s)")

    if checks == 'Y':
        return sourcecount

    if (source_system_name == 'ltcg' and domain_name == 'masterpool'):
        if curated_table in ['completedbatch', 'curateddeletes']:
            curated_ovrd = " where source_system in ('ltcg', 'ltcghybrid')"
            querycurcount = "select count(*) from {0}.{1}{2}".format(curated_database, curated_table, curated_ovrd)
            start_time = datetime.now(est_tz)
            result = spark.sql(querycurcount)
            curcount = result.collect()[0][0]
            end_time = datetime.now(est_tz)
            time_taken = calc_time_taken(start_time, end_time)
            log.info(f"Number of Records Inserted for {curated_database}.{curated_table} Table is: {curcount} at {datetime.now(est_tz).strftime('%Y-%m-%d %H:%M:%S')}. Time Taken: {time_taken}(h:m:s)")
            if sourcecount != curcount:
                msg = f"Error: Count Mismatch between Source {source_system_name} / {domain_name} (cnt={sourcecount}) and Curated {curated_database}.{curated_table} (cnt={curcount}). Please Validate the Loads."
                log.error(msg)
                raise Exception(msg)
            log.info("Data Copy Completed For {0}".format(curated_table) + " at " + datetime.now(est_tz).strftime("%Y-%m-%d %H:%M:%S"))
        finaldf.unpersist()

if __name__ == "__main__":
    try:
        log.info("Curated Load from GDQdatastore Start : " + datetime.now(est_tz).strftime("%Y-%m-%d %H:%M:%S"))

        # Parse CommandLine Arguments
        parser = argparse.ArgumentParser()
        parser.add_argument('--source_system_name', '-s', required=True, type=str, dest='source_system_name')
        parser.add_argument('--domain', '-d', required=True, type=str, dest='domain')
        log.info("Start : " + datetime.now(est_tz).strftime("%Y-%m-%d %H:%M:%S"))
        variablesList = dir()
        args = parser.parse_args()
        source_system_name = args.source_system_name
        domain = args.domain

        variables = common_vars(source_system_name)
        curated_database = variables['curated']
        gdc_database = variables['gdq']
        gdc_database = gdc_database.replace('financeorchestration', 'financedwgdq')  # from screenshot text
        findw_db = variables['findw_db']
        querypath = home + "/Query/DataCopy/" + source_system_name + "/"
        batchid = 0
        cycle_date = '9999-12-31'
        phase_name = 'DATA COPY'

        # Initialize Spark
        appname = f"Curated Load From GDQdatastore for {0}".format(source_system_name, domain)
        spark = SparkSession.builder.appName(appname).enableHiveSupport() \
            .config("hive.exec.dynamic.partition", "true") \
            .config("hive.exec.dynamic.partition.mode", "nonstrict").getOrCreate()

        sc = spark.sparkContext
        sc.setLogLevel("ERROR")

        spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        spark.conf.set("spark.sql.tungsten.enabled", "true")
        spark.conf.set("spark.sql.broadcastTimeout", "3000")
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "104857600")
        spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", "true")
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

        fileName = domain
        location = source_system_name.lower() + "_" + domain
        log.info(f"Source Database is: {gdq_database}")
        log.info(f"Target Database is: {curated_database}")
        log.info("Verifying batchtracking if Data load is already complete for {source_system_name} and {domain}")
        querybatchcount = "select count(*) from financeorchestration.batch_tracking where source_system_name = '{source_system_name}' \
            and domain_name = '{domain}' and batch_id = {batchid} and cycle_date = '{cycle_date}' and phase_name = '{phase_name}' \
            and load_status = 'complete'"
        result = get_jdbc_df(spark, 'findw', querybatchcount)
        count_bt = result.collect()[0][0]
        if count_bt == 0:
            batchtracking_logging(source_system_name, domain, batchid, 'in-progress', cycle_date, 'insert', phase_name, None, None, 'Y')
        else:
            log.info(f"Data is already loaded for {source_system_name} and {domain}. Skipping...Data Load.")
            sys.exit(0)

        if domain == 'batchtracking':
            curated_table = "completedbatch"
            log.info("Data Copy Started For {0}".format(curated_table) + " at " + datetime.now(est_tz).strftime("%Y-%m-%d %H:%M:%S"))
            qry = "select batch_id, cycle_date, domain_name, source_system_name, insert_timestamp from \
                  (select batch_id, cycle_date, domain_name, source_system_name order by insert_timestamp desc) as rnum \
                   from financeorchestration.batch_tracking where source_system_name in ('ltcg','ltcghybrid') \
                   and domain_name not in ('financecontract') and load_status = 'complete' ) where rnum = 1"
            orchdf = get_jdbc_df(spark, 'findw', qry)
            finaldf = orchdf.withColumn("batch_frequency", F.lit('DAILY'))
            finaldf = finaldf.select('batch_id', 'cycle_date', F.col('insert_timestamp').alias('recorded_timestamp'), 'domain_name', F.col('source_system_name').alias('source_system'), 'batch_frequency')
            write_and_check(source_system_name, domain, gdq_database, curated_database, curated_table, finaldf)

        elif domain == 'gdqdelete':
            curated_table = "curateddeletes"
            log.info("Data Copy Started For {0}".format(curated_table) + " at " + datetime.now(est_tz).strftime("%Y-%m-%d %H:%M:%S"))
            btry = "select batch_id, cycle_date, domain_name, source_system_name, insert_timestamp, \
                    row_number() over(partition by batch_id, cycle_date, domain_name, source_system_name order by insert_timestamp desc) as rnum \
                    from financeorchestration.batch_tracking where source_system_name in ('ltcg','ltcghybrid') \
                    and domain_name not in ('financecontract') \
                    and phase_name = 'gdqdatastore' and load_status = 'complete' ) where rnum = 1"
            btdf = get_jdbc_df(spark, 'findw', btry)
            btdf.createOrReplaceTempView("redshift_batches")

            qry = "select insert_timestamp as recorded_timestamp, SHA2(concat(naturalkey, ':', cdcKey), 256) documentid, cast(pointofviewstopdate as date) as pointofviewstopdate, batch_id, \
                   tablename as domain_name, sourcesystem as source_system, 'DAILY' batch_frequency, cast(load_date as date) as cycle_date \
                   from {gdq_database}.gdqdatastore_deletes d join redshift_batches b on sourcesystem = source_system_name and load_date = cycle_date and domain_name = tablename"
            finaldf = spark.sql(qry)
            write_and_check(source_system_name, domain, gdq_database, curated_database, curated_table, finaldf)

        elif domain == 'masterpool':
            log.info("Data Copy Started For {0}".format('masterpool') + " at " + datetime.now(est_tz).strftime("%Y-%m-%d %H:%M:%S"))
            btry = "select batch_id, cycle_date, domain_name, source_system_name, insert_timestamp, \
                    (select batch_id, cycle_date, insert_timestamp, \
                     row_number() over(partition by batch_id, cycle_date, domain_name, source_system_name order by insert_timestamp desc) as rnum \
                       from financeorchestration.batch_tracking where source_system_name = 'ltcg' and domain_name = 'masterpool' \
                       and phase_name = 'gdqdatastore' and load_status = 'complete') bt where bt.rnum = 1"
            masterpoolqry = f"select bt.insert_timestamp as recorded_timestamp, 'ltcg' as source_system_name, \
                               gdqref.*, bt.batch_id from gdq_databases.ref_master_pool gdqref \
                               inner join batchtracking_batches bt \
                               on gdqref.cycle_date = bt.cycle_date"
            prior_dt = '2023-01-01'
            intr_count = 0
            cnt = 0
            dates = ['2023-03-31', '2023-06-30', '2023-09-30', '2023-12-31', datetime.now(est_tz).strftime('%Y-%m-%d')]
            for i in dates:
                prefix = f" and bt.cycle_date > '{prior_dt}' and bt.cycle_date <= '{i}'"
                log.info(f"Additional Prefix Added to Query is : {prefix}")
                btry2 = btry + prefix
                masterpoolqry2 = masterpoolqry + prefix
                btdf = get_jdbc_df(spark, 'findw', btry2)
                btdf.createOrReplaceTempView("batchtracking_batches")
                prior_dt = i
                finaldf = spark.sql(masterpoolqry2)
                cnt = cnt + 1
                if cnt == len(dates):
                    write_and_check(source_system_name, domain, gdq_database, curated_database, 'masterpool', finaldf, checks='Y', final_count=intr_count)
                else:
                    intr_count2 = write_and_check(source_system_name, domain, gdq_database, curated_database, 'masterpool', finaldf, checks='N')
                    intr_count = intr_count + intr_count2

        else:
            with open("{0}{1}".format(querypath, fileName)) as code_file:
                code = code_file.read()
            finaldf = spark.sql(code.format(gdq_database)).coalesce(20).cache()
            write_and_check(source_system_name, domain, gdq_database, curated_database, domain, finaldf)

        batchtracking_logging(source_system_name, domain, batchid, 'complete', cycle_date, 'update', phase_name, None, None, 'Y')

    except Exception as ex:
        traceback.print_exc()
        log.error("Failing the script: ", ex)
        batchtracking_logging(source_system_name, domain, batchid, 'failed', cycle_date, 'update', phase_name, None, None, 'Y', ex)
        log.info("End: " + datetime.now(est_tz).strftime("%Y-%m-%d %H:%M:%S"))
        exit(255)
    finally:
        log.info("Script completed")
        log.info("End: " + datetime.now(est_tz).strftime("%Y-%m-%d %H:%M:%S"))