from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, SQLContext
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import os
import sys
import subprocess
import argparse
import traceback

path = os.path.dirname(os.path.realpath(__file__))
print("Current Directory Path - ", path)
parent = os.path.abspath(os.path.join(path, os.pardir))
print("Parent Directory Path - ", parent)
sys.path.append(parent)
from common_utils import *
cst_tz = ZoneInfo("America/Chicago")

if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('--source_system', '-s', required=True, type=str, dest='source_system', choices = ['arr', 'ahd'])
        parser.add_argument('--odate', '-o', required=True, type=str, dest='odate')
        args = parser.parse_args()
        source_system = args.source_system
        odate = args.odate
        cycle_date = datetime.strptime(odate, "%Y-%m-%d").strftime("%Y-%m-%d")
        #source_system = 'ahd'
        now_cst = datetime.now(cst_tz)
        #yesterday_cst = now_cst - timedelta(days=1)
        #cycle_date = yesterday_cst.strftime("%Y-%m-%d")
        #cycle_date = '2025-08-24'
        batch_id = 100

        batch_frequency_dict = {'ahd': 'DAILY', 'arr': 'MONTHLY'}
        source_flag_dict = {'ahd': 'Y', 'arr': 'Y'}
        batch_frequency = batch_frequency_dict[source_system]
        source_flag = source_flag_dict[source_system]

        variables = common_vars(source_system, 'CN_InitialLoad.txt')
        curated_database = variables['curated']

        appname = f"Contract Plan Initial Load - {source_system}"
        spark = SparkSession.builder.appName(appname).enableHiveSupport() \
            .config("hive.exec.dynamic.partition", "true") \
            .config("hive.exec.dynamic.partition.mode", "nonstrict").getOrCreate()

        sc = spark.sparkContext
        sc.setLogLevel("ERROR")

        location = f"s3://ta-individual-findw-fileshare-{environment}/prod/{source_system}/" + "<<<UNREADABLE: filename or subfolder >>>"
        log.info(f"Input File Location is: {location}")

        ahd_schema = StructType([
            StructField("contractsourcesystemname", StringType(), True),
            StructField("contractnumber", StringType(), True),
            StructField("pincode", StringType(), True),
            StructField("contractsissuedate", StringType(), True),
            StructField("contractissueage", StringType(), True),
            StructField("contractparticipationindicator", StringType(), True),
            StructField("contractsourcemarketorganizationcode", StringType(), True)
        ])

        arr_schema = StructType([
            StructField("contractsourcesystemname", StringType(), True),
            StructField("contractnumber", StringType(), True),
            StructField("pincode", StringType(), True),
            StructField("contractsissuedate", StringType(), True),
            StructField("contractissueage", StringType(), True),
            StructField("contractparticipationindicator", StringType(), True),
            StructField("contractsourcemarketorganizationcode", StringType(), True),
            StructField("firstyearindicator", StringType(), True),
            StructField("blockofbusiness", StringType(), True)
        ])

        if source_system == 'ahd':
            csv_schema = ahd_schema
        else:
            csv_schema = arr_schema

        df = spark.read.format('csv') \
            .schema(csv_schema) \
            .load(location).cache()

        source_cnt = df.count()
        log.info(f"Input File Count is: {source_cnt}")

        df = df.withColumn(
            "contractsissuedate",
            F.to_date(F.col("contractsissuedate"), "M/d/yyyy H:mm:ss")
        )

        length_counts_df = df.withColumn("length", F.length("contractnumber")) \
            .groupBy("length") \
            .count() \
            .orderBy("length")
        log.info("Length Counts From Input")
        length_counts_df.show()

        if source_system == 'ahd':
            df_stripped = df.withColumn(
                "stripped_contract",
                F.regexp_replace(F.col("contractnumber"), r"[^0-9]+", "")
            )

            padded_df = df_stripped.withColumn(
                "padded_contract",
                F.when(F.length(F.col("stripped_contract")) < 8, F.lpad(F.col("stripped_contract"), 8, "0")) \
                 .when((F.length(F.col("stripped_contract")) >= 8) & (F.length(F.col("stripped_contract")) < 10), F.lpad(F.col("stripped_contract"), 10, "0")) \
                 .otherwise(F.col("stripped_contract"))
            )

            length_counts_df = padded_df.withColumn("length_pd", F.length("padded_contract")) \
                .groupBy("length_pd") \
                .count() \
                .orderBy("length_pd")
            log.info("Length Counts From Padded")
            length_counts_df.show()

            padded_df_mod = padded_df.withColumn("contractnumber", F.col("padded_contract")) \
                .drop("stripped_contract", "padded_contract")
        else:
            padded_df_mod = df

        padded_df_mod.createOrReplaceTempView("contractplan")

        if source_system == 'ahd':
            sql = f"""
select distinct from_utc_timestamp(current_timestamp, 'US/Central') as recorded_timestamp,
       '{source_system}' as source_system_name,
       sha2(
         concat(
           '{source_system}',
           ':',
           coalesce(trim(contractsourcesystemname), ''),
           ':',
           coalesce(regexp_replace(contractnumber, '^0+', ''), '')
         ),
         256
       ) as documentid,
       '{source_system}' as sourcesystemname,
       contractsourcesystemname,
       regexp_replace(contractnumber, '^0+', '') as contractnumber,
       pincode,
       contractsissuedate,
       contractissueage,
       contractparticipationindicator,
       contractsourcemarketorganizationcode,
       cast('{cycle_date}' as date) as cycle_date,
       cast({batch_id} as int) as batch_id,
       'INSERT' as cdc_action
from contractplan
where length(trim(contractnumber)) > 0
"""
        else:
            sql = f"""
select distinct from_utc_timestamp(CURRENT_TIMESTAMP, 'US/Central') as recorded_timestamp,
       '{source_system}' as source_system_name,
       SHA2(
         concat(
           '{source_system}',
           ':',
           coalesce(trim(contractsourcesystemname), ''),
           ':',
           coalesce(regexp_replace(contractnumber, '^0+', ''), '')
         ),
         256
       ) AS documentid,
       '{source_system}' as sourcesystemname,
       contractsourcesystemname,
       regexp_replace(contractnumber, '^0+', '') as contractnumber,
       pincode,
       contractsissuedate,
       contractissueage,
       contractparticipationindicator,
       contractsourcemarketorganizationcode,
       firstyearindicator,
       blockofbusiness,
       CAST('{cycle_date}' AS DATE) as cycle_date,
       CAST({batch_id} AS INT) as batch_id,
       'INSERT' as cdc_action
from contractplan
where length(trim(contractnumber)) > 0
"""

        finaldf = spark.sql(sql).cache()
        cnt_finaldf = finaldf.count()
        log.info(f"FINAL DF Count is: {cnt_finaldf}")

        df_cnt = finaldf.remove_dups_and_expired(spark, source_system, finaldf, "finaldf")   # <<<UNREADABLE: if helper is named differently, keep original >>>
        log.info(f"Writing to Curated Table - contractplancodedetails")
        finaldf.drop("<<<UNREADABLE>>>").repartition(2).write.mode("overwrite").insertInto(f"{curated_database}.{{table_name}}")  # <<<UNREADABLE: exact drop column & table_name token >>>

        tgt_cnt = spark.sql(f"SELECT COUNT(*) FROM {curated_database}.{{table_name}} \
                             where cycle_date = '{{cycle_date}}' and batch_id = {{batch_id}}").collect()[0][0]  # <<<UNREADABLE placeholders kept >>>
        if tgt_cnt != source_cnt:
            log.error(f"Target Count {tgt_cnt} and Source Count {source_cnt} are not matching")
            raise Exception("Count mismatch")
        else:
            log.info(f"Target Count {tgt_cnt} and Source Count {source_cnt} are matching")

        log.info("Writing to Batch Tracking Table - contractplancodedetails")
        ins_query = f"""
--INSERT INTO { 'financecochestration.batch_tracking' }      -- <<<UNREADABLE: schema/casing of orchestration schema >>>
( source_system_name, domain_name, phase_name, load_status, batch_id, cycle_date, job_name)
select ('{source_system}'), 'contractplancodedetails', 'curated', 'complete', {batch_id}, cast('{cycle_date}' as date), 'INITIAL_LOAD'
union all
select ('{source_system}'), 'contractorleainsurancetreatycompidassignment', 'curated controls', 'complete', {batch_id}, cast('{cycle_date}' as date), 'INITIAL_LOAD'
"""
        load_table('findw', ins_query)

        sql = f"""
SELECT DISTINCT cast({batch_id} as bigint) batch_id,
       cast('{cycle_date}' as date) cycle_date,
       from_utc_timestamp(CURRENT_TIMESTAMP, 'US/Central') AS recorded_timestamp,
       'contractplancodedetails' domain_name,
       '{source_system}' as source_system,
       '{batch_frequency}' as batch_frequency
union all
SELECT DISTINCT cast({batch_id} as bigint) batch_id,
       cast('{cycle_date}' as date) cycle_date,
       from_utc_timestamp(CURRENT_TIMESTAMP, 'US/Central') AS recorded_timestamp,
       'contractorleainsurancetreatycompidassignment' domain_name,
       '{source_system}' as source_system,
       '{batch_frequency}' as batch_frequency
"""
        df_current = spark.sql(sql)

        log.info("Writing to Current Batch Table")
        df_current.repartition(1).write.mode("overwrite").insertInto(f"{curated_database}.currentbatch")
        print("******************************************************************************************")

        command = f"sh /application/financedw/curated/scripts/load_curated.sh -s {source_system} -d completedbatch_contractplancodedetails -f {batch_frequency} -j INITIAL_LOAD -g {source_flag}"
        log.info(f"Table contractplancodedetails Completed Batch Data Load Command: {command}")
        result = subprocess.run(command, shell = True, capture_output = True, text = True)
        if result.stdout:
            log.info(result.stdout)
        if result.returncode != 0:
            log.error(result.stderr)
            raise Exception(f"Command failed with return code {result.returncode}: {result.stderr}")
        log.info("Table contractorleainsurancetreatycompidassignment Data Load Command: {command}")
        print("******************************************************************************************")

        command = f"sh /application/financedw/curated/scripts/load_curated.sh -s {source_system} -d contractorleainsurancetreatycompidassignment -f {batch_frequency} -j INITIAL_LOAD -g {source_flag}"
        log.info(f"Table contractorleainsurancetreatycompidassignment Completed Batch Data Load Command: {command}")
        result = subprocess.run(command, shell = True, capture_output = True, text = True)
        if result.stdout:
            log.info(result.stdout)
        if result.returncode != 0:
            log.error(result.stderr)
            raise Exception(f"Command failed with return code {result.returncode}: {result.stderr}")
        log.info("Table contractorleainsurancetreatycompidassignment Initial Completed Batch Load completed successfully")

    except Exception as ex:
        traceback.print_exc()
        log.exception(f"Failing the script: {ex}")
        exit(255)
    finally:
        log.info("Script completed")
        log.info("End: " + datetime.now(cst_tz).strftime("%Y-%m-%d %H:%M:%S"))
