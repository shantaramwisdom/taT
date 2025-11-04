# ########################## DEVELOPMENT LOG #############################
# DESCRIPTION - Script to load Finance Curated tables from financedw tables
# USAGE 1: spark-submit /application/financedw/curated/scripts/utilities/load_curated_from_financedw.py
#          -s VantagePAS -d activity
# USAGE 2: spark-submit /application/financedw/curated/scripts/utilities/load_curated_from_financedw.py
#          -s all -d batchtracking
# USAGE 3: spark-submit /application/financedw/curated/scripts/utilities/load_curated_from_financedw.py
#          -s all -d gdqdeletes
# # 12/04/2023 : Sagar Sawant : Initial Development
# # 12/15/2023 : Prathyush Preemachandan : Enhancements for VantagePAS activity and logging mechanism for tracebility
# #######################################################################

import argparse
import traceback
import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from common_utils import *
path = os.path.dirname(os.path.realpath(__file__))
print("Current Directory Path - ", path)
parent = os.path.abspath(os.path.join(path, os.pardir))
print("Parent Directory Path - ", parent)
sys.path.append(parent)

def write_and_check(source_system_name, domain_name, curated_database, curated_table, finaldf, checks='Y', final_count=0):
 log.info(f"Data Read Started at : {datetime.now(est_tz).strftime('%Y-%m-%d %H:%M:%S')}")
 finaldf = finaldf.cache()
 start_time = datetime.now(est_tz)
 sourcecount = finaldf.count()
 end_time = datetime.now(est_tz)
 time_taken = calc_time_taken(start_time, end_time)
 log.info(f"Number of Records for {source_system_name} and {domain_name} in Source Table is: {sourcecount} at {datetime.now(est_tz).strftime('%Y-%m-%d %H:%M:%S')}. Time Taken: {time_taken}(h:m:s)")
 finaldf.repartition(2).write.mode('overwrite').insertInto('{0}.{1}'.format(curated_database, curated_table))
 end_time = datetime.now(est_tz)
 time_taken = calc_time_taken(start_time, end_time)
 log.info(f"Writen into {curated_database}.{curated_table} at {datetime.now(est_tz).strftime('%Y-%m-%d %H:%M:%S')}. Time Taken: {time_taken}(h:m:s)")
 if checks == 'Y':
  curcount = sourcecount
  if curcount != sourcecount:
   msg = f"Error: Count Mismatch between Source {source_system_name} / {domain_name} (cnt={sourcecount}) and Curated {curated_database}.{curated_table} (cnt={curcount}). Please Validate the Loads."
   log.error(msg)
   raise Exception(msg)
  log.info(f"Data Copy Completed For {0}".format(curated_table) + " at " + datetime.now(est_tz).strftime("%Y-%m-%d %H:%M:%S"))
 finaldf.unpersist()

if __name__ == '__main__':
 try:
  # Parse Command-line Arguments
  parser = argparse.ArgumentParser()
  parser.add_argument('--source_system_name','-s', required=True, type=str, dest='source_system_name')
  parser.add_argument('--domain_name','-d', required=True, type=str, dest='domain_name')
  log.info("Start: " + datetime.now(est_tz).strftime("%Y-%m-%d %H:%M:%S"))
  variables = dir()
  args = parser.parse_args()
  source_system_name = args.source_system_name
  domain_name = args.domain_name

  variables = common_vars(environment, project, source_system_name)
  curated_database = variables['curated']
  gdq_database = variables['gdq']
  findw_db = variables['findw_db']
  batchid = 0
  cycle_date = '9999-12-31'
  phase_name = 'DATA COPY'

  # Initialize Spark
  appname = f"Curated Load from financedw for {0}".format(source_system_name, domain_name)
  spark = SparkSession.builder.appName(appname).enableHiveSupport() \
   .config("hive.exec.dynamic.partition","true") \
   .config("hive.exec.dynamic.partition.mode","nonstrict").getOrCreate()
  sc = spark.sparkContext
  sc.setLogLevel("ERROR")
  spark.conf.set("spark.serialization","org.apache.spark.serializer.KryoSerializer")
  spark.conf.set("spark.sql.tungsten.enabled","true")
  spark.conf.set("spark.sql.broadcastTimeout","3000")
  spark.conf.set("spark.driver.maxResultSize","1844678800")
  spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed","true")
  spark.conf.set("spark.sql.adaptive.enabled","true")
  spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled","true")
  log.info(f"Source Database is: {gdq_database}")
  log.info(f"Target Database is: {curated_database}")

  log.info("Verifying batchtracking if Data load is already complete for {source_system_name} and {domain_name}")
  querybatchcount = "select count(*) from financedwcochestration.batch_tracking where source_system_name = '{source_system_name}' " \
                    "and domain_name = '{domain_name}' and batch_id = {batchid} and cycle_date = '{cycle_date}' and phase_name = '{phase_name}'"
  load_status = 'complete'
  result = get_jdbc_df(spark, 'findw', querybatchcount)
  count = result.collect()[0][0]
  if count != 0:
   batchtracking_logging(source_system_name, domain_name, batchid, 'in-progress', cycle_date, 'insert', phase_name, None, None, 'Y')
  else:
   log.info(f"Data is already loaded for {source_system_name} and {domain_name}. Skipping...Data Load.")
   sys.exit(0)

  if domain_name == 'batchtracking':
   curated_table = 'completedbatch'
   log.info(f"Data Copy Started For {0}".format(curated_table) + " at " + datetime.now(est_tz).strftime("%Y-%m-%d %H:%M:%S"))
   qry = "select batch_id, cycle_date, domain_name, source_system_name, insert_timestamp from " \
         "(select batch_id, cycle_date, domain_name, source_system_name order by insert_timestamp desc) as rnum " \
         "from financedwcochestration.batch_tracking where source_system_name in ('ALL','VantageP5','VantageP6','VantagePAS','VantageP7S') " \
         "and phase_name = 'dataverse' and load_status = 'complete' ) where rnum = 1"
   orcHdf = get_jdbc_df(spark, 'findw', qry)
   orcHdf = orcHdf.withColumn("batch_frequency", when(col("source_system_name") == "ALL","MONTHLY"))
   finaldf = orcHdf.select("batch_id","cycle_date",col("insert_timestamp").alias("recorded_timestamp"),"domain_name",col("source_system_name").alias("source_system"),"batch_frequency")
   finaldf.createOrReplaceTempView("recent_batches")
   write_and_check(source_system_name, domain_name, curated_database, curated_table, finaldf)

  elif domain_name == 'gdqdeletes':
   curated_table = 'curateddeletes'
   log.info(f"Data Copy Started For {0}".format(curated_table) + " at " + datetime.now(est_tz).strftime("%Y-%m-%d %H:%M:%S"))
   qry = "select tablename, batch_frequency, pointofreviewstopdate, domain, sourcesystemname, cycledate " \
         "from {0}.gdqdeletes where (gdqdeletes.sourcesystemname = 'ALL','MONTHLY')".format(gdq_database)
   gdqdeletes = spark.sql(qry)
   finaldf = gdqdeletes.withColumn("recorded_timestamp", from_utc_timestamp(current_timestamp(),'US/Central')) \
                       .withColumn("batch_id", col("batchid").alias("batch_id")) \
                       .withColumn("domain_name", col("domain")) \
                       .withColumn("source_system", col("sourcesystemname")) \
                       .select("recorded_timestamp","batch_id","domain_name","source_system","batch_frequency","cycledate")
   btqry = "select batch_id, cycle_date, domain_name, source_system_name, insert_timestamp from " \
           "(select batch_id, cycle_date, domain_name, source_system_name order by insert_timestamp desc) as rnum " \
           "from financedwcochestration.batch_tracking where source_system_name in ('ltcg','ltcghybrid') " \
           "and domain_name not in ('financecontract') and phase_name = 'datawarehouse' and load_status = 'complete' ) where rnum = 1"
   btdf = get_jdbc_df(spark, 'findw', btqry)
   btdf.createOrReplaceTempView("recent_batches")
   ltcg_gdq_database = gdq_database.replace('financedwcochestration','financedwgdq')
   qry = "select insert_timestamp, {0} documentid, cast(pointofreviewstopdate as date) as pointofreviewstopdate, batch_id, " \
         "tablename as domain_name, sourcesystem as source_system, 'DAILY' batch_frequency, cast(load_date as date) as cycle_date " \
         "from {0}.gdqdeletes a join redshift_batches b on sourcesystem = source_system and load_date = cycle_date and domain_name = tablename"
   tsc_gdqdeletes = spark.sql(qry)
   finaldf = finaldf.unionAll(tsc_gdqdeletes)
   write_and_check(source_system_name, domain_name, curated_database, curated_table, finaldf)

  else:
   curated_table = source_system_name.lower() + '_' + domain_name
   log.info(f"Data Copy Started For {0}".format(curated_table) + " at " + datetime.now(est_tz).strftime("%Y-%m-%d %H:%M:%S"))
   curschema = spark.sql("select * from {0}.{1}".format(curated_database, curated_table))
   curschema = curschema.withColumnRenamed("cycle_date","cycledate") \
                        .withColumnRenamed("batch_id","batchid") \
                        .drop("recorded_timestamp")
   selectcmd = "select "
   for key in curschema.columns:
    selectcmd = selectcmd + f"{key},"
   qry = selectcmd + " insert_timestamp from financedw.{0} where sourcesystemname = '{1}'".format(domain_name, source_system_name)
   prior_dt = '2019-01-01'
   fetchsize = 1000000
   intra_count = 0
   cnt = 0

   if source_system_name == 'VantagePAS' and domain_name == 'activity':
    dates = ['2019-12-31','2020-12-31','2021-12-31','2022-12-31', datetime.now(est_tz).strftime('%Y-%m-%d')]
    for d in dates:
     prefix = f" and cycledate > '{prior_dt}' and cycledate <= '{d}' "
     log.info(f"Additional Prefix Added: prior_dt = {prior_dt}, new = {prefix}")
     qry2 = qry + prefix
     financedwf = get_jdbc_df(spark, 'findw', qry2, None, fetchsize, 10, 'cycledate', prior_dt, datetime.now(est_tz).strftime('%Y-%m-%d'))
     finaldf = financedwf.select(col("insert_timestamp").alias("recorded_timestamp"), '*')
     finaldf = finaldf.drop("insert_timestamp")
     write_and_check(source_system_name, domain_name, curated_database, curated_table, finaldf, checks='Y')
     cnt = cnt + 1
    if cnt == len(dates):
     write_and_check(source_system_name, domain_name, curated_database, curated_table, finaldf, checks='N', final_count=cnt)
   else:
    financedwf = get_jdbc_df(spark, 'findw', qry, None, fetchsize, 10, 'cycledate', prior_dt, datetime.now(est_tz).strftime('%Y-%m-%d'))
    finaldf = financedwf.select(col("insert_timestamp").alias("recorded_timestamp"), '*')
    finaldf = finaldf.drop("insert_timestamp")
    write_and_check(source_system_name, domain_name, curated_database, curated_table, finaldf)

  batchtracking_logging(source_system_name, domain_name, batchid, 'complete', cycle_date, 'update', phase_name, None, None, 'Y')

 except Exception as ex:
  traceback.print_exc()
  log.exception(f"Failing the script due to following Exception : {ex}")
  log.info("End: " + datetime.now(est_tz).strftime("%Y-%m-%d %H:%M:%S"))
  exit(255)
 finally:
  log.info("Script completed")
  log.info("End: " + datetime.now(est_tz).strftime("%Y-%m-%d %H:%M:%S"))
