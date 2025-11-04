import traceback
import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F

path = os.path.dirname(os.path.realpath(__file__))
print("Current Directory Path -", path)
parent = os.path.abspath(os.path.join(path, os.pardir))
print("Parent Directory Path -", parent)
sys.path.append(parent)

from common_utils import *

source_system_name_list = ['ahd','tatasig','lifepro109ifrs17','lifepro119']
domain_name_list = ['general_ledger_line_item','general_ledger_header']

field_value_dict = {'ahd': {'src_desc': 'regular'},
 'tatasig': {'src_desc': 'regular'},
 'lifepro109ifrs17': {'src_desc': 'regular'},
 'lifepro119': {'src_desc': 'regular'}}

field_logic_dict = {
 'ahd_general_ledger_line_item': {
 'statutoryresidentstatecode' : 'case when parsed_json.gl_source_code = ''EX'' then parsed_json.statutoryresidentstatecode else parsed_json.statutoryresidentcountrycode end',
 'activitywithholdingtaxjurisdiction': 'case when parsed_json.gl_source_code = ''EX'' then parsed_json.activitywithholdingtaxjurisdiction else parsed_json.activitywithholdingtaxjurisdictioncountrycode end'},
}

source_system_name_list = ['processoroneplano','ahd','cyberlife0001tebifrs17']
domain_name_list = ['general_ledger_line_item']

field_value_dict = {
 'processoroneplano': {'src_desc': 'regular'}}

field_logic_dict = {
 'processoroneplano_general_ledger_line_item': {
 'orig_gl_center': 'case when reprocess_flag = ''Y'' then parsed_json.statutoryresidentstatecode else parsed_json.orig_gl_center end'},
 'ahd_general_ledger_line_item': {
 'temp_secondary_ledger_code': 'case when reprocess_flag = ''Y'' and error_record_aging_days > 0 then parsed_json.secondary_ledger_code else NULL end',
 'temp_activityreversalcode': 'case when reprocess_flag = ''Y'' and error_record_aging_days > 0 then parsed_json.activityreversalcode else NULL end',
 'temp_data_type': 'case when reprocess_flag = ''Y'' and error_record_aging_days > 0 then parsed_json.data_type else NULL end',
 'secondary_ledger_code': 'case when reprocess_flag = ''Y'' and error_record_aging_days > 0 then parsed_json.temp_secondary_ledger_code else parsed_json.secondary_ledger_code end',
 'activityreversalcode': 'case when reprocess_flag = ''Y'' and error_record_aging_days > 0 then parsed_json.temp_data_type else parsed_json.activityreversalcode end',
 'data_type': 'case when reprocess_flag = ''Y'' and error_record_aging_days > 0 then parsed_json.temp_secondary_ledger_code else parsed_json.data_type end'},
 'cyberlife0001tebifrs17_general_ledger_line_item': {
 'pincode': 'case when reprocess_flag = ''R'' then parsed_json.pincode_dwd else parsed_json.pincode end',
 'pincode_dwd': 'case when reprocess_flag = ''R'' then NULL end'},
}

field_rename_dict = {
 'table_name': {'old_field': 'new_field'},
}

cycle_date = '9999-12-31'
batchid = 9999

if __name__ == '__main__':
 try:
  log.info("Start: " + datetime.now(est_tz).strftime("%Y-%m-%d %H:%M:%S"))

  appname = 'Curated Error Rebuild'
  spark = SparkSession.builder.appName(appname).enableHiveSupport() \
   .config("hive.exec.dynamic.partition", "true") \
   .config("hive.exec.dynamic.partition.mode", "nonstrict").getOrCreate()

  sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  variables = common_vars('NA')
  curated_database = variables['curated']

  log.info(f"Curated Database - {curated_database}")

  for source_system_name in source_system_name_list:
   for domain_name in domain_name_list:
    print(f"===================={source_system_name}.{domain_name}====================")
    try:
     curated_table_name = f"{source_system_name}_{domain_name}"
     field_dict = field_value_dict.get(source_system_name)
     sql = f"select * from {curated_database}.curated_error where table_name = '{curated_table_name}'"
     error_df = spark.sql(sql).cache()
     error_df_cnt = error_df.count()
     has_input = False
     if error_df_cnt > 0:
      if field_dict or curated_table_name in field_rename_dict or curated_table_name in field_logic_dict:
       has_input = True
       log.info(f"Found {error_df_cnt} Error Records for {curated_table_name}")
       curated_error_backup(spark, cycle_date, batchid, curated_database, curated_table_name, None, 'full_backup')
      if field_dict:
       for field_name, field_value in field_dict.items():
        print(f"Updating Error {curated_table_name} with fields: {field_dict}")
        log.info(f"Updating Error {curated_table_name} with fields: {field_dict}")
        error_df = add_field_to_error_record(spark, error_df, field_name, field_value)
      else:
       log.info(f"No action Taken for Field Addition for Error table {curated_table_name}")
      print("===================================================")
      if curated_table_name in field_rename_dict:
       log.info(f"Updating Error {curated_table_name} with fields: {field_rename_dict[curated_table_name]}")
       field_mapping = field_rename_dict[curated_table_name]
       error_df = rename_field_in_error_record(spark, error_df, field_mapping)
      else:
       log.info(f"No action Taken for Field Rename for Error table {curated_table_name}")
      print("===================================================")
      if curated_table_name in field_logic_dict:
       log.info(f"Updating Error {curated_table_name} with Fields Logic: {field_logic_dict[curated_table_name]}")
       logic_dict = field_logic_dict[curated_table_name]
       for field_name, field_logic in logic_dict.items():
        error_df = update_field_with_logic_in_error_record(spark, error_df, field_name, field_logic, True)
      else:
       log.info(f"No action Taken using Field Logic for Error table {curated_table_name}")
      if has_input:
       error_df.repartition(2).write.mode("overwrite").insertInto(f"{curated_database}.curated_error")
       log.info(f"Updated Error {curated_table_name}")
       new_error_df_cnt = spark.sql(sql).count()
       if error_df_cnt != new_error_df_cnt:
        log.error(f"Data got corrupted.. Please restore from Earlier Backup")
        raise Exception(f"Error Count Mismatch for {curated_table_name}. Old Count {error_df_cnt} and new Count {new_error_df_cnt}")
       else:
        log.info(f"Successfully updated Error {curated_table_name}")
       error_df.unpersist()
      else:
       log.info(f"No Error Records Found for {curated_table_name}")
    except Exception as ex:
     traceback.print_exc()
     log.error(f"Failed to process {curated_table_name}: {ex}")
     continue

  sql = f"select * from {curated_database}.curated_error"
  curated_df = spark.sql(sql).cache()
  curated_df_cnt = curated_df.count()
  if curated_df_cnt > 0:
   print("===================================================")
   if field_dict:
    log.info(f"Found {curated_df_cnt} Curated Records for {curated_table_name}")
    curated_table_location = glue_table_location(curated_database, curated_table_name)
    tsmp = spark.sql("select date_format(from_utc_timestamp(CURRENT_TIMESTAMP, 'US/Central'),'yyyyMMddHHmmss')").collect()[0][0]
    tgt_loc = f"s3://as_extract_bucket/miscellaneous/{project}/curated/table_backup/{curated_table_name}/{tsmp}/"
    log.info(f"Backing up curated table to {tgt_loc}")
    loc = curated_table_location
    command = f"aws s3 sync --delete --only-show-errors {loc} {tgt_loc}"
    log.info(f"Backup Command: {command}")
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    if result.stdout:
     log.info(result.stdout)
    if result.returncode != 0:
     log.error(f"Backup failed: {result.stderr}")
     raise Exception(f"Curated Table Backup Failed")
    else:
     log.info(f"Successfully backed up {curated_table_name}")
    for field_name, field_value in field_dict.items():
     curated_df = curated_df.withColumn(field_name, F.lit(field_value))
    curated_df.repartition(2).write.mode("overwrite").insertInto(f"{curated_database}.{curated_table_name}")
    new_curated_df_cnt = spark.sql(sql).count()
    if curated_df_cnt != new_curated_df_cnt:
     log.error(f"Data got corrupted.. Please restore from Earlier Backup")
     raise Exception(f"Curated Table Count Mismatch for {curated_table_name}. Old Count {curated_df_cnt} and new Count {new_curated_df_cnt}")
    else:
     log.info(f"Successfully updated {curated_table_name}")
    curated_df.unpersist()
   else:
    log.info(f"No action Taken for Field Addition for Curated table {curated_table_name}")
  else:
   log.info(f"No Curated Records Found for {curated_table_name}")
 except Exception as ex:
  traceback.print_exc()
  log.exception(f"Failing the script due to Following Exception: {ex}")
  log.info("End: " + datetime.now(est_tz).strftime("%Y-%m-%d %H:%M:%S"))
  sys.exit(255)
 finally:
  print("===================================================")
  log.info("Script completed")
  log.info("End: " + datetime.now(est_tz).strftime("%Y-%m-%d %H:%M:%S"))
