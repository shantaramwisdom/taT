import os
# import os for filesystem operations
base = r'C:\Users\shany\OneDrive\Desktop\findw-scripts\airflow'
# base folder where folders/files will be created (no trailing backslash)
folders_and_files = {
# DAG folder structure (relative to base)
"dags/aah": ["aah_main_load.py"],
# aah DAG folder and file
"dags/alm": ["alm_gdq_quarterly_load.py", "alm_main_load.py"],
# alm DAG folder and files
"dags/bestow": ["bestow_main_load.py"],
# bestow DAG folder and file
"dags/configs": [
"config.ini",
"emr_commands.py",
"emr_params.py",
"emr_serverless_commands.py",
"emr_serverless_params.py",
"gdq.py",
"metadata.csv"
],
# configs folder and files
"dags/erpdw": [
"affinity_main_load.py",
"ahd_main_load.py",
"ahdfuturefirst_main_load.py",
"arr_contract_main_load.py",
"arr_main_load.py",
"clearwater_main_load.py",
"cyberlife0001lfrs17_main_load.py",
"cyberlife1001teblfrs17_main_load.py",
"cyberlife9701lfrs17_main_load.py",
"erpdw_vendor_main_load.py",
"findw_bestow_main_load.py",
"findw_ltcg_main_load.py",
"findw_ltcghybrid_main_load.py",
"findw_vantagep5_main_load.py",
"findw_vantagep6_main_load.py",
"findw_vantagep65_main_load.py",
"findw_vantagep75_main_load.py",
"fsp_main_load.py",
"geac_main_load.py",
"geacap_main_load.py",
"genesyslfrs17_main_load.py",
"horizonlfrs17_main_load.py",
"icosexaminerceded_main_load.py",
"icosexaminerdocdir_main_load.py",
"illumlfrs17_main_load.py",
"investranet_main_load.py",
"jewirebot_main_load.py",
"lifecommitlfrs17_main_load.py",
"lifepro109lfrs17_main_load.py",
"lifepro111lfrs17_main_load.py",
"lifepro119_main_load.py",
"mantislfrs17_main_load.py",
"mkdb_main_load.py",
"mlcslfrs17_main_load.py",
"murex_main_load.py",
"oracleap_escheatment_main_load.py",
"oracleap_main_load.py",
"oracleap_monthly_main_load.py",
"oracledmcs_main_load.py",
"oraclefah_main_load.py",
"oraclegl_main_load.py",
"p3dss_main_load.py",
"p5dss_main_load.py",
"p7dss_main_load.py",
"performanceplus_main_load.py",
"processoroneexton_main_load.py",
"processoroneplano_main_load.py",
"rebuild_curated_error.py",
"revport_main_load.py",
"sailpoint_main_load.py",
"tagetik_main_load.py",
"ta_main_load.py",
"tp_main_load.py",
"tpaclaims_main_load.py",
"tpadlfrs17_main_load.py",
"tpapiflfrs17_main_load.py",
"upcs_main_load.py",
"yardi_main_load.py"
],
# erpdw folder and files
"dags/finmod": [
"AL_datastage_crawler.py",
"datastage_crawler.py",
"emr_terminator.py",
"erpdw_emr_terminator.py",
"ingestion_load.py",
"ingestion_load_monthly.py",
"ingestion_oldfw.py",
"ingestion_oldfw_monthly.py"
],
# finmod folder and files
"dags/ltcg": ["ltcg_main_load.py", "ltcghybrid_main_load.py"],
# ltcg folder and files
"dags/p5": ["p5_gdq_daily_load.py", "p5_main_load.py"],
# p5 folder and files
"dags/p6": ["p6_gdq_daily_load.py", "p6_main_load.py"],
# p6 folder and files
"dags/p65": ["p65_gdq_daily_load.py", "p65_kc2_quarterly.py", "p65_main_load.py"],
# p65 folder and files
"dags/p75": ["p75_gdq_daily_load.py", "p75_main_load.py"],
# p75 folder and files
"dags/spl": ["spl_gdq_daily_load.py", "spl_main_load.py"],
# spl folder and files
"dags/sweepjob": ["sweepjob.py", "sweepjobforcecomplete.py"],
# sweepjob folder and files
"dags/utilities": [
"check_for_missing_batches.py",
"curated_error_json_correction.py",
"custom_dml_exec_financedw.py",
"drop_table.py",
"emr_serverless_dag.py",
"InitialLoad_contractplan.py",
"InitialLoad_reversemap.py",
"nydfs_purge.py",
"nydfs_purge_table.py",
"refresh_materialized_view.py",
"refresh_materialized_views.py",
"s3_parquet_merge.py",
"s3_parquet_merge_file_based.py",
"unload_a_table_to_lower_env.py",
"unload_all_tables_to_lower_env.py",
"unload_all_tables_weekly.py",
"update_monthend_or_weekend_dates.py"
],
# utilities folder and files
"dags/vantageone": [
"common_utils.py",
"custom_smtp_email.py",
"customtasksensor.py",
"emr_actions.py",
"emr_serverless.py"
]
# vantageone folder and files
}
# dictionary prepared from the screenshots (best-effort transcription)
for folder, files in folders_and_files.items():
# iterate each relative folder and file list
  dirpath = os.path.join(base, folder)
# build full directory path under base
  os.makedirs(dirpath, exist_ok=True)
# create directory (including parents) if missing
  for fname in files:
# iterate filenames for this folder
    full = os.path.join(dirpath, fname)
# build full path for the file to create
    if not os.path.exists(full):
# only create the file if it does not already exist
      open(full, "w", encoding="utf-8").close()
# create an empty file with utf-8 encoding
    print("Created or exists:", full)
# print the path created or already existing
