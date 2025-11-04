import argparse
import os
parser = argparse.ArgumentParser()
parser.add_argument('--db_name', required=True, help='Name of the database')
parser.add_argument('--conf', required=True, help='Name of the configuration file to run the redshift operations')
args = parser.parse_args()
# Getting script execution path and deriving the config absolute path
script_path = os.path.dirname(os.path.abspath(__file__))
if '.config' not in str(args.conf):
 print(f'Input configuration file [{args.conf}] is not valid')
 exit('Not a valid config file')
if os.path.isfile(script_path + f"/configs/{args.conf}"):
 print(f"Deploy/Rollback Configuration exists for : {args.db_name}")
 config_file_path = script_path + f"/configs/{args.conf}"
else:
 exit('Config file does not exist')
print("config_file_path : " + config_file_path)
with open(config_file_path, 'r') as file:
 sql_file_paths = file.readlines()
 for sql_file_path in sql_file_paths:
  sql_file_path = script_path + sql_file_path.strip()
  print("sql_file_path start run : " + sql_file_path)
  # validate if file exists
  try:
   sqlfile = open(sql_file_path, 'r')
   print("INFO: File exists : " + sql_file_path)
  except Exception:
   print(" ERROR : File does not exist for : " + sql_file_path)
   raise Exception(" ERROR : File does not exist for : " + sql_file_path)
try:
 print("Evaluating alter dba procedure")
 sql_file_path = script_path + '/findw/dba/alter/financedw_dba_procedures.sql'
 sqlfile = open(sql_file_path, 'r')
 print("INFO: File exists " + sql_file_path)
except Exception:
 print(" ERROR : File does not exist for : " + sql_file_path)
 raise Exception(" ERROR : File does not exist for : " + sql_file_path)
