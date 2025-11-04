import argparse
import psycopg2
import boto3
import os
import json
def get_secrets(db_name, secret_id, svc_acc_nm):
 try:
  secrets_json = {}
  secret_session = boto3.client('secretsmanager')
  response = secret_session.get_secret_value(SecretId=secret_id)
  secret_val = json.loads(response['SecretString'])
  secrets_json['host_name'] = secret_val[svc_acc_nm]['hostname']
  secrets_json['user_name'] = secret_val[svc_acc_nm]['username']
  secrets_json['password'] = secret_val[svc_acc_nm]['password']
  secrets_json['port'] = secret_val[svc_acc_nm]['port']
  return secrets_json
 except Exception as e:
  print("ERROR : Cannot retrieve the secrets " + str(e))
  raise
def create_db(db_name):
 # Initialising psycopg2 connection
 connection = None
 cursor = None
 try:
  connection = psycopg2.connect(host=secrets_json['host_name'], dbname='findw', user=secrets_json['user_name'], password=secrets_json['password'])
  connection.set_session(autocommit=True)
  cursor = connection.cursor()
  cmd = f"SELECT 1 FROM pg_catalog.pg_database WHERE datname= '{db_name}';"
  print(cmd)
  cursor.execute(cmd)
  exists = cursor.fetchone()
  print(exists)
  if not exists:
   print(f"Creating database: {db_name}")
   cursor.execute(f"CREATE DATABASE {db_name};")
  else:
   print(f"Database : {db_name} already exists")
 except Exception as e:
  print(" ERROR : could not open psycopg2 connection" + str(e))
  raise
 finally:
  if cursor is not None:
   cursor.close()
  if connection is not None:
   connection.close()
def get_clusters():
 try:
  cluster_list = []
  namespace_list = []
  if os.path.isfile(script_path + "/findw/dba/inputs/cluster_identifiers.txt"):
   print("Cluster identifier file exists")
   clusterid_file_path = script_path + "/findw/dba/inputs/cluster_identifiers.txt"
  else:
   exit('Not a valid cluster id file')
  print("clusterid_file_path " + clusterid_file_path)
  with open(clusterid_file_path, 'r') as file:
   cluster_ids = file.readlines()
   for cluster_id in cluster_ids:
    cluster_id = str(cluster_id.rstrip('\n')).format(env=args.env)
    cluster_list.append(cluster_id)
  for a in cluster_list:
   redshift = boto3.client('redshift')
   a1 = redshift.describe_clusters(ClusterIdentifier=a)["Clusters"][0]["ClusterNamespaceArn"].split(':')
   namespace_list.append(a1[6])
  return namespace_list
 except Exception as e:
  print(f"ERROR : Redshift Cluster {a} is missing or shutdown. Exception is - " + str(e))
  pass
def run_sql_file_dba(cursor, file_path, env, redenv, iam_arn, project_name, project_db, db_name, cluster_namespace=None):
 print(f"Executing sql file : {file_path}")
 with open(file_path, 'r') as f:
  sqlfile = f.read()
 try:
  if 'v_generate_' in file_path:
   cursor.execute(sqlfile)
  else:
   cursor.execute(sqlfile.format(env=env, redenv=redenv, iam_arn=iam_arn, project_name=project_name, project_db=project_db, db_name=db_name, cluster_namespace=cluster_namespace))
 except Exception as e:
  if '/findw/dba/views/financedw_dba_v_' in file_path:
   print("DBA View has an error but Skipping. The Error is: ", e)
   pass
  else:
   raise e
def run_sql_file(cursor, file_path, env, redenv, iam_arn, project_name, project_db, db_name, cluster_namespace=None):
 print(f"Executing sql file : {file_path}")
 with open(file_path, 'r') as f:
  sqlfile = f.read()
 if '/findw/ddls/copy/' in file_path or '/findw/dba/' in file_path:
  cursor.execute(sqlfile.format(env=env, redenv=redenv, iam_arn=iam_arn, project_name=project_name, project_db=project_db, db_name=db_name, cluster_namespace=cluster_namespace))
 else:
  cursor.execute(sqlfile)
# Parsing the arguments (received from Jenkins CI/CD JOB)
parser = argparse.ArgumentParser()
parser.add_argument('--secret_id', help="Provide secret id for db credentials", required=True)
parser.add_argument('--svc_acc_nm', help="Provide secret id for db credentials", required=True)
parser.add_argument('--create_db', required=True, choices=['y', 'n'], help="Create database")
parser.add_argument('--db_name', required=True, help="Name of the database")
parser.add_argument('--env', required=True, help="Environment w.r.t AWS")
parser.add_argument('--iam_arn', required=True, help="IAM ARN for ta-individual-findw-<env>-redshift")
parser.add_argument('--conf', required=True, help="Name of the configuration file to run the redshift operations")
args = parser.parse_args()
secrets_json = get_secrets(args.db_name, args.secret_id, args.svc_acc_nm)
# Getting script execution path and deriving the config absolute path
script_path = os.path.dirname(os.path.abspath(__file__))
if '.config' not in str(args.conf):
 print(f'Input configuration file [{args.conf}] is not valid')
 exit('Not a valid config file')
if os.path.isfile(script_path + f"/configs/{args.conf}"):
 print(f"Deploy/Rollback Configuration exists for : {args.db_name}")
 config_file_path = script_path + f"/configs/{args.conf}"
else:
 exit('Not a valid config file')
print("config_file_path " + config_file_path)
if args.create_db == 'y':
 create_db(args.db_name)
 print('Completed Creating Database')
print('Running sql files listed in deploy configuration')
connection = None
cursor = None
try:
 connection = psycopg2.connect(host=secrets_json['host_name'], dbname=args.db_name, user=secrets_json['user_name'], password=secrets_json['password'], port=secrets_json['port'])
 connection.set_session(autocommit=True)
 cursor = connection.cursor()
 # deriving project name from db name
 if(args.db_name == 'findw'):
  project_name = 'financedw'
  project_db = ''
 else:
  project_name = 'financedw' + str(args.db_name).split('dw')[-1]
  project_db = '_' + project_name
 cluster_namespace = None
 cluster_namespace = get_clusters()
 if config_file_path != script_path + "/configs/deploy_dba_actions.config":
  with open(config_file_path, 'r') as file:
   sql_file_paths = file.readlines()
   for sql_file_path in sql_file_paths:
    sql_file_path = script_path + sql_file_path.strip()
    print("sql_file_path start run " + sql_file_path)
    run_sql_file(cursor, sql_file_path, args.env, args.redenv, args.iam_arn, project_name, project_db, args.db_name, cluster_namespace)
 dba_config_file_path = script_path + "/configs/deploy_dba_actions.config"
 print("Rebasing and Executing dba procedures from - ", dba_config_file_path)
with open(dba_config_file_path, 'r') as file:
 sql_file_paths = file.readlines()
 for sql_file_path in sql_file_paths:
  sql_file_path = script_path + sql_file_path.strip()
  print("sql_file_path start run " + sql_file_path)
  run_sql_file_dba(cursor, sql_file_path, args.env, args.redenv, args.iam_arn, project_name, project_db, args.db_name, cluster_namespace)
 sql_file_path = script_path + '/findw/dba/alter/financedw_dba_procedures.sql'
 run_sql_file(cursor, sql_file_path, args.env, args.redenv, args.iam_arn, project_name, project_db, args.db_name, cluster_namespace)
 try:
  if cluster_namespace is not None:
   print('Executing grant procedures for datashare')
   sql_file_path = script_path + '/findw/dba/alter/grants_datashare.sql'
   for i in range(len(cluster_namespace)):
    run_sql_file(cursor, sql_file_path, args.env, args.redenv, args.iam_arn, project_name, project_db, args.db_name, cluster_namespace[i])
  else:
   print("Redshift Cluster's NameSpace Value is None (since its shutdown) so skipping Datashare Grants")
 except Exception as e:
  print("Skipped Grants for DataShare. Exception is " + str(e))
  pass
except Exception as e:
 print(" ERROR : could not open psycopg2 connection" + str(e))
 raise
finally:
 if cursor is not None:
  cursor.close()
 if connection is not None:
  connection.close()
