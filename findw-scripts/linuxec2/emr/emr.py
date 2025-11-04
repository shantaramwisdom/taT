import json
import os
import re
import sys
import aws_utils
import random
import subprocess
import apscheduler.schedulers.background
import psutil
from botocore.exceptions import ClientError
from datetime import datetime, date
from botocore.client import Config
region = 'us-east-1'
config = Config(retries={'max_attempts': 15, 'mode': 'adaptive'})
class EMR:
    def __init__(self, args):
        self.args = args
        self.emr_config = None
        self.emr_name = None
        self.emr_client = boto3.client('emr', region_name=region, config=config)
        self.s3_resource = boto3.resource('s3', region_name=region, config=config)
        self.ssm_client = boto3.client('ssm', region_name=region, config=config)
        self.dir_name = os.path.dirname(__file__)
        self.class_emr_cluster = []
        self.sched = apscheduler.schedulers.background.BackgroundScheduler({'apscheduler.job_defaults.max_instances': 3, 'apscheduler.timezone': 'America/New_York'})
        timestamp = datetime.now()
        self.cur_day_format = timestamp.strftime("%Y-%m-%d_%H-%M-%S")
    def update_ip_address(self, self_cluster_id):
        ip_list = []
        bucket_name = f"ta-individual-findw-{self.args['env']}-codedeployment"
        s3_log_bucket = f"ta-individual-findw-{self.args['env']}-logs"
        s3_bucket_prefix = f"Inhive/EC2Logs/{date.today()}"
        ec2_instance_id = os.popen(f"aws ec2 describe-instances --filters 'Name=tag:Name,Values=ta-technology-emap-lx-dei-{self.args['env']}' --output text --query 'Reservations[*].Instances[*].InstanceId'").read()
        ec2_instance_id = ec2_instance_id.strip()
        if self.args['env'] == 'dev' or self.args['env'] == 'tst':
            dns_domain = 'datalake.tanorpod.aegon.io'
        elif self.args['env'] == 'mdl':
            dns_domain = 'datalake.model.aegon.io'
        elif self.args['env'] == 'prd':
            dns_domain = 'datalake.aegon.io'
        for instance_group in ['MASTER', 'CORE']:
            print(f"Instance group is {instance_group}")
            instances = self.emr_client.list_instances(
                ClusterId=self_cluster_id, InstanceGroupTypes=[instance_group])
            for instance in instances['Instances']:
                ip_list.append(instance['PrivateIpAddress'] + " " + instance['PrivateDnsName'].replace('ec2.internal', 'us.aegon.com') + " " + self.args['project'])
                if instance_group == 'MASTER':
                    master_ip = instance['PrivateIpAddress']
        ip_string = ",".join(ip_list)
        if self.args['source_system'] == 'other':
            bde_flag = 'Y'
        else:
            bde_flag = 'N'
        command = f"sh /efs/informatica/TIME_DQ/Bde/update_ip.sh {ip_string} {master_ip} {self.args['project']} {self.args['env']} {bde_flag}"
        print(command)
        try:
            response = self.ssm_client.send_command(
                InstanceIds=[ec2_instance_id],
                DocumentName='AWS-RunShellScript',
                Parameters={'commands': [command]},
                OutputS3BucketName=s3_log_bucket,
                OutputS3KeyPrefix=s3_bucket_prefix
            )
        except Exception as e:
            print(f"The following exception occurred for SSM to run update ip script in EMAP Server ta-technology-emap-lx-dei-{self.args['env']} with instance ID {ec2_instance_id}")
            raise e
        ssm_wait_status = aws_utils.wait_ssm_command(
            response['Command']['CommandId'], ec2_instance_id, self.ssm_client, s3_log_bucket)
        print(f"SSM Wait status: {ssm_wait_status}")
        if not ssm_wait_status:
            print("Update ip Script failed in the BDE EC2 Server")
            sys.exit(90)
    def build_emr_config(self):
        if self.args['jenkins']:
            with open(f"{self.dir_name}/config/{self.args['env']}/jenkins/all_emr_config.json") as f:
                emr_config = json.load(f)
        elif self.args['step'] == 'curated' and (self.args['source_system'] == 'all' or self.args['source_system'] is None):
            with open(f"{self.dir_name}/config/{self.args['env']}/curated/all_emr_config.json") as f:
                emr_config = json.load(f)
        elif self.args['step'] == 'curated' and self.args['source_system'] is not None:
            try:
                with open(f"{self.dir_name}/config/{self.args['env']}/{self.args['source_system']}/all_emr_config.json") as f:
                    emr_config = json.load(f)
            except Exception:
                with open(f"{self.dir_name}/config/{self.args['env']}/curated/all_emr_config.json") as f:
                    emr_config = json.load(f)
        else:
            with open(f"{self.dir_name}/config/{self.args['env']}/{self.args['step']}/{self.args['source_system']}_emr_config.json") as f:
                emr_config = json.load(f)
        with open(f"{self.dir_name}/config/tags.json") as f:
            tag_json = json.load(f)
        emr_config = {"emr_config": emr_config, "tag_json": tag_json}
        if self.args['jenkins'] and (emr_config['emr_config']['release_label'] != 'emr-6.15.0' or not self.args['jenkins']):
            ovrrd_emr_version = None
            file_path = "/mnt/FINDW/emr/emr_version.txt"
            try:
                with open(file_path, 'r') as file:
                    ovrrd_emr_version = file.readline().strip().strip('\"')
            except FileNotFoundError:
                print(f"Error: The file {file_path} was not found.")
            except Exception as e:
                print(f"An error occurred: {e}")
            if ovrrd_emr_version and re.match(r'emr-\d+\.\d+\.\d+$', ovrrd_emr_version) and emr_config['emr_config']['release_label'] != ovrrd_emr_version:
                emr_config['emr_config']['release_label'] = ovrrd_emr_version
                print(f"EMR Version Override {ovrrd_emr_version}")
        temp_dict = {}
        temp_dict['project'] = self.args['project']
        temp_dict['env'] = self.args['env']
        temp_dict['step'] = self.args['step']
        temp_dict['source_system'] = self.args['source_system']
        temp_dict['cur_day_format'] = self.cur_day_format
        temp_dict['emr_name'] = self.emr_name
        if not self.args['cycle_date']:
            temp_dict['cycle_date'] = datetime.now()
        else:
            try:
                temp_dict['cycle_date'] = datetime.strptime(self.args['cycle_date'], "%Y%m%d")
            except Exception:
                print(f"Not a valid cycle date: {self.args['cycle_date']}")
                temp_dict['cycle_date'] = datetime.now()
        temp_dict['cycle_date'] = datetime.strftime(temp_dict['cycle_date'], "%Y-%m-%d")
        print(f"Cycle date: {temp_dict['cycle_date']}")
        if self.args['step'] == 'BDE':
            if self.args['source_system'] == 'other':
                temp_dict['bde_flag'] = 'Y'
                temp_dict['bde_other_flag'] = 'Y'
            else:
                temp_dict['bde_flag'] = 'Y'
                temp_dict['bde_other_flag'] = 'N'
        else:
            temp_dict['bde_flag'] = 'N'
            temp_dict['bde_other_flag'] = 'N'
        emr_config_str = json.dumps(emr_config)
        for k, v in temp_dict.items():
            placeholder = "<" + k + ">"
            emr_config_str = emr_config_str.replace(placeholder, v)
        self.emr_config = json.loads(emr_config_str)
        print(self.emr_config)
        def sched_check_emr(self):
        print("########################RUN-APSCHEDULER-CHECK-EMR-EXISTENCE########################")
        print("Duplicate EMR'S presence will be Checked Every 134 Seconds")
        print("########################RUN-APSCHEDULER-CHECK-EMR-EXISTENCE########################")
        try:
            self.sched.add_job(self.check_emr, 'interval', seconds=134)
        except Exception as ex:
            print("########################RUN-APSCHEDULER-CHECK-EMR-EXISTENCE-EXCEPTION########################")
            print("The Following Exception Occured in apscheduler but passed - ", ex)
            print("########################RUN-APSCHEDULER-CHECK-EMR-EXISTENCE-EXCEPTION########################")
            pass
    def check_emr(self):
        print("########################CHECKING-EMR-EXISTENCE########################")
        print(f"Checking EMR (Duplicate) existence at {datetime.now()}")
        cluster_final_invalid_status = ['TERMINATED', 'TERMINATING', 'TERMINATED_WITH_ERRORS']
        emr_clusters = self.emr_client.list_clusters(
            ClusterStates=['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING']
        )
        cluster_id = []
        emr_name_hyphen = self.emr_name + '-'
        for cluster in emr_clusters['Clusters']:
            cluster_status = cluster['Status']['State']
            if emr_name_hyphen in cluster['Name']:
                self.classs_emr_cluster.append(cluster['Id'])
                self.classs_emr_cluster = list(dict.fromkeys(self.classs_emr_cluster))
                if self.args['action'] == 'terminate':
                    cluster_id.append(cluster['Id'])
            else:
                if len(self.classs_emr_cluster) > 1:
                    print("More than one emr exists for the given emr configuration. All following EMRs are Killed", self.classs_emr_cluster)
                    self.sched.shutdown(wait=False)
                    for emr_id in self.classs_emr_cluster:
                        aws_utils.terminate_cluster(emr_id, self.emr_client)
                    psutil.Process().send_signal(10)
        while True:
            print("Loop Status: " + cluster_status)
            if cluster_status in ['WAITING', 'RUNNING']:
                print("In waiting/running state")
                break
            else:
                print("Cluster not in waiting/running status- Sleeping 60 secs")
                time.sleep(60)
            print("***********************************************************")
            print("***********************************************************")
            cluster_status = aws_utils.describe_cluster(
                cluster['Id'], self.emr_client)
            cluster_status = cluster_status.strip()
            if cluster_status in cluster_final_invalid_status:
                print(f"EMR entered the final invalid status - ({cluster_status})")
                self.sched.shutdown(wait=False)
                psutil.Process().send_signal(10)
                sys.exit(10)
            cluster_id.append(cluster['Id'])
        print(f"Total clusters found: {len(cluster_id)}")
        print(cluster_id)
        if cluster_id:
            return True, cluster_id
        else:
            return False, None
    def wait_for_steps_completion(self, cluster_id, step_ids):
        step_final_status = ['COMPLETED']
        completed_count = 0
        step_status = None
        while True:
            time.sleep(60)
            print("***********************************************************")
            print("***********************************************************")
            try:
                for step_id in step_ids:
                    print(f"Checking Step Id: {step_id}")
                    step_status = aws_utils.describe_step(cluster_id, step_id, self.emr_client)
                    if step_status in ['FAILED', 'CANCELLED']:
                        print(f"Bootstrap step {step_id} failed with status {step_status}")
                        aws_utils.terminate_cluster(cluster_id, self.emr_client)
                        sys.exit(99)
                    if step_status in step_final_status:
                        completed_count += 1
                    if completed_count == len(step_ids):
                        print("All bootstrap steps completed")
                        break
            except ClientError as error:
                print("Exception occurred")
                if error.response['Error']['Code'] == 'ThrottlingException':
                    print("ThrottlingException- Continuing creating EMR..")
                    continue
            return step_status
    def create_jenkins_emr(self):
        self.build_emr_config()
        ami_id = subprocess.check_output("aws ec2 describe-images --filters 'Name=name,Values=ENCRYPTED-AL2-EMR-*' 'Name=architecture,Values=x86_64' 'Name=virtualization-type,Values=hvm' 'Name=root-device-type,Values=ebs' --query 'sort_by(Images, &CreationDate)[-1].ImageId'", shell=True, universal_newlines=True)
        ami_id = ami_id.strip().strip('"')
        ami_name = subprocess.check_output("aws ec2 describe-images --filters 'Name=name,Values=ENCRYPTED-AL2-EMR-*' 'Name=architecture,Values=x86_64' 'Name=virtualization-type,Values=hvm' 'Name=root-device-type,Values=ebs' --query 'sort_by(Images, &CreationDate)[-1].Name'", shell=True, universal_newlines=True)
        ami_name = ami_name.strip().strip('"')
        if "null" in ami_id:
            print("GTS AMI is not Available for EMR so defaulting to AMAZON's AMI")
            ami_id = ""
        else:
            print("GTS AMI ID for EMR is: ", ami_id)
            print("GTS AMI Name for EMR is: ", ami_name)
        cluster_id = aws_utils.run_job_flow_fleet(
            self.emr_config, self.emr_client, True, ami_id)
        self.classs_emr_cluster.append(cluster_id)
        bucket_name = f"ta-individual-findw-{self.args['env']}-codedeployment"
        step_id_list = []
        emr_version = self.emr_config.get('release_label', '')
        emr_major_version = int(emr_version.split('.')[1].split('.')[0]) if emr_version.startswith('emr-') else 0
        if emr_major_version >= 7:
            print(f"EMR version {emr_version} detected - adding bootstrap steps")
            prefix = "miscellaneous/bootstrap-action/emr_steps/"
            script_args = ''
            s3_bucket = self.s3_resource.Bucket(bucket_name)
            for sh_file_obj in s3_bucket.objects.filter(Prefix=prefix):
                if '.sh' in sh_file_obj.key:
                    sh_file = sh_file_obj.key
                    script_uri = f"s3://{bucket_name}/{sh_file}"
                    name = sh_file.split('/')[-1].upper()
                    action_on_failure = 'CONTINUE'
                    step_id = aws_utils.add_step_script_runner(
                        cluster_id,
                        name,
                        script_uri,
                        script_args,
                        self.emr_client,
                        action_on_failure
                    )
                    step_id_list.append(step_id)
            print("Waiting for bootstrap steps to complete...")
            step_status = self.wait_for_steps_completion(cluster_id, step_id_list)
            print("Bootstrap steps completed. Adding jenkins job step...")
        else:
            print(f"EMR version {emr_version} detected - skipping bootstrap steps (only for EMR 7+)")
            if self.args['jenkins_job'] == 'create_db':
                sh_file = "miscellaneous/bootstrap-action/jenkins_steps/dd1_runner.sh"
                script_args = f"{self.args['project']} {self.args['env']} {self.args['step']}"
            elif self.args['jenkins_job'] == 'deploy_config':
                sh_file = "miscellaneous/bootstrap-action/jenkins_steps/dd1_deploy_config.sh"
                script_args = f"{self.args['project']} {self.args['env']} {self.args['step']}"
            elif self.args['jenkins_job'] == 'dropdb':
                sh_file = "miscellaneous/bootstrap-action/jenkins_steps/drop_db.sh"
                script_args = f"{self.args['jenkins_params']}"
            step_id_list = []
            script_uri = f"s3://{bucket_name}/{sh_file}"
            name = sh_file.split('/')[-1].upper()
            action_on_failure = 'CONTINUE'
            step_id = aws_utils.add_step_script_runner(
                cluster_id,
                name,
                script_uri,
                script_args,
                self.emr_client,
                action_on_failure
            )
            step_id_list.append(step_id)
        print("Waiting for Jenkins steps to complete...")
        step_status = self.wait_for_steps_completion(cluster_id, step_id_list)
        print("Jenkins steps completed...")
        if step_status == 'COMPLETED':
            aws_utils.terminate_cluster(cluster_id, self.emr_client)

    def get_instance_config_weight(self, emr_config_core, fleet_instance_type):
        instance_weight = 0
        print(f"Fleet instance type: {fleet_instance_type}")
        for instance_config in emr_config_core:
            if instance_config['InstanceType'] == fleet_instance_type:
                instance_weight = instance_config['WeightedCapacity']
                print(f"Instance weight for {fleet_instance_type}: {instance_weight}")
        return instance_weight
    def create_emr(self):
        self.build_emr_config()
        if 'Managed_Scaling_Policy' in self.emr_config:
            print("Managed Scaling policy already exists")
        else:
            self.emr_config['Managed_Scaling_Policy'] = {}
        ami_id = subprocess.check_output("aws ec2 describe-images --filters 'Name=name,Values=ENCRYPTED-AL2-EMR-*' 'Name=architecture,Values=x86_64' 'Name=virtualization-type,Values=hvm' 'Name=root-device-type,Values=ebs' --query 'sort_by(Images, &CreationDate)[-1].ImageId'", shell=True, universal_newlines=True)
        ami_id = ami_id.strip().strip('"')
        ami_name = subprocess.check_output("aws ec2 describe-images --filters 'Name=name,Values=ENCRYPTED-AL2-EMR-*' 'Name=architecture,Values=x86_64' 'Name=virtualization-type,Values=hvm' 'Name=root-device-type,Values=ebs' --query 'sort_by(Images, &CreationDate)[-1].Name'", shell=True, universal_newlines=True)
        ami_name = ami_name.strip().strip('"')
        if "null" in ami_id:
            print("GTS AMI is not Available for EMR so defaulting to AMAZON's AMI")
            ami_id = ""
        else:
            print("GTS AMI ID for EMR is: " + ami_id)
            print("GTS AMI Name for EMR is: " + ami_name)
        if self.args['step'] in ('data-migration'):
            cluster_id = aws_utils.run_job_flow(
                self.emr_config, self.emr_client, True, ami_id)
        else:
            cluster_id = aws_utils.run_job_flow_fleet(
                self.emr_config, self.emr_client, True, ami_id)
        self.classs_emr_cluster.append(cluster_id)
        bucket_name = f"ta-individual-findw-{self.args['env']}-codedeployment"
        prefix = f"miscellaneous/bootstrap-action/emr_steps/"
        script_args = ''
        s3_bucket = self.s3_resource.Bucket(bucket_name)
        step_id_list = []
        for sh_file_obj in s3_bucket.objects.filter(Prefix=prefix):
            if '.sh' in sh_file_obj.key:
                sh_file = sh_file_obj.key
                script_uri = f"s3://{bucket_name}/{sh_file}"
                name = sh_file.split('/')[-1].upper()
                action_on_failure = 'CONTINUE'
                step_id = aws_utils.add_step_script_runner(
                    cluster_id,
                    name,
                    script_uri,
                    script_args,
                    self.emr_client,
                    action_on_failure
                )
                step_id_list.append(step_id)
        self.sched_check_emr()
        self.sched.start()
        self.wait_for_steps_completion(cluster_id, step_id_list)
        self.sched.shutdown(wait=False)
        if self.args['step'] == "BDE":
            print("BDE: Updating IP in EMAP Server")
            timeout = time.time() + (60*60)
            while True:
                try:
                    print("***********************************************************")
                    print("***********************************************************")
                    instance_fleets = self.emr_client.list_instance_fleets(ClusterId=cluster_id)['InstanceFleets']
                    final_check = False
                    for ins in instance_fleets:
                        if ins['InstanceFleetType'] == 'CORE':
                            if ins['Status']['State'] in ('TERMINATING', 'TERMINATED'):
                                print(f"Instance Fleet CORE node Status is {ins['Status']['State']}: Terminating the emr")
                                sys.exit(70)
                            elif ins['Status']['State'] == 'PROVISIONING':
                                pass
                            else:
                                print("Instance Type of CORE type is either bootstrapped/running")
                                final_check = True
                                break
                    if final_check:
                        break
                    instance_fleet_status = []
                    instance_core_fleets = self.emr_client.list_instances(ClusterId=cluster_id, InstanceFleetType='CORE')['Instances']
                    if time.time() > timeout:
                        print("TERMINATING: BDE CORE node execution took more than an hour.")
                        break
                    else:
                        print(f"Execution time left to auto-terminate: {int(timeout-time.time())}")
                    if len(instance_core_fleets) == 0:
                        print(f"WAITING: No CORE instance found for {cluster_id}")
                        time.sleep(60)
                        continue
                    target_ondemand_capacity = 0
                    total_instance_weight = 0
                    target_spot_capacity = 0
                    for fleet in self.emr_config['Instance_Fleets']:
                        if fleet['InstanceFleetType'] == 'CORE':
                            target_ondemand_capacity = fleet['TargetOnDemandCapacity']
                            target_spot_capacity = fleet['TargetSpotCapacity']
                    target_capacity = target_ondemand_capacity + target_spot_capacity
                    for instance in instance_core_fleets:
                        weight = self.get_instance_config_weight(emr_config_core, instance['InstanceType'])
                        total_instance_weight += weight
                    print(f"Target on demand capacity: {target_capacity}")
                    print(f"Instance weight: {total_instance_weight}")
                    if target_capacity != total_instance_weight:
                        print(f"Expected target instance length: {target_capacity}, actual instance length: {total_instance_weight}")
                        print("WAITING: More instances to be up")
                        time.sleep(60)
                        continue
                    else:
                        print(f"Both target and actual instance match")
                        for instance in instance_core_fleets:
                            status = instance['Status']['State'].strip()
                            print(f"{instance['Id']} : {instance['Status']['State']}")
                            if status == 'TERMINATED':
                                aws_utils.terminate_cluster(cluster_id, self.emr_client)
                                print(f"Instance: {instance['Id']} terminated due to {instance['StateChangeReason']}")
                                sys.exit(100)
                            else:
                                instance_fleet_status.append(status)
                    exit_cond = True
                    for ins_status in instance_fleet_status:
                        if ins_status == 'PROVISIONING':
                            print("One of the instance is still in PROVISIONING status")
                            exit_cond = False
                    if exit_cond:
                        print("All the instances are bootstrapped/running")
                        break
                    time.sleep(60)
                except ClientError as error:
                    print("Exception occurred")
                    if error.response['Error']['Code'] == 'ThrottlingException':
                        print("Exception: ThrottlingException")
                        continue
            #self.update_ip_address(cluster_id)
        print("Cluster Id: " + cluster_id)
        def emr_handler(self):
        if self.args['step'] is None:
            self.args['step'] = 'BDE'
        elif self.args['step'] == 'gdq':
            self.args['step'] = 'BDE'
        if self.args['source_system'] is None or self.args['step'] == 'BDE':
            self.args['source_system'] = 'all'
        if self.args['source_system'] == 'all' and self.args['action'] == 'create':
            time.sleep(random.randrange(1, 60, 10))
        elif self.args['step'] in ['BDE', 'curated'] and self.args['action'] == 'create':
            time.sleep(random.randrange(12, 120, 10))
        if self.args['action'] == 'create':
            time.sleep(random.randrange(1, 60, 10))
        self.emr_name = f"ta-individual-findw-{self.args['env']}-ondemand-emr-{self.args['source_system']}-{self.args['step']}-{self.args['project']}"
        print(self.emr_name)
        emr_status, emr_ids = self.check_emr()
        if self.args['action'] == 'create':
            if emr_status:
                print(f"EMR already exists for given configuration: {emr_ids[0]}")
            else:
                self.create_emr()
        elif self.args['action'] == 'terminate':
            if emr_status:
                for emr_id in emr_ids:
                    aws_utils.terminate_cluster(emr_id, self.emr_client)
                pass
            else:
                print("No emr exists for the given configuration")
    def emr_jenkins(self):
        self.args['source_system'] = 'all'
        if self.args['step'] is None:
            self.args['step'] = 'all'
        self.emr_name = f"ta-individual-findw-{self.args['env']}-ondemand-emr-{self.args['step']}-{self.args['project']}-jenkins"
        self.create_jenkins_emr()


