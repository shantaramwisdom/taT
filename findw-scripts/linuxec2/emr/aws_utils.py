import logging
import time
import sys
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from botocore.exceptions import ClientError
region = 'us-east-1'
logger = logging.getLogger(__name__)
def send_email_amazon_ami(emr_config, cluster_id, ami_id):
    if len(ami_id) != 0:
        return
    for i in range(1, len(emr_config['Tags'])):
        if emr_config['Tags'][i]['Key'] == "Environment":
            env = emr_config['Tags'][i]['Value']
    env_dict = {"tst":"Test", "dev":"Dev", "mdl":"Model", "prd":"Prod"}
    for key in env_dict:
        env = env.replace(key, env_dict[key])
    fromEmail='tatechdataengineering-dwdev@transamerica.com'
    recipients='tatechdataengineering-dwdev@transamerica.com'
    msg = MIMEMultipart()
    #if env == 'Prod':
    #   fromEmail='tatechdataengineering-dwops@transamerica.com'
    #   recipients='tatechdataengineering-dwdev@transamerica.com'
    #   msg['CC'] = fromEmail
    msg['From'] = fromEmail
    msg['To'] = recipients
    msg['Subject'] = "{0} Env Warning: GTS AMI Not Available for EMR".format(env)
    html = """\
    <html>
    <head></head>
    <body>
    Hi! <br>
    GTS AMI Not Available for Below EMR. Marketplace Amazon AMI used to create EMR. <br>
    Environment : {0} <br>
    Cluster Name: {1} <br>
    Cluster ID: {2} <br>
    Regards, <br>
    Finance Datawarehouse Team <br>
    </body>
    </html>""".format(env, emr_config['name'], cluster_id)
    partHTML = MIMEText(html, 'html')
    msg.attach(partHTML)
    smtpObj = smtplib.SMTP('mail.us.aegon.com', 25)
    smtpObj.ehlo()
    if float(f"{sys.version_info[0]}.{sys.version_info[1]}") >= 3.9:
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS)
        ssl_context.options |= ssl.OP_NO_SSLv2
        ssl_context.options |= ssl.OP_NO_SSLv3
        ssl_context.set_ciphers('DEFAULT:!DH:!kRSA')
        smtpObj.starttls(context=ssl_context)
    else:
        smtpObj.starttls()
    try:
        smtpObj.sendmail(msg['From'], msg['To'].split(","), msg.as_string())
        print("Email Notification for missing GTS AMI for EMR was sent!!")
    except Exception as ex:
        print("Exception Caught While Sending email. Exception Details: ", ex)
def emr_return_instances_ip(cluster_id):
    pass
def add_step_script_runner(cluster_id, name, script_uri, script_args, emr_client, action_on_failure):
    """
    Adds a job step to the specified cluster using script_runner.jar
    :param cluster_id: The ID of the cluster.
    :param name: The name of the step.
    :param script_uri: The URI where the Python script is stored.
    :param script_args: Arguments to pass to the Python script.
    :param emr_client: The Boto3 EMR client object.
    :return: The ID of the newly added step.
    'ActionOnFailure':'TERMINATE_JOB_FLOW'|'TERMINATE_CLUSTER'|'CANCEL_AND_WAIT'|'CONTINUE'
    """
    try:
        response = emr_client.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    'Name': name,
                    'ActionOnFailure': action_on_failure,
                    'HadoopJarStep': {
                        'Jar': 's3://' + region + '.elasticmapreduce/libs/script-runner/script-runner.jar',
                        'Args': [script_uri] + script_args.split(' ')
                    }
                }
            ]
        )
        step_id = response['StepIds'][0]
        print("Started step with ID", step_id)
        logger.info("Started step with ID %s", step_id)
    except ClientError:
        print("Couldn't start step with URI. Cluster is:", name, "script is:", script_uri)
        logger.exception("Couldn't start step Xs with URI Xs.", name, script_uri)
        raise
    else:
        return step_id
def run_job_flow_fleet(emr_config, emr_client, job_flow_alive, ami_id):
    for i in range(1, len(emr_config['Tags'])):
        if emr_config['Tags'][i]['Key'] == "Environment":
            env = emr_config['Tags'][i]['Value']
    if 'emr-7' not in emr_config['release_label']:
        emr_config['Bootstrap_Actions'] = emr_config['Bootstrap_Actions'] + [{'Name': 'GTS BootStrap Action', 'ScriptBootstrapAction': {'Path': 's3://gts-emr-bootstrap/bootstrap.sh'}}]
        if emr_config['release_label'] == 'emr-7.2.0':
            emr_config['applications'] = emr_config['applications'] + ["Sqoop"]
    try:
        response = emr_client.run_job_flow(
            Name=emr_config['name'],
            LogUri=emr_config['log_uri'],
            ReleaseLabel=emr_config['release_label'],
            #CustomAmiId=ami_id,
            Instances={
                'InstanceFleets': emr_config['Instance_Fleets'],
                'KeepJobFlowAliveWhenNoSteps': job_flow_alive,
                'EmrManagedMasterSecurityGroup': emr_config['Emr_Managed_Master_Security_Group'],
                'EmrManagedSlaveSecurityGroup': emr_config['Emr_Managed_Slave_Security_Group'],
                'Ec2KeyName': emr_config['Ec2_Key_Name'],
                'AdditionalMasterSecurityGroups': emr_config['Additional_Master_Security_Groups'],
                'AdditionalSlaveSecurityGroups': emr_config['Additional_Slave_Security_Groups'],
                'ServiceAccessSecurityGroup': emr_config['Service_Access_Security_Group'],
                'Ec2SubnetIds': emr_config['Subnet_Ids']
            },
            BootstrapActions=emr_config['Bootstrap_Actions'],
            Applications=[{'Name': app} for app in emr_config['applications']],
            Configurations=emr_config['Configurations'],
            JobFlowRole=emr_config['InstanceProfile'],
            ServiceRole=emr_config['Service_Role'],
            SecurityConfiguration=emr_config['Security_Configuration'],
            ScaleDownBehavior='TERMINATE_AT_TASK_COMPLETION',
            EbsRootVolumeSize=emr_config['Ebs_Root_Volume_Size'],
            VisibleToAllUsers=True,
            Tags=emr_config['Tags'],
            ManagedScalingPolicy=emr_config['Managed_Scaling_Policy'],
            StepConcurrencyLevel=75,
            AutoTerminationPolicy=emr_config['Auto_Termination_Policy']
        )
        cluster_id = response['JobFlowId']
        #send_email_amazon_ami(emr_config, cluster_id, ami_id)
        print("Created cluster.", cluster_id)
        logger.info("Created cluster %s.", cluster_id)
    except ClientError:
        print("Couldn't create cluster.")
        logger.exception("Couldn't create cluster.")
        raise
    else:
        return cluster_id
def run_job_flow(emr_config, emr_client, job_flow_alive, ami_id):
    try:
        response = emr_client.run_job_flow(
            Name=emr_config['name'],
            LogUri=emr_config['log_uri'],
            ReleaseLabel=emr_config['release_label'],
            #CustomAmiId=ami_id,
            Instances={
                'InstanceGroups': emr_config['Instance_Groups'],
                'KeepJobFlowAliveWhenNoSteps': job_flow_alive,
                'EmrManagedMasterSecurityGroup': emr_config['Emr_Managed_Master_Security_Group'],
                'EmrManagedSlaveSecurityGroup': emr_config['Emr_Managed_Slave_Security_Group'],
                'Ec2KeyName': emr_config['Ec2_Key_Name'],
                'AdditionalMasterSecurityGroups': emr_config['Additional_Master_Security_Groups'],
                'AdditionalSlaveSecurityGroups': emr_config['Additional_Slave_Security_Groups'],
                'ServiceAccessSecurityGroup': emr_config['Service_Access_Security_Group'],
                                'Ec2SubnetId': emr_config['Subnet_Id']
            },
            BootstrapActions=emr_config['Bootstrap_Actions'],
            Applications=[
                {'Name': app}
                for app in emr_config['applications']
            ],
            Configurations=emr_config['Configurations'],
            JobFlowRole=emr_config['InstanceProfile'],
            ServiceRole=emr_config['Service_Role'],
            SecurityConfiguration=emr_config['Security_Configuration'],
            AutoScalingRole=emr_config['Auto_Scaling_Role'],
            ScaleDownBehavior='TERMINATE_AT_TASK_COMPLETION',
            EbsRootVolumeSize=emr_config['Ebs_Root_Volume_Size'],
            VisibleToAllUsers=True,
            Tags=emr_config['Tags'],
            StepConcurrencyLevel=75,
            AutoTerminationPolicy=emr_config['Auto_Termination_Policy']
        )
        cluster_id = response['JobFlowId']
        print("Created cluster.", cluster_id)
        #send_email_amazon_ami(emr_config, cluster_id, ami_id)
        logger.info("created Cluster %s.", cluster_id)
        return cluster_id
    except KeyError as e:
        print("The exeption is " + str(e) + " Issuing new")
        response = emr_client.run_job_flow(
            Name=emr_config['name'],
            LogUri=emr_config['log_uri'],
            ReleaseLabel=emr_config['release_label'],
            #CustomAmiId=ami_id,
            Instances={
                'InstanceGroups': emr_config['Instance_Groups'],
                'KeepJobFlowAliveWhenNoSteps': job_flow_alive,
                'EmrManagedMasterSecurityGroup': emr_config['Emr_Managed_Master_Security_Group'],
                'EmrManagedSlaveSecurityGroup': emr_config['Emr_Managed_Slave_Security_Group'],
                'Ec2KeyName': emr_config['Ec2_Key_Name'],
                'AdditionalMasterSecurityGroups': emr_config['Additional_Master_Security_Groups'],
                'AdditionalSlaveSecurityGroups': emr_config['Additional_Slave_Security_Groups'],
                'ServiceAccessSecurityGroup': emr_config['Service_Access_Security_Group'],
                'Ec2SubnetId': emr_config['Subnet_Id']
            },
            BootstrapActions=emr_config['Bootstrap_Actions'],
            Applications=[
                {'Name': app}
                for app in emr_config['applications']
            ],
            Configurations=emr_config['Configurations'],
            JobFlowRole=emr_config['InstanceProfile'],
            ServiceRole=emr_config['Service_Role'],
            SecurityConfiguration=emr_config['Security_Configuration'],
            AutoScalingRole=emr_config['Auto_Scaling_Role'],
            ScaleDownBehavior='TERMINATE_AT_TASK_COMPLETION',
            EbsRootVolumeSize=emr_config['Ebs_Root_Volume_Size'],
            VisibleToAllUsers=True,
            Tags=emr_config['Tags'],
            StepConcurrencyLevel=75,
            AutoTerminationPolicy=emr_config['Auto_Termination_Policy']
        )
        cluster_id = response['JobFlowId']
        print("Created cluster.", cluster_id)
        #send_email_amazon_ami(emr_config, cluster_id, ami_id)
        logger.info("created Cluster %s.", cluster_id)
        return cluster_id
    except ClientError:
        print("Couldn't create cluster.")
        logger.exception("Couldn't create cluster.")
        raise
    else:
        return cluster_id
def describe_cluster(cluster_id, emr_client):
    """
    Gets detailed information about a cluster.
    :param cluster_id: The ID of the cluster to describe.
    :param emr_client: The Boto3 EMR client object.
    :return: The retrieved cluster information.
    """
    try:
        response = emr_client.describe_cluster(ClusterId=cluster_id)
        cluster = response['Cluster']
        MasterPublicDnsName = cluster.get('MasterPublicDnsName', 'Master Node IP is Unavailable at this time')
        status = cluster['Status']
        StateChangeReason = status['StateChangeReason']
        StateChangeReasonMsg = StateChangeReason.get('Message', 'Cluster Status Change Reason is Unavailable at this time')
        print("Cluster Name:", cluster['Name'])
        print("Cluster Status:", status['State'])
        print("Cluster IP:", MasterPublicDnsName)
        print("Cluster Status Change Reason:", StateChangeReasonMsg)
        logger.info("Got data for cluster %s.", cluster['Name'])
    except ClientError:
        print("Couldn't get data for cluster.", cluster_id)
        logger.exception("Couldn't get data for cluster %s.", cluster_id)
        raise
    else:
        return cluster, status['State']
def describe_step(cluster_id, step_id, emr_client):
    """
    Gets detailed information about a step.
    :param cluster_id: The ID of the cluster.
    :param step_id: The ID of the step.
    :param emr_client: The Boto3 EMR client object.
    :return: The retrieved step information.
    """
    try:
        response = emr_client.describe_step(
            ClusterId=cluster_id,
            StepId=step_id
        )
        Step = response['Step']
        Status = Step['Status']
        StateChangeReason = Status['StateChangeReason']
        StateChangeReasonMsg = StateChangeReason.get('Message', 'Step Status Change Reason is Unavailable at this time')
        print("Step Id:", Step['Id'])
        print("Step Name:", Step['Name'])
        print("Step Status:", Status['State'])
        print("Step Status Change Reason:", StateChangeReasonMsg)
        print("Got data for Step:", Step['Name'], "in Cluster", cluster_id)
        #logger.info("Got data for Step %s", Step['Name'])
    except ClientError:
        print("Couldn't get data for step", step_id, "in Cluster", cluster_id)
        logger.exception("Couldn't get data for step %s", step_id, "in Cluster", cluster_id)
        raise
    else:
        return Step, State
def cancel_steps(cluster_id, step_ids, emr_client):
    """
    Cancels steps in a running cluster.
    :param cluster_id: The ID of the cluster.
    :param step_ids: The IDs of the steps to cancel.
    :param emr_client: The Boto3 EMR client object.
    :return: Response from EMR cancel_steps API.
    """
    step_ids = LIST [StepId1, StepId2]
    StepCancellationOption='SEND_INTERRUPT'|'TERMINATE_PROCESS'
    Default='SEND_INTERRUPT'
    try:
        response = emr_client.cancel_steps(
            ClusterId=cluster_id,
            StepIds=step_ids,
            StepCancellationOption='SEND_INTERRUPT'
        )
        print("Cancelled Steps", step_ids, "and response is", response)
        #logger.info("Cancelled Steps", step_ids, "and response is", response)
    except ClientError:
        print("Couldn't cancel Steps", step_ids)
        #logger.exception("Couldn't cancel Steps", step_ids)
        raise
    else:
        return response
def add_step_command_runner(cluster_id, name, script_uri, script_args, emr_client, action_on_failure, script_type):
    """
    Adds a job step to the specified cluster. This example adds a Spark
    step, which is run by the cluster as soon as it is added.
    :param cluster_id: The ID of the cluster.
    :param name: The name of the step.
    :param script_uri: The URI where the Python script is stored.
    :param script_args: Arguments to pass to the Python script.
    :param emr_client: The Boto3 EMR client object.
    :return: The ID of the newly added step.
    'ActionOnFailure':'TERMINATE_JOB_FLOW'|'TERMINATE_CLUSTER'|'CANCEL_AND_WAIT'|'CONTINUE'
    script_type = bash or sh or python or python3 or ...
    """
    try:
        response = emr_client.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[{
                'Name': name,
                'ActionOnFailure': action_on_failure,
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [script_type, script_uri] + script_args.split(' ')
                }
            }]
        )
        step_id = response['StepIds'][0]
        print("Started step with ID", step_id)
        logger.info("Started step with ID %s", step_id)
    except ClientError:
        print("Couldn't start step with URI. Step is", name, "script is", script_uri, "EMR Cluster", cluster_id, "may be missing")
        raise
    else:
        return step_id
def add_step_spark_submit(cluster_id, name, script_uri, script_args, emr_client, action_on_failure, spark_args, py_files=None):
    """
    Adds a job step to the specified cluster. This example adds a Spark
    step, which is run by the cluster as soon as it is added.
    :param cluster_id: The ID of the cluster.
    :param name: The name of the step.
    :param script_uri: The URI where the Python script is stored.
    :param script_args: Arguments to pass to the Python script.
    :param emr_client: The Boto3 EMR client object.
    :return: The ID of the newly added step.
    'ActionOnFailure':'TERMINATE_JOB_FLOW'|'TERMINATE_CLUSTER'|'CANCEL_AND_WAIT'|'CONTINUE'
    """
    try:
        if not py_files:
            py_files = ''
        response = emr_client.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[{
                'Name': name,
                'ActionOnFailure': action_on_failure,
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['spark-submit'] + spark_args.split(' ') + py_files.split(' ') + [script_uri] + script_args.split(' ')
                }
            }]
        )
        step_id = response['StepIds'][0]
        print("Started step with ID", step_id)
        logger.info("Started step with ID %s", step_id)
    except ClientError:
        print("Couldn't start step with URI. Step is", name, "script is", script_uri, "EMR Cluster ", cluster_id, "may be missing")
        raise
    else:
        return step_id
def terminate_cluster(cluster_id, emr_client):
    """
    Terminates a Cluster. This terminates all instances in the cluster and cannot
    be undone. Any data not saved elsewhere, such as in an Amazon S3 bucket, is lost.
    :param cluster_id: The ID of the cluster to terminate.
    :param emr_client: The Boto3 EMR client object.
    """
    try:
        emr_client.terminate_job_flows(JobFlowIds=[cluster_id])
        print("Terminated cluster.", cluster_id)
        logger.info("Terminated cluster %s.", cluster_id)
    except ClientError:
        print("Couldn't terminate cluster.", cluster_id)
        logger.exception("Couldn't terminate cluster %s.", cluster_id)
        raise
    return cluster_id
def wait_ssm_command(command_id, instance_id, ssm_client, s3_log_bucket):
    status = ''
    if not command_id:
        print("Not a valid SSM id")
        return False
    time.sleep(30)
    while(status == 'Pending' or status == 'InProgress' or status == 'Delayed' or not status):
        response = ssm_client.get_command_invocation(
            CommandId=command_id,
            InstanceId=instance_id
        )
        status = response['Status']
        print(f"Status for the ssm is: {status}")
        if (status == 'Cancelled' or status == 'TimedOut' or status == 'Failed' or status == 'Cancelling'):
            get_ssm_details(command_id, instance_id, ssm_client, s3_log_bucket, False)
            print("Command Failed")
            return False
        elif (status == 'Success'):
            print("Command Completed Successfully. Breaking the loop")
            get_ssm_details(command_id, instance_id, ssm_client, s3_log_bucket, True)
            break
        time.sleep(60)
    return True
def get_ssm_details(command_id, instance_id, ssm_client, s3_log_bucket, status):
    response = ssm_client.get_command_invocation(
        CommandId=command_id,
        InstanceId=instance_id
    )
    response_output_path = ssm_client.list_command_invocations(
        CommandId=command_id,
        InstanceId=instance_id,
        Details=True
    )
    output_path = response_output_path['CommandInvocations'][0]['CommandPlugins'][0]['OutputS3KeyPrefix']
    print(f"The Command's standard output/error is uploaded to: s3://{s3_log_bucket}/{output_path}/0.awsrunShellScript/")
    stdout = os.popen(f"aws s3 cp s3://{s3_log_bucket}/{output_path}/0.awsrunShellScript/stdout -").readlines()
    stderr = os.popen(f"aws s3 cp s3://{s3_log_bucket}/{output_path}/0.awsrunShellScript/stderr -").readlines()
    print('#############################SSM-LOGS-STDOUT#############################')
    for line in stdout:
        print(line)
    if not status:
        print('#############################SSM-LOGS-STDERR#############################')
        for line in stderr:
            print(line)
    print('#########################################################################')

