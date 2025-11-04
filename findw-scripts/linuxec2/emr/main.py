import argparse
import os
import re
import sys
from emr import EMR
def parse_arguments(args):
    args.add_argument(
        '-e', '--env',
        required=True,
        help='Environment name for the deployment',
        choices=['dev', 'tst', 'mdl', 'prd']
    )
    args.add_argument(
        '-a', '--action',
        required=False,
        help='EMR action',
        choices=['create', 'terminate']
    )
    args.add_argument(
        '-p', '--project',
        required=True,
        help='Project Name',
        default='financedw'
    )
    args.add_argument(
        '-s', '--step',
        required=False,
        help='Hop/Layer for running the job',
        choices=['curated', 'annuities', 'financedwdq', 'masterdatastore', 'datastage', 'controls', 'maintenance']
    )
    args.add_argument(
        '-ss', '--source_system',
        required=False,
        help='Source system for the job'
    )
    args.add_argument(
        '-j', '--jenkins',
        required=False,
        action='store_true',
        help='Jenkins Job'
    )
    args.add_argument(
        '-jj', '--jenkins_job',
        required=False,
        help='Step name needed for the jenkins job',
        choices=['dd1', 'create_db', 'deploy_config', 'dropdb']
    )
    args.add_argument(
        '-jp', '--jenkins_params',
        required=False,
        help='Jenkins parameter for the database name'
    )
    args.add_argument(
        '-d', '--cycle_date',
        required=False,
        help='The Cycle Date - format YYYYMMDD'
    )
    sys_args = args.parse_args()
    return vars(sys_args)
def emr_runner(sys_args):
    emr = EMR(sys_args)
    if sys_args['jenkins']:
        emr.emr_jenkins()
    else:
        emr.emr_handler()
        # print(emr_config)
if __name__ == '__main__':
    args = argparse.ArgumentParser()
    sys_args = parse_arguments(args)
    print(sys_args)
    if sys_args['source_system'] is not None and sys_args['step'] != 'curated':
        print(f"Source System Can be passed only if Step is 'curated'. Source System is ({sys_args['source_system']}) and Step is ({sys_args['step']}).")
        sys.exit(1)
    if not re.match(rf"^financedw({re.escape(sys_args['env'])})([1-9][0-9]{0,1})?$", sys_args['project']) and sys_args['project'] != 'financedwuat':
        print("Please enter a valid project: financedw | financedw<env><2 digits> | financedwuat")
        sys.exit(1)
    if sys_args['jenkins_job'] is not None and sys_args['step'] not in ['curated', 'annuities', 'maintenance']:
        print(f"Invalid Step for EMR Creation. Allowed values should be in ['curated', 'annuities', 'maintenance']")
        sys.exit(1)
    elif sys_args['jenkins_job'] is not None and sys_args['step'] not in ['curated', 'annuities', 'financedwdq', 'masterdatastore', 'datastage', 'controls']:
        print(f"Invalid Step for Jenkins {sys_args['jenkins_job']}. Allowed values should be in ['curated', 'annuities', 'financedwdq', 'masterdatastore', 'datastage', 'controls']")
        sys.exit(1)
    #os.system('sh ../application/financedw/scripts/set_aws_config.sh')
    emr_runner(sys_args)