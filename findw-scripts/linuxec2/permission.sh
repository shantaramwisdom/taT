#!/bin/bash
##################################################################################
# SCRIPT TO CHANGE THE PERMISSION AND GROUP FOR THE FILES UNDER SCRIPTS DIRECTORY
# CHANGE THE EOL SEQUENCE FOR WINDOWS TO UNIX
#
# THIS CAN BE USED ON BOTH OLD FRAMEWORK AND NEW FRAMEWORK
##################################################################################
# 9/13/2021 - GEETHAN PALANISAMI - RTS1333 AWS Cloud Migration
##################################################################################
env=$1
if [[ -z $env ]]; then
  echo "ERROR 1: One of the mandatory argument (environment name) is not provided, please check"
  exit 1
elif [[ $env == "dev" ]] || [[ $env == "tst" ]] || [[ $env == "mdl" ]] || [[ $env == "prd" ]]; then
  usergrp=ueantwstaindividualfindwsracct${env}
else
  echo "ERROR 2: Invalid environment name: $env name, please check"
  exit 2
fi
sudo sh /application/financedw/scripts/set_aws_config.sh
sudo chown -R ec2-user:$usergrp /application/financedw/scripts/
sudo chmod g+w /application/financedw/.aws/credentials
sudo find /application/financedw/scripts/ -type f -iname "*.sh" -exec dos2unix {} \;
sudo find /application/financedw/scripts/ -type f -iname "*.py" -exec dos2unix {} \;
find /application/financedw/scripts/ -type f -iname "*.sh" -exec chmod gu+x {} \;
find /application/financedw/scripts/ -type f -iname "*.py" -exec chmod gu+x {} \;
sudo chmod 775 /application/financedw/logs/
sudo chmod 775 /application/financedw/temp/
