#!/bin/bash
###################################################################################################
# DESCRIPTION - This program executes the Commands (AWS) that are passed to the job. The command is governed by the access of the EC2 machine
#                Input Variable - Pipe Delimited Table names (glue_database_name.table_name), that are continous without spaces
#
# Usage: sh command_runner.sh "echo hello;echo hola"
#
# 01/27/2022 - PRATHYUSH PREMACHANDRAN - AWS MIGRATION RTS 1333, Initial Release
###################################################################################################
export AWS_SHARED_CREDENTIALS_FILE=/application/financedw/scripts/.aws/credentials
command=$1
eval $command
RC=$?
if [ $RC != "0" ]; then
echo "ERROR_100: Failed Running the Command $command, with Return Code $RC"
exit 100
fi
