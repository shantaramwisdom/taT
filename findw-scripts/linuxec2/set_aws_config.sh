#!/bin/bash
##################################################################################
# DESCRIPTION - Script to Change AWS configuration For support account
#
# 05/13/2021 - PRATHYUSH PREMA CHANDRAN - AWS MIGRATION RTS 1333, Initial Release
# 01/28/2022 - PRATHYUSH PREMA CHANDRAN - AWS MIGRATION RTS 1333, Set AWS Region for the Machine
##################################################################################
homedir="/application/financedw/scripts"
TIMESTAMP=$(date +%Y-%m-%d_%H:%M:%S:%N)
TEMP_FILE=/application/financedw/temp/temp_config_$TIMESTAMP.txt
region=$(curl -s http://169.254.169.254/latest/meta-data/placement/region)
if [[ -z $region ]]; then
 region=$(TOKEN=`curl -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600"` \
 && curl -H "X-aws-ec2-metadata-token: $TOKEN" -v http://169.254.169.254/latest/meta-data/placement/region)
fi
val="[default]\nmax_attempts = 15\nretry_mode = adaptive\nregion = ${region}\ns3 = \n max_concurrent_requests=1000\n max_queue_size=1000"
echo -e $val > $TEMP_FILE
echo "AWS Configuration Setting Script started at $(date +%Y-%m-%d_%H:%M:%S:%N)"
mkdir -p $homedir/.aws
if cmp -s "$homedir/.aws/credentials" "$TEMP_FILE" ; then
 echo "AWS Configuration File $homedir/.aws/credentials doesn't need an update as unchanged"
else
 echo -e $val > $homedir/.aws/credentials
 echo "AWS Configuration File $homedir/.aws/credentials Updated"
fi
chmod o+r $homedir/.aws/credentials
chmod g+w $homedir/.aws/credentials
rm $TEMP_FILE
echo "AWS Configuration Setting Script completed at $(date +%Y-%m-%d_%H:%M:%S:%N)"
