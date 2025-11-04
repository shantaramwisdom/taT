#!/bin/bash
dataPurge() {
 s3_path=$1
 #cmd=$(echo "$s3_path" | sed 's/*//g')
 echo "******************************************************************************************"
 echo "Command: aws s3 rm --recursive --only-show-errors $s3_path execution started at $(date +%Y-%m-%d' '%H:%M:%S)"
 #$cmd
 aws s3 rm --recursive --only-show-errors $s3_path
 RC=$?
 if [ $RC != "0" ]; then
  #echo "ERROR 100: Command: ${cmd} execution Failed at $(date +%Y-%m-%d' '%H:%M:%S)"
  echo "ERROR 100: Command: aws s3 rm --recursive --only-show-errors $s3_path execution Failed at $(date +%Y-%m-%d' '%H:%M:%S)"
  exit 100
 fi
 #echo "Command: ${cmd} execution completed at $(date +%Y-%m-%d' '%H:%M:%S)"
 echo "Command: aws s3 rm --recursive --only-show-errors $s3_path execution completed at $(date +%Y-%m-%d' '%H:%M:%S)"
 echo "******************************************************************************************"
}

while getopts "c:" opt
do
 case "$opt" in
  c) s3_path="$OPTARG" ;;
  \?) echo "Invalid option"
      exit 99
  ;;
 esac
done

if [ -z "$s3_path" ]; then
 echo "ERROR 10: Mandatory argument that is S3 Paths cannot be empty"
 exit 10
fi

echo "******************************************************************************************"
echo "Data Purge Process Started at $(date +%Y-%m-%d' '%H:%M:%S)"
chunk_size=50
export -f dataPurge
/usr/local/bin/parallel --will-cite -L1 -j $chunk_size dataPurge ::: $s3_path
RC=$?
if [ $RC != "0" ]; then
 echo "ERROR 100: Data Purge Process Failed at $(date +%Y-%m-%d' '%H:%M:%S)"
 exit 100
fi
echo "Data Purge Process Completed at $(date +%Y-%m-%d' '%H:%M:%S)"
