#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
if [ ! -d "$DIR" ]; then DIR=$(pwd); fi
# Check if the correct number of arguments is provided
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <environment> <cycle_date>"
    exit 1
fi
# Assign arguments to variables
ENVIRONMENT=$1
CYCLE_DATE=$2
ROOT_DIR=$DIR
DIR=${DIR}/.

####Getting LOGDIR and all other variables from script.properties file
source "$DIR/script.properties"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
mkdir -p "$logFilePath/nydfs/itcg"
LOGFILE="$logFilePath/nydfs/itcg/nydfs_itcg_refresh_${TIMESTAMP}.log"
ERRORFILE="$logFilePath/nydfs/itcg/nydfs_itcg_refresh_${TIMESTAMP}.err"
# Run the Python script with the provided arguments
spark-submit --master yarn $spark_properties "${ROOT_DIR}/nydfs_itcg_refresh.py" $ENVIRONMENT $CYCLE_DATE 1>> $LOGFILE 2>> $ERRORFILE
RC=${PIPESTATUS[0]}
if [ $RC -ne 0 ]; then
    echo "ERROR 20: NYDFS ITCG one time cleanup script failed with RC: $RC at: $(date +%Y-%m-%d' '%H:%M:%S)" | tee -a $LOGFILE
    exit $RC
fi
echo "NYDFS ITCG one time cleanup script completed at: $(date +%Y-%m-%d' '%H:%M:%S)" | tee -a $LOGFILE
