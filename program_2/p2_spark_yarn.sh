#!/bin/bash

#Sanjay Roberts

# Run p2.py using Spark on Yarn (on the cluster)

#INPUT_FILE_FUND="hdfs:///fundamentals-short.csv"
#INPUT_FILE_PRICE="hdfs:///prices-short.csv"

INPUT_FILE_FUND="hdfs:///Prog2Fundamentals/"
INPUT_FILE_PRICE="hdfs:///Prog2Prices/"

OUTPUT_DIR="/u/home/stocks"

# Create HDFS directory path same as current directory
hadoop fs -mkdir -p "hdfs://$PWD"

# Delete any old copies output directory on linux filesystem and HDFS
rm -rf "$OUTPUT_DIR"
hadoop fs -rm -f -r "hdfs://$OUTPUT_DIR"

# Run the job passing command line parameters for the input/output directories
# if HDFS is configured on the system, that is the default for spark
# Attach with prefix "hdfs://" when passing to python/spark anyway
spark-submit \
    --master "yarn" \
    --deploy-mode "cluster" \
    p2.py $INPUT_FILE_FUND $INPUT_FILE_PRICE "hdfs://$OUTPUT_DIR" 


# Copy result from HDFS to Linux FS
spark_exit=$?
if [[ $spark_exit -eq 0 ]] ; then
    echo "Copying output from hdfs://$OUTPUT_DIR"
    hadoop fs -get "hdfs://$OUTPUT_DIR" "."
    exit $?
else
    echo "Spark job failed with status $spark_exit"
    exit $spark_exit
fi

