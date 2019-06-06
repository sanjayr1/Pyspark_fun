#!/bin/bash

#Sanjay Roberts
# Run p2.py using Spark locally (on a single machine)

#INPUT_FILE_FUND="/fundamentals-short.csv"
#INPUT_FILE_PRICE="/prices-short.csv"

INPUT_FILE_FUND="/Prog2Fundamentals/"
INPUT_FILE_PRICE="/Prog2Prices/"


OUTPUT_DIR="/u/home/stocks"

# Delete any old copies output directory on linux filesystem
rm -rf "$OUTPUT_DIR"


# Run the job passing command line parameters for the input/output directories
# if HDFS is configured on the system, that is the default for spark
# Override with prefix "file://" when passing to python/spark
spark-submit \
    --master "local[*]" \
    --deploy-mode "client" \
    p2.py "file://$INPUT_FILE_FUND" "file://$INPUT_FILE_PRICE" "file://$OUTPUT_DIR"

exit $?



