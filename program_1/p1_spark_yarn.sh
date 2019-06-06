#!/bin/bash

#Sanjay Roberts

# Run p1.py using Spark on Yarn (on the cluster)

# Set cellSize, and names of files for input and output.
# Paths must be absolute paths.
# $PWD is bash shell variable for the current directory
CELL_SIZE=1
INPUT_FILE="hdfs:///Public/Data/Lab3Short"
#INPUT_FILE="hdfs:///Public/Data/Lab3Full"
OUTPUT_DIR="/u/home/sanrober/comp4333/programs/program_1/grid_points"
MAX_DIST=0.95


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
    --py-files "p1_class.py" \
    p1.py $CELL_SIZE $INPUT_FILE "hdfs://$OUTPUT_DIR" $MAX_DIST


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
