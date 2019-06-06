#!/bin/bash

#Sanjay Roberts


# Run p1.py using Spark locally (on a single machine)

# Set cell size, and names of files for input and output.
# Paths must be absolute paths.
CELL_SIZE=1
INPUT_FILE="/Lab3Short"
#INPUT_FILE="/Lab3Full"
OUTPUT_DIR="/u/home/grid_points"
MAX_DIST=0.95

# Delete any old copies output directory on linux filesystem
rm -rf "$OUTPUT_DIR"


# Run the job passing command line parameters for the input/output directories
# if HDFS is configured on the system, that is the default for spark
# Override with prefix "file://" when passing to python/spark
spark-submit \
    --master "local[*]" \
    --deploy-mode "client" \
    p1.py $CELL_SIZE "file://$INPUT_FILE" "file://$OUTPUT_DIR" $MAX_DIST

exit $?

