#!/bin/bash

# Run the graphframes code

graphfile="/u/home/sanrober/comp4333/programs/program_3/connect.csv"

spark-submit \
    --master "local[*]" \
    --deploy-mode "client" \
    --packages graphframes:graphframes:0.7.0-spark2.4-s_2.11 \
    connected.py "file://$graphfile"

exit $?
