#!/bin/bash

# Run the graphframes code

graphfile="/tiny_graph.csv"

spark-submit \
    --master "local[*]" \
    --deploy-mode "client" \
    --packages graphframes:graphframes:0.7.0-spark2.4-s_2.11 \
    shortest.py "file://$graphfile"

exit $?
