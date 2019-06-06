#!/bin/bash

# Run the graphframes code

graphfile="/u/home/sanrober/comp4333/programs/program_3/tiny_graph.csv"

spark-submit \
    --master "local[*]" \
    --deploy-mode "client" \
    --packages graphframes:graphframes:0.7.0-spark2.4-s_2.11 \
    shortest.py "file://$graphfile"

exit $?
