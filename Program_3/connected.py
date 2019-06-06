#Sanjay Roberts

from pyspark import SparkContext, SparkConf

import sys

conf = SparkConf().setAppName("Graph")
sc = SparkContext(conf=conf)

sc.setLogLevel("WARN")

# Import Spark SQL for DataFrames (for use in GraphFrames)
from pyspark.sql import SQLContext
sqlCtx = SQLContext(sc)

from graphframes import *    # for graphframes
from pyspark.sql import functions as F   # for the functions we need to work with GraphFrames
from graphframes.lib import Pregel    # for Pregel

# load in the edges
file = sys.argv[1]
file = sc.textFile(file)

edges = sqlCtx.createDataFrame(file.map(lambda x: x.split(" ")).collect(), ["src", "dst"])

srcVerts = edges.select("src").distinct()
dstVerts = edges.select("dst").distinct()
verts = srcVerts.union(dstVerts).distinct().sort("src").select(F.col("src").alias("id"))

g = GraphFrame(verts, edges)
g.vertices.show()
g.edges.show()

###############################

# The pregel functions

# What we want the initial extra column (label in this case) set to
# We will set everyone's to there initial value
def initialValue():
    return F.col("id")

# Update our value with the received one.
# keep smallest value
def updatedValue():
    return F.when(Pregel.msg() >= F.col("connect"), F.col("connect")).otherwise(Pregel.msg())

# Sending messages along the edges in the direction of the edge
def sendDst():
    return Pregel.src("connect")

def sendSrc():
    return Pregel.dst("connect")

# Called when we want to aggregrate the incoming messages
# If there is more than one, we can take any of them,
#   for repeatibility take the smallest
def agg():
    return F.min(Pregel.msg())

# The pregel setup calling the above functions to do most of the work
connect = g.pregel.setMaxIter(5).setCheckpointInterval(0).\
withVertexColumn("connect", initialValue(), updatedValue()).\
sendMsgToDst(sendDst()).sendMsgToSrc(sendSrc()).aggMsgs(agg()).run()

# Show the results - this is our connected components
# So vertice and connected component
connect.show()


##############################
#---------------------
sc.stop()



# what we want
# (0,0)
# (1,1)
# (2,1)
# (3,3)
# (4,0)
# (5,3)
# (6,0)
# (7,0)
# (8,3)
# (9,0)


#
# What i get
# +---+-------+
# | id|connect|
# +---+-------+
# |  7|      0|
# |  3|      3|
# |  8|      3|
# |  0|      0|
# |  5|      3|
# |  6|      0|
# |  9|      0|
# |  1|      1|
# |  4|      0|
# |  2|      1|
# +---+-------+
































