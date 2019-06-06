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

edges = sqlCtx.createDataFrame(file.map(lambda x: x.split(",")).collect(), ["src", "dst", "weight"])

srcVerts = edges.select("src").distinct()
dstVerts = edges.select("dst").distinct()
verts = srcVerts.union(dstVerts).distinct().sort("src").select(F.col("src").alias("id"))

g = GraphFrame(verts, edges)
#g.vertices.show()
#g.edges.show()


##########################

# The pregel functions

# What we want the initial extra column (label in this case) set to
# We will set everyone's to the unvisited label (100 for infinity, just use big number), except for the 
#   selected root (in this case we are picking node 0 as the root).  The
#   root gets set to its own id
def initialValue():
    return F.when(F.col("id") == F.lit(0), F.lit(0)).otherwise(F.lit(100))

# Update our value with the received one.
# If we get a null then we just keep our own label since we didn't
#    receive a message
def updatedValue():
    return F.when(Pregel.msg().isNull(), F.col("short")).otherwise(Pregel.msg())

# Sending messages along the edges in the direction of the edge
# If Dst is unvisited and Src is visited then send the Src id, otherwise null
def sendDst():
    return F.when((Pregel.dst("short") == F.lit(100)), Pregel.src("id")+Pregel.edge("weight")).otherwise(F.lit(None))

# If Src is unvisited and Dst is visited then send the Dst id, otherwise null
def sendSrc():
    return F.when((Pregel.src("short") == F.lit(100)), Pregel.dst("id")+Pregel.edge("weight")).otherwise(F.lit(None))


# Called when we want to aggregrate the incoming messages
# If there is more than one, we can take any of them,
#   for repeatibility take the smallest
def agg():
    return F.min(Pregel.msg())

# The pregel setup calling the above functions to do most of the work
short = g.pregel.setMaxIter(5).setCheckpointInterval(0).\
withVertexColumn("short", initialValue(), updatedValue()).\
sendMsgToDst(sendDst()).sendMsgToSrc(sendSrc()).aggMsgs(agg()).run()

# Show the results - this is basically our edges
short.show()

##########################
#---------------------
sc.stop()


#end results, order doens't matter
# (0,0)
# (1,1)
# (2,3)
# (3,2)
# (4,6)


#my results
#+---+-----+
#| id|short|
#+---+-----+
#|  3|  2.0|
#|  0|  0.0|
#|  1|  1.0|
#|  4|  6.0|
#|  2|  3.0|
#+---+-----+





