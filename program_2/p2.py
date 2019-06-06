#Sanjay Roberts

from pyspark import SparkContext, SparkConf
import math
import sys
from operator import add

print(sys.argv)
fund = sys.argv[1]
price = sys.argv[2]
output_dir = sys.argv[3]

conf = SparkConf().setAppName("Stocks")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

# Import Spark SQL
from pyspark.sql import SQLContext, Row
import pyspark.sql.functions as func
sqlCtx = SQLContext(sc)

fund_rdd = sc.textFile(fund).map(lambda l: l.split(","))
price_rdd = sc.textFile(price).map(lambda l: l.split(","))

#### TASK 1 ####

fund_df = fund_rdd.map(lambda l: Row(ticker=l[1], estimated_shares_outstanding=l[78])).toDF()
fund_df.registerTempTable("fund")

#print("average Estimated Shares Outstanding")
#sqlCtx.sql("SELECT ticker, AVG(estimated_shares_outstanding) AS avg_ESO FROM fund \
#WHERE estimated_shares_outstanding > 0 OR estimated_shares_outstanding != '' GROUP BY ticker").show()

avg_ESO_df =sqlCtx.sql("SELECT ticker, AVG(estimated_shares_outstanding) AS avg_ESO FROM fund \
WHERE estimated_shares_outstanding > 0 OR estimated_shares_outstanding != '' GROUP BY ticker")

avg_ESO_df.registerTempTable("ESO")

################

#### TASK 2 ####
price_df = price_rdd.map(lambda l: Row(ticker=l[1], close=float(l[3]))).toDF()
price_df.registerTempTable("price")

#print('mean')
#sqlCtx.sql("SELECT ticker, AVG(close) AS pMean from price GROUP BY ticker").show()
pMean_df = sqlCtx.sql("SELECT ticker, AVG(close) AS pMean from price GROUP BY ticker")
pMean_df.registerTempTable("pMean_df")

#print('max')
#sqlCtx.sql("SELECT ticker, MAX(close) AS pMax from price GROUP BY ticker").show()
pMax_df = sqlCtx.sql("SELECT ticker, MAX(close) AS pMax from price GROUP BY ticker")
pMax_df.registerTempTable("pMax_df")

#print('min')
#sqlCtx.sql("SELECT ticker, MIN(close) AS pMin from price GROUP BY ticker").show()
pMin_df = sqlCtx.sql("SELECT ticker, MIN(close) AS pMin from price GROUP BY ticker")
pMin_df.registerTempTable("pMin_df")

#print('pop variance')
import pyspark.sql.functions as F
#(price_df.groupBy("ticker").agg(F.var_pop("close").alias("pVar"))).show()
pVar_df = (price_df.groupBy("ticker").agg(F.var_pop("close").alias("pVar")))
pVar_df.registerTempTable("pVar_df")

#################

####TASK 3#######
valuation_df = sqlCtx.sql("SELECT p.ticker, p.close, e.avg_ESO, (p.close*e.avg_ESO) as valuation \
FROM ESO e, price p WHERE e.ticker=p.ticker")

valuation_df.registerTempTable("val")

#print('mean valuation')
#sqlCtx.sql("SELECT ticker, AVG(valuation) AS vMean from val GROUP BY ticker").show()
vMean_df = sqlCtx.sql("SELECT ticker, AVG(valuation) AS vMean from val GROUP BY ticker")
vMean_df.registerTempTable("vMean_df")

#print('max valuation')
#sqlCtx.sql("SELECT ticker, MAX(valuation) AS vMax from val GROUP BY ticker").show()
vMax_df = sqlCtx.sql("SELECT ticker, MAX(valuation) AS vMax from val GROUP BY ticker")
vMax_df.registerTempTable("vMax_df")


#print('min valuation')
#sqlCtx.sql("SELECT ticker, MIN(valuation) AS vMin from val GROUP BY ticker").show()
vMin_df = sqlCtx.sql("SELECT ticker, MIN(valuation) AS vMin from val GROUP BY ticker")
vMin_df.registerTempTable("vMin_df")

#print('pop variance')
#(valuation_df.groupBy("ticker").agg(F.var_pop("valuation").alias("vVar"))).show()
vVar_df = (valuation_df.groupBy("ticker").agg(F.var_pop("valuation").alias("vVar")))
vVar_df.registerTempTable("vVar_df")
#################

###TASK 4 ################

#print('Merge tables')
out = sqlCtx.sql("SELECT * FROM pMean_df a, pMin_df b, pMax_df c, pVar_df d, \
vMean_df e, vMin_df f, vMax_df g, vVar_df h \
WHERE a.ticker=b.ticker and a.ticker=c.ticker and a.ticker=d.ticker and \
a.ticker=e.ticker and a.ticker=f.ticker and a.ticker=g.ticker and \
a.ticker=h.ticker")

out_rdd = out.rdd.map(list)

final_out = out_rdd.map(lambda x: (x[0], '{}'.format(str(x[1]) +\
 ' ' + str(x[3]) + ' ' +  str(x[5]) + ' ' + str(x[7]) + ' ' + str(x[9])\
 + ' ' + str(x[11]) + ' ' + str(x[13]) + ' ' + str(x[15]))))

#print(final_out.collect())

final_out.saveAsTextFile(output_dir)

#############################

###TASK 5 ##########
count_price_df = sqlCtx.sql("SELECT p.ticker, p.close FROM price p \
LEFT JOIN ESO e \
ON e.ticker=p.ticker")

count_price_df.registerTempTable("price_count")
price_count = sqlCtx.sql("SELECT count(ticker) FROM price_count")

price_count = price_count.rdd.map(list).collect()
print('Stock price records read: {}'.format(price_count[0][0]))

###
#############

g = sqlCtx.sql("SELECT COUNT(estimated_shares_outstanding) FROM fund \
WHERE estimated_shares_outstanding > 0 OR estimated_shares_outstanding != '' ")

h = sqlCtx.sql("SELECT COUNT(estimated_shares_outstanding) FROM fund")
h = h.rdd.map(list).collect()

print('Fundamentals records read: {}'.format(h[0][0]))

#############
out = sqlCtx.sql("SELECT COUNT(a.ticker) AS ticker FROM pMean_df a, pMin_df b, pMax_df c, pVar_df d, \
vMean_df e, vMin_df f, vMax_df g, vVar_df h \
WHERE a.ticker=b.ticker and a.ticker=c.ticker and a.ticker=d.ticker and \
a.ticker=e.ticker and a.ticker=f.ticker and a.ticker=g.ticker and \
a.ticker=h.ticker")

#out.show()
b = sqlCtx.sql("SELECT COUNT(DISTINCT ticker) FROM fund")
c = sqlCtx.sql("SELECT COUNT(DISTINCT ticker) FROM price")

a = out.rdd.map(list).collect()
b = b.rdd.map(list).collect()
c = c.rdd.map(list).collect()

k = a[0][0]
l = b[0][0]
i = c[0][0]

print("Missing price tickers: {}".format(i-k))
print("Missing fundamentals tickers: {}".format(l-k))

sc.stop()



