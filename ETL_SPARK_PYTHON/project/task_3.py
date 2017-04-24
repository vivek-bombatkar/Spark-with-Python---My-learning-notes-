#task_3

#3. Join the two tables in Hive into table AB. The column “productid” can be used for that join.

#TODO
#1. take table names as input 

import sys, getopt
from pyspark import SparkContext, SparkConf, HiveContext


conf= SparkConf().setAppName("Asdasd")
sc=SparkContext(conf=conf)
sqlContext=HiveContext(sc)

#read tables from HIVE schema directly
df_table_a = sqlContext.sql("SELECT * FROM vivekb123.table_a_2016_10_16")

df_table_b = sqlContext.sql("SELECT * FROM vivekb123.table_B_2016_10_16")

#rename price field to price_b as we have samiller field in both the tables!
df_join2 = df_table_a.join(df_table_b.withColumnRenamed("price","price_B"),['productid'])


#dump the data 
df_join2.saveAsTable("vivekb123.table_A_able_B_2016_10_16")


