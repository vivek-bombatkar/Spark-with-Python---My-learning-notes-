#task_2

import sys, getopt
from pyspark import SparkContext, SparkConf, HiveContext

#check if we have received arguments as inputs to the script
if len(sys.argv) == 2:
	# get weeks data
	#check if week passed as param
	# if use use it 
	strWeek = sys.argv[1]
else:
	#else fetch all data as defauly
	strWeek = "all_weeks"


conf= SparkConf().setAppName("Asdasd")
sc=SparkContext(conf=conf)
sqlContext=HiveContext(sc)

#connect to Mysql
df_dataset_A = sqlContext.read.format("jdbc").options(
url ="jdbc:mysql://nn01.itversity.com:3306/retail_export",
driver="com.mysql.jdbc.Driver",
dbtable="dataset_A",
user="retail_dba",
password="itversity"
).load()

df_dataset_B = sqlContext.read.format("jdbc").options(
url ="jdbc:mysql://nn01.itversity.com:3306/retail_export",
driver="com.mysql.jdbc.Driver",
dbtable="dataset_B",
user="retail_dba",
password="itversity"
).load()


#logic to derive where clause based on input provided
strSql = ""
if strWeek <> "all_weeks":
	strSql = " where week = \'" + strWeek + "\'" 

df_dataset_A.registerTempTable("dataset_A")
df_dataset_B.registerTempTable("dataset_B")


#table names
table_a = "vivekb123.table_a_"+ strWeek.replace("-","_")
table_b = "vivekb123.table_b_"+ strWeek.replace("-","_")

#overwrite table if exist
sqlContext.sql("DROP TABLE IF EXISTS  " + table_a)
sqlContext.sql("DROP TABLE IF EXISTS  " + table_b)

#dump the data 
df_dataset_A_filtered = sqlContext.sql("select * from dataset_A " + strSql).saveAsTable(table_a)
df_dataset_B_filtered = sqlContext.sql("select * from dataset_B").saveAsTable(table_b)


