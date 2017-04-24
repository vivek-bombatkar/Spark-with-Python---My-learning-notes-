'''
ETL pipeline using Apache Spark!
MySQL -> Spark-DataFrame(HiveContext) -> HIVE(hdfs)

todo
4. create new partitions each time
6. mode ETL, like cleanning rejecting record & scd2 ?
'''

import sys, getopt
import ConfigParser
from pyspark import SparkContext, SparkConf, HiveContext

def getMysqlConn(url, driver, dbtable, user, password):
        print (" ############ inside getMysqlConn" )
        df_dataset = sqlContext.read.format("jdbc").options(
        url = url,
        driver=driver,
        dbtable=dbtable,
        user=user,
        password=password
        ).load()

        return df_dataset
try:
        conf= SparkConf().setAppName("Spark_ETL")
        sc=SparkContext(conf=conf)
        sqlContext=HiveContext(sc)
        
        conf =  ConfigParser.ConfigParser()
        conf.read("param.config")

        url = conf.get("MySQL","url")
        driver = conf.get("MySQL","driver")
        dbtable_A = conf.get("MySQL","dbtable_A")
        dbtable_B = conf.get("MySQL","dbtable_B")
        user = conf.get("MySQL","user")
        password = conf.get("MySQL","password")
        HiveSchema = conf.get("HiveSchema","schema")

        print (url,driver,dbtable_A,dbtable_B,user,password)

        #check if we have received arguments as inputs to the script
        strWeek = sys.argv[1] if len(sys.argv) == 2 else "all_weeks"

        #logic to derive where clause based on input provided
        strSql = "" if strWeek == "all_weeks" else " where week = \'" + strWeek + "\'"

        
        df_dataset_A = getMysqlConn(url,driver,dbtable_A,user,password)
        df_dataset_B = getMysqlConn(url,driver,dbtable_B,user,password)

                
        df_dataset_A.registerTempTable("dataset_A")
        df_dataset_B.registerTempTable("dataset_B")

        df_dataset_A_filtered = sqlContext.sql("select * from dataset_A " + strSql)
        df_dataset_B_filtered = sqlContext.sql("select * from dataset_B")

                #df_dataset_A_filtered.printSchema()
                #df_dataset_B_filtered.printSchema()        

                #rename price field to price_b as we have samiller field in both the tables!
        df_join = df_dataset_A_filtered.join(df_dataset_B_filtered.withColumnRenamed("price","price_B"),['productid'])
                
                #dump the data 
        df_join.saveAsTable(HiveSchema + ".table_A_Joined_B_" + datetime.datetime.now().strftime('%Y_%m_%d_%H%M%S'))


except Exception as e:
        print (" ### Exception ###: ", e)