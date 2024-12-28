from sqlite3.dbapi2 import Timestamp

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark =SparkSession.builder.appName("q4").getOrCreate()


df = spark.read.option('header','true').option('inferSchema','true')\
    .csv("/home/sunbeam/Git_pull/BigData/data/movies/ratings.csv")


df = df.withColumn("tst",to_timestamp("timestamp")).drop("timestamp")

df.groupBy(year("tst")).count().show()
# df.show()
spark.stop()