from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("q6").config("spark.driver.memory", "4g").getOrCreate()

df = spark.read.option('header','true').option('inferSchema','true')\
    .csv("/home/sunbeam/Git_pull/BigData/data/movies/ratings.csv")


df = df.withColumn("tst",to_timestamp("timestamp")).drop("timestamp")

# df.groupBy(month("tst")).count().show()
df.createOrReplaceTempView("v_view")
spark.sql("SELECT MONTH(tst),COUNT(rating) FROM v_view GROUP BY MONTH(tst)").show()



spark.stop()