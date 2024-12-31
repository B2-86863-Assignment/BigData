from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.config('spark.sql.shuffle.partitions','2').getOrCreate()

jdbcUrl = "jdbc:mysql://localhost:3306/classwork_db"
driver="com.mysql.cj.jdbc.Driver"

df= spark.read.text("/home/sunbeam/Git_pull/BigData/data/ncdc/1901")

regex = "^.{15}([0-9]{4}).{68}([-\+][0-9]{4})([0-9]).*$"
ncdc = df.select(regexp_extract("value", regex, 1).alias("yr").cast('SHORT'), regexp_extract("value", regex, 2).alias("temp").cast('SHORT'), regexp_extract("value", regex, 3).alias("quality").cast('BYTE'))\
    .where("temp != 9999 AND quality IN (0,1,2,4,5,9)")


ncdc.write.option('driver',driver) \
    .option("user", "root") \
    .option("password", "manager") \
    .mode("overwrite") \
    .jdbc(url=jdbcUrl, table="ncdc")

print("NCDC loaded successfully.")

spark.stop()