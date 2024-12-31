
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
                    .config("spark.sql.shuffle.partitions","2")\
                    .config('spark.sql.warehouse.dir', 'file:///home/sunbeam/bigdata/spark-warehouse')\
                    .enableHiveSupport()\
                    .getOrCreate()

df = spark.read.option('header','true').option('inferSchema','true').csv("/home/sunbeam/Git_pull/BigData/data/Fire_Service_Calls_Sample.csv")


# df.show()
# df.printSchema()

df.write.mode("OVERWRITE").saveAsTable("fire_service_calls")

spark.stop()
