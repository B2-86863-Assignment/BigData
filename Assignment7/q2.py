from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.config('spark.sql.shuffle.partitions','2').getOrCreate()

jdbcUrl = "jdbc:mysql://localhost:3306/classwork_db"
driver="com.mysql.cj.jdbc.Driver"

df = spark.read.option('driver',driver)\
                .option('user','root')\
                .option('password','manager')\
                .jdbc(url=jdbcUrl,table='ncdc')

# df.select('yr').distinct().show()

df.groupBy('yr').agg(avg('temp').alias("AVGtemp")).orderBy(desc("AVGtemp")).show()

spark.stop()