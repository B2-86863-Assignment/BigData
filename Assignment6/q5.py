from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("q5").config("spark.driver.memory", "4g").getOrCreate()

movie = spark.read.option('header','true')\
                    .option('inferSchema','true')\
                    .csv("/home/sunbeam/Git_pull/BigData/data/movies/movies.csv")

rating = spark.read.option('header','true')\
                    .option('inferSchema','true')\
                    .csv("/home/sunbeam/Git_pull/BigData/data/movies/ratings.csv")



rating_join = rating.alias("r1").join(rating.alias("r2"),on="userId",how='inner')\
    .select(col("r1.userId").alias("userId"),col("r1.movieId").alias("Movie1"),col("r1.rating").alias("Rating1"), col("r2.movieId").alias("Movie2"),col("r2.rating").alias("Rating2")	)\
    .filter("Movie1 < Movie2")

corr_table = rating_join.groupBy("Movie1","Movie2").agg(round(corr("Rating1","Rating2"),2).alias("Correlation"))





spark.stop()
