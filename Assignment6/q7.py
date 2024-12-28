from pyspark.sql import SparkSession
from pyspark.sql.functions import col, corr, round as spark_round


spark = SparkSession.builder.appName("q5").config("spark.driver.memory", "4g").getOrCreate()


movie = spark.read.option('header', 'true') \
                  .option('inferSchema', 'true') \
                  .csv("/home/sunbeam/Git_pull/BigData/data/movies/movies.csv")
movie.createOrReplaceTempView("movies")


rating = spark.read.option('header', 'true') \
                   .option('inferSchema', 'true') \
                   .csv("/home/sunbeam/Git_pull/BigData/data/movies/ratings.csv")
rating.createOrReplaceTempView("ratings")


rating_join_query = """
    SELECT 
        r1.userId AS userId,
        r1.movieId AS Movie1,
        r1.rating AS Rating1,
        r2.movieId AS Movie2,
        r2.rating AS Rating2
    FROM ratings r1
    INNER JOIN ratings r2
    ON r1.userId = r2.userId
    WHERE r1.movieId < r2.movieId
"""
rating_join = spark.sql(rating_join_query)
rating_join.createOrReplaceTempView("rating_join")


corr_table_query = """
    SELECT 
        Movie1,
        Movie2,
        IFNULL(ROUND(CORR(Rating1, Rating2), 2),0) AS Correlation
    FROM rating_join
    GROUP BY Movie1, Movie2
"""
corr_table = spark.sql(corr_table_query)
corr_table.createOrReplaceTempView("corr_table")

corr_table.show()

spark.stop()
