from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("q6").config("spark.driver.memory", "4g").getOrCreate()

df = spark.read.option("header","true").option('inferSchema','true').csv("/home/sunbeam/Git_pull/BigData/data/Fire_Service_Calls_Sample.csv")

# Queestion 1 :
# cnt = df.select("Call Type").distinct().count()
# print(f"Distinct call types : {cnt}")

# Question 2 :
# df.select("Call Type").distinct().show(truncate=False)

delayed_responses = df.filter(
    (unix_timestamp("Response DtTm") - unix_timestamp("Received DtTm")) > 5
).count()
print(f"Delayed Responses > 5 mins: {delayed_responses}")


# 4. Most common call types
call_type_count = df.groupBy("Call Type") \
    .agg(count("Call Type").alias("Count")) \
    .orderBy(col("Count").desc())
call_type_count.show()

# 5. Zip codes with most common calls
zipcode_calltype_count = df.groupBy("Zipcode of Incident", "Call Type") \
    .agg(count("Call Type").alias("Count")) \
    .orderBy(col("Count").desc()) \
    .limit(50)
zipcode_calltype_count.show()

# 6. Neighborhoods in San Francisco for zip codes 94102 and 94103
neighborhoods_sf = df.filter((col("City") == "San Francisco") &
                                       (col("Zipcode of Incident").isin(94102, 94103))) \
                               .select("Analysis Neighborhoods") \
                               .distinct()
neighborhoods_sf.show()

# 7. Sum, average, min, and max of response times
response_times = df.select(
    (unix_timestamp("Response DtTm") - unix_timestamp("Dispatch DtTm")).alias("Response Time")
)

response_times_summary = response_times.select(
    sum("Response Time").alias("Total Response Time"),
    avg("Response Time").alias("Average Response Time"),
    min("Response Time").alias("Minimum Response Time"),
    max("Response Time").alias("Maximum Response Time")
)
response_times_summary.show()

# 8. Distinct years in the dataset
distinct_years = df.select(year("Call Date").alias("Year")).distinct().count()
print(f"Distinct Years: {distinct_years}")

# 9. Week of 2018 with the most fire calls
most_fire_calls_week = df.filter(year("Call Date") == 2018) \
    .groupBy(weekofyear("Call Date").alias("Week of Year")) \
    .agg(count("*").alias("Total Calls")) \
    .orderBy(col("Total Calls").desc()) \
    .limit(1)
most_fire_calls_week.show()

# 10. Neighborhoods with the worst response time in 2018
worst_response_time_2018 = df.filter((year("Call Date") == 2018) &
                                               (col("City") == "San Francisco")) \
                                       .groupBy("Analysis Neighborhoods") \
                                       .agg(max(unix_timestamp("Response DtTm") - unix_timestamp("Dispatch DtTm")).alias("Worst Response Time")) \
                                       .orderBy(col("Worst Response Time").desc()) \
                                       .limit(1)
worst_response_time_2018.show()




spark.stop()