# 1. Wordcount using Spark Dataframes and ﬁnd top 10 words (except stopwords). Take ﬁle from HDFS/S3.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
    .appName("Question1")\
    .config("spark.sql.shuffle.partitions","3")\
    .getOrCreate()

licSchema = "textfile"
licPath = "hdfs://localhost:9000/user/aditya/new_wordcount/LICENSE"


df = spark.read.text(licPath)


result = df\
    .withColumn("text",explode(split("value",r"[^a-zA-Z]")))\
    .where("len(text)>1")\
    .select(lower("text").alias("text"))\
    .where('''text not in ('i', 'me', 'my', 'myself', 'we', 'our', 'ours',
'ourselves', 'you', "you're", "you've", "you'll", "you'd", 'your', 'yours', 'yourself',
'yourselves', 'he', 'him', 'his', 'himself', 'she', "she's", 'her', 'hers', 'herself',
'it', "it's", 'its', 'itself', 'they', 'them', 'their', 'theirs',
 'what', 'which', 'who', 'whom', 'this', 'that', "that'll", 'these', 'those', 'am', 'is', 'are',
 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing',
 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for',
'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to',
'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once',
'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most',
'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very',
's', 't', 'can', 'will', 'just', 'don', "don't", 'should', "should've", 'now', 'd', 'll', 'm', 'o', 're', 've', 'y',
 'ain', 'aren', "aren't", 'couldn', "couldn't", 'didn', "didn't", 'doesn', "doesn't",
'hadn', "hadn't", 'hasn', "hasn't", 'haven', "haven't", 'isn', "isn't", 'ma', 'mightn',
 "mightn't", 'mustn', "mustn't", 'needn', "needn't", 'shan', "shan't", 'shouldn', "shouldn't",
'wasn', "wasn't", 'weren', "weren't", 'won', "won't", 'wouldn', "wouldn't","www")''')\
    .drop('value')

# result.show()
newresult = result.groupBy("text").count()\
    .orderBy(desc("count"))
newresult.show(n=10)
# df.show(truncate=False)

#
# +-------+-----+
# |   text|count|
# +-------+-----+
# |    org|  187|
# | apache|  109|
# |    com|   50|
# |license|   43|
# |commons|   37|
# | hadoop|   35|
# |   work|   34|
# |  jetty|   32|
# |   hive|   30|
# |jackson|   28|
# +-------+-----+
#
#
