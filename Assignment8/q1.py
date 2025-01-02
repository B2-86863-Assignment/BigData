from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
from pyspark.sql.functions import *

spark = SparkSession.builder\
       .appName("q1")\
       .config("spark.sql.shuffle.partitions","2")\
       .getOrCreate()


df = spark.read\
    .schema("empno INT ,ename STRING,job STRING,mgr INT ,hire DATE,sal DOUBLE,comm DOUBLE,deptno INT")\
    .csv("/home/sunbeam/Cdac/BigDataAssignments/data/emp.csv")
    

df.printSchema()
df.show()

df2 = df.toPandas()

print(df2)


plt.scatter([df2['empno']],[df2['sal']])

spark.stop()