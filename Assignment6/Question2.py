# 2. Find max sal, min sal, avg sal, total sal per dept per job in emp.csv ﬁle.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder\
        .appName("Question2")\
        .getOrCreate()

empfile = "/home/aditya/Desktop/Sunbeam/BigData/data/emp.csv"
empSchema = "empno INT, ename STRING,job STRING,mgr INT, hire STRING,sal DOUBLE,comm DOUBLE,deptno INT"

df = spark.read\
    .schema(empSchema)\
    .option("path",empfile)\
    .format("csv")\
    .load()

df_filled = df.fillna({'comm':0})

result = df_filled.groupBy(["deptno","job"])\
        .agg(max("sal").alias("max_sal"),min("sal").alias("min_sal"),avg("sal").alias("avg_sal"),sum("sal").alias("total_sal"))\
        .sort(["deptno","job"])

result.show()
# df_filled.show()

# df.show()
spark.stop()

'''
+------+---------+-------+-------+-------+---------+
|deptno|      job|max_sal|min_sal|avg_sal|total_sal|
+------+---------+-------+-------+-------+---------+
|    10|    CLERK| 1300.0| 1300.0| 1300.0|   1300.0|
|    10|  MANAGER| 2450.0| 2450.0| 2450.0|   2450.0|
|    10|PRESIDENT| 5000.0| 5000.0| 5000.0|   5000.0|
|    20|  ANALYST| 3000.0| 3000.0| 3000.0|   6000.0|
|    20|    CLERK| 1100.0|  800.0|  950.0|   1900.0|
|    20|  MANAGER| 2975.0| 2975.0| 2975.0|   2975.0|
|    30|    CLERK|  950.0|  950.0|  950.0|    950.0|
|    30|  MANAGER| 2850.0| 2850.0| 2850.0|   2850.0|
|    30| SALESMAN| 1600.0| 1250.0| 1400.0|   5600.0|
+------+---------+-------+-------+-------+---------+
'''
