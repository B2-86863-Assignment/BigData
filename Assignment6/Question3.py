
#
# 3. Find deptwise total sal from emp.csv and dept.csv. Print dname and total sal.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
    .appName("Question3")\
    .getOrCreate()

empfile = '/home/aditya/Desktop/Sunbeam/BigData/data/emp.csv'
deptfile = '/home/aditya/Desktop/Sunbeam/BigData/data/dept.csv'
empSchema = 'empno INT, ename STRING,job STRING,mgr INT, hire STRING,sal DOUBLE,comm DOUBLE,deptno INT'
deptSchema = 'deptno INT, dname STRING, location STRING'


emp_df = spark.read\
    .schema(empSchema)\
    .option("path",empfile)\
    .format("csv")\
    .load()

dept_df = spark.read\
    .schema(deptSchema)\
    .option("path",deptfile)\
    .format("csv")\
    .load()

df_joined = emp_df.join(dept_df,'deptno','right').select('ename','dname','sal')

result = df_joined.groupBy("dname")\
    .agg(ifnull(sum('sal'),lit(0)).alias("total"))

result.show()
spark.stop()


'''
+----------+-------+
|     dname|  total|
+----------+-------+
|OPERATIONS|   NULL|
|     SALES| 9400.0|
|  RESEARCH|10875.0|
|ACCOUNTING| 8750.0|
+----------+-------+
'''