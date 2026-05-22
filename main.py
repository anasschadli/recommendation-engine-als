import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestPySpark") \
    .master("local[*]") \
    .getOrCreate()

df = spark.createDataFrame([(1, "Anass"), (2, "Sara")], ["id", "name"])
df.show()

spark.stop()