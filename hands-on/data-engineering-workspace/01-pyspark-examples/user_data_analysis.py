from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
import sys

spark = SparkSession.builder.getOrCreate()
input_path = sys.argv[1]
output_path = sys.argv[2]

df = spark.read.csv(path=input_path, header=True, inferSchema=True)
result_df = df.groupBy("designation").agg(avg("salary"))
result_df.write.save(path=output_path, mode="OVERWRITE")