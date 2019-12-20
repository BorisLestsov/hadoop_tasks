import pyspark
import argparse


parser = argparse.ArgumentParser()
parser.add_argument('parquet_file', type=str)
args = parser.parse_args()

spark = pyspark.sql.SparkSession.builder\
    .appName("Read parquet_file")\
    .getOrCreate()

df = spark.read.parquet(args.parquet_file)
df.show()
