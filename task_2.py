from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Simple Spark app").master("local[2]").getOrCreate()

df = spark.read.option('header', True).option('inferSchema', True).csv("/home/oxana/Documents/Spark/сoviddata.csv")

data = df.select('location', 'new_cases', 'date').where((col('date').between('2021-03-29', '2021-03-31'))&(~col('iso_code').like('OWID%'))).sort(col('new_cases').desc())

max_value = data.groupBy('location').agg(first('new_cases').alias('max_cases'), first('date').alias('date')).sort(col('max_cases').desc()).limit(10)

result = max_value.select('date', 'location', 'max_cases')

result.show()

# result_save = result.write.options(header=True).csv("sample_result/TOP_10_country_max_cases_covid19.csv")

spark.stop()


