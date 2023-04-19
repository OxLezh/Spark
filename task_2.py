from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Simple Spark app").master("local[2]").getOrCreate()

df = spark.read.option('header', True).option('inferSchema', True).csv("/home/oxana/Documents/Spark/сoviddata.csv")

data = df.select(col('location'), 'new_cases', 'date').where((col('date').between('2021-03-24', '2021-03-31'))&(~col('iso_code').like('OWID%')))

max_value = data.groupBy('location').agg(max('new_cases').alias('max_cases')).sort(col('max_cases').desc()).limit(10)

result = data.alias('a').join(max_value.alias('b'), col('a.new_cases') ==col('b.max_cases'),"inner").\
    select('date', col('a.location'), 'max_cases').sort(col('max_cases').desc())

result.show()

# result_save = result.write.options(header=True).csv("sample_result/TOP_10_country_max_cases_covid19.csv")

spark.stop()
