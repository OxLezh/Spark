from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Simple Spark app").master("local[2]").getOrCreate()

df = spark.read.option('header', True).option('inferSchema', True).csv("/home/oxana/Documents/Spark/сoviddata.csv") 

data = df.select('iso_code', 'location', 'total_cases').where((col('date')== '2021-03-31'))

total = data.select(("total_cases")).where((col('location') == "World")).alias("total").collect()[0][0]

result = data.select('iso_code', 'location', round(((col('total_cases')/total)*100), 2).alias('ill, %')).where(~col('iso_code').like('OWID%')).sort(col('ill, %').desc()).limit(15)

result.show()

# result_save = result.write.options(header=True).csv("sample_result/TOP_15_country_ill_covid19.csv")

spark.stop()