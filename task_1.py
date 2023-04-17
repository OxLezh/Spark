from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Simple Spark app").master("local[2]").getOrCreate()

df = spark.read.option('header', True).option('inferSchema', True).csv("/home/oxana/Documents/Spark/сoviddata.csv") 

result = df.select('iso_code', 'location', (round(col( 'total_cases')/col('population')*100)).alias('ill, %')).\
where((col('date')== '2021-03-31')&(~col('iso_code').like('OWID%'))).sort(col('ill, %').desc()).limit(15)

result.show()

spark.stop()