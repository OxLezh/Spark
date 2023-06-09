from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Simple Spark app").master("local[2]").getOrCreate()

df = spark.read.option('header', True).option('inferSchema', True).csv("/home/oxana/Documents/Spark/сoviddata.csv")

data = df.select('location', col('new_cases').alias('new_cases_today'), 'date').\
    where((col('date').between('2021-03-23', '2021-03-31'))&(col('location') == 'Russia'))

w = Window().partitionBy('location').orderBy('date')

win_today = data.withColumn('new_cases_yesterday',(lag('new_cases_today').over(w)))

result = win_today.select('date', 'new_cases_today', 'new_cases_yesterday', (col('new_cases_today') - col('new_cases_yesterday')).alias('delta')).\
    na.fill(value=0).where(col('date').between('2021-03-23', '2021-03-31'))

result.show()

spark.stop()


