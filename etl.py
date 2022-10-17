from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, regexp_replace, when

spark = SparkSession.builder.getOrCreate()

df = spark.read.csv('gs://[BUCKET_NAME]/data/online_retail_II.csv', header=True, inferSchema=True)

df = df.withColumnRenamed('Price', 'GBPPrice') \
    .withColumn('InvoiceDateTime', to_timestamp(col='InvoiceDate', format='dd/MM/yy HH:mm')) \
    .withColumn('StockCode', regexp_replace(df['Stockcode'], r'[A-Za-z]', '')) \
    .withColumn('Customer ID', when(df['Customer ID'].isNull(), -1).otherwise(df['Customer ID']))

df.write.csv('gs://[BUCK_NAME]/output/cleaned_online_retail_II', header=True)

spark.stop()