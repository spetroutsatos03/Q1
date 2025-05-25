# Q1
Να υλοποιηθεί το Query 1 χρησιμοποιώντας τα RDD και DataFrame APIs (με udf και χωρίς). Σχολιάστε τις διαφορές στην επίδοση μεταξύ των διαφορετικών υλοποιήσεών σας. 


from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, avg

spark = SparkSession.builder \
    .appName("Parquet Analysis") \
    .getOrCreate()


df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/spetroutsatos/data/parquet/yellow_tripdata_2015")


no_zeros_df = df.filter((df.pickup_longitude != 0) & (df.pickup_latitude != 0))

no_zeros_df.select("tpep_pickup_datetime", "pickup_longitude" , "pickup_latitude").show()

df_with_hour = no_zeros_df.withColumn("hour", hour("tpep_pickup_datetime"))

hour_counts = df_with_hour.groupBy("hour").count().orderBy("hour")

hour_counts.show(24)

mean_pickup_longitude = df_with_hour.groupBy("hour").agg(avg("pickup_longitude").alias("mean_pickup_longitude_per_hour")).orderBy("hour")
mean_pickup_longitude.show(24)


mean_pickup_latitude = df_with_hour.groupBy("hour").agg(avg("pickup_latitude").alias("mean_pickup_latitude")).orderBy("hour")
mean_pickup_latitude.show(24)

mean_pick_ups = mean_pickup_longitude.join(mean_pickup_latitude, on="hour", how="inner").orderBy("hour")
mean_pick_ups.show(24)
