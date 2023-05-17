# Databricks notebook source
from pyspark.sql.functions import col, when

# COMMAND ----------

hourly_df = spark.read.format("delta").load("abfss://silver@bikesharingdl.dfs.core.windows.net/hourly_full_data")

# COMMAND ----------

display(hourly_df)

# COMMAND ----------

weather_df = hourly_df.select(col('weather_id'))

# COMMAND ----------

#drop duplicates
weather_unique_df = weather_df.dropDuplicates()

# COMMAND ----------

rain = {
    1: 'No Rain',
    2: 'Mist',
    3: 'Light Rain',
    4: 'Heavy Rain'
}

clouds = {
    1: 'Clear Sky',
    2: 'Few Clouds',
    3: 'Few Clouds',
    4: 'Cloudly'
}

snow = {
    1: 'No Snow',
    2: 'No Snow',
    3: 'Light Snow',
    4: 'Ice Pallets'
}


# COMMAND ----------

weather_final_df = weather_unique_df.withColumn('weather_condition',
when(col("weather_id") == 1, 'Perfect') \
.when(col("weather_id") == 2, 'Not Bad') \
.when(col("weather_id") == 3, 'Bad') \
.when(col("weather_id") == 4, 'Worst') \
)

# COMMAND ----------

weather_final_df = weather_final_df.withColumn('rain',
when(col("weather_id") == 1, 'No Rain') \
.when(col("weather_id") == 2, 'Mist') \
.when(col("weather_id") == 3, 'Light Rain') \
.when(col("weather_id") == 4, 'Heavy Rain') \
)

# COMMAND ----------

weather_final_df = weather_final_df.withColumn('clouds',
when(col("weather_id") == 1, 'Clear Sky') \
.when(col("weather_id") == 2, 'Few Clouds') \
.when(col("weather_id") == 3, 'Few Clouds') \
.when(col("weather_id") == 4, 'Cloudly') \
)

# COMMAND ----------

weather_final_df = weather_final_df.withColumn('snow',
when(col("weather_id") == 1, 'No Snow') \
.when(col("weather_id") == 2, 'No Snow') \
.when(col("weather_id") == 3, 'Light Snow') \
.when(col("weather_id") == 4, 'Ice Pallets') \
)

# COMMAND ----------

display(weather_final_df)

# COMMAND ----------

weather_final_df.write.mode("overwrite").format("delta").saveAsTable("bike_sharing_gold.dim_weather")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from bike_sharing_gold.dim_weather