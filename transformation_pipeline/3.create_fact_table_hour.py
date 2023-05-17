# Databricks notebook source
hourly_df = spark.read.format("delta").load("abfss://silver@bikesharingdl.dfs.core.windows.net/hourly_full_data")

# COMMAND ----------

hour_df = hourly_df.select('id', 'date_','hour_', 'weather_id', 'temperature', 'feel_like_temperature',
                    'humidity', 'wind_speed', 'casual_riders', 'registeres_riders', 'total_riders')

# COMMAND ----------

hour_df.write.mode("overwrite").format("delta").saveAsTable("bike_sharing_gold.fact_hour")

# COMMAND ----------

display(hour_df)