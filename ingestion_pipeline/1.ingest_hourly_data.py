# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest hour.csv file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, DateType, BooleanType
from pyspark.sql.functions import col, year

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV files using the spark dataframe reader API

# COMMAND ----------

hourly_data_schema = StructType(fields =
[
    StructField("instant", IntegerType(), False),
    StructField("dteday", DateType(), True),
    StructField("season", IntegerType(), True),
    StructField("yr", IntegerType(), True),
    StructField("mnth", IntegerType(), True),
    StructField("hr", IntegerType(), True),
    StructField("holiday", IntegerType(), True),
    StructField("weekday", IntegerType(), True),
    StructField("workingday", IntegerType(), True),
    StructField("weathersit", IntegerType(), True),
    StructField("temp", DoubleType(), True),
    StructField("atemp", DoubleType(), True),
    StructField("hum", DoubleType(), True),
    StructField("windspeed", DoubleType(), True),
    StructField("casual", IntegerType(), True),
    StructField("registered", IntegerType(), True),
    StructField("cnt", IntegerType(), True)
])

# COMMAND ----------

hourly_df = spark.read \
    .option("header", True) \
    .schema(hourly_data_schema) \
    .csv("abfss://bronze@bikesharingdl.dfs.core.windows.net/hour.csv")

# COMMAND ----------

hourly_df.columns

# COMMAND ----------

hourly_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns as per requirement

# COMMAND ----------

hourly_renamed_df = hourly_df.withColumnRenamed("instant", "id") \
                            .withColumnRenamed("dteday", "date_") \
                            .withColumnRenamed("mnth", "month_") \
                            .withColumnRenamed("hr", "hour_") \
                            .withColumnRenamed("weekday", "week_day") \
                            .withColumnRenamed("workingday", "working_day") \
                            .withColumnRenamed("weathersit", "weather_id") \
                            .withColumnRenamed("temp", "temperature") \
                            .withColumnRenamed("atemp", "feel_like_temperature") \
                            .withColumnRenamed("hum", "humidity") \
                            .withColumnRenamed("windspeed", "wind_speed") \
                            .withColumnRenamed("casual", "casual_riders") \
                            .withColumnRenamed("registered", "registeres_riders") \
                            .withColumnRenamed("cnt", "total_riders")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Drop yr column and create a new column: year

# COMMAND ----------

hourly_dropped_df = hourly_renamed_df.drop(col('yr'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Create new column: year

# COMMAND ----------

hourly_final_df = hourly_dropped_df.withColumn("year", year(col("date_")))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write data to delta lake silver containter

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE bike_sharing_silver.hourly_full_data

# COMMAND ----------

hourly_final_df.write.mode("overwrite").partitionBy('year').format("delta").saveAsTable("bike_sharing_silver.hourly_full_data")