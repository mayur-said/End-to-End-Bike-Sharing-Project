# Databricks notebook source
# MAGIC %md
# MAGIC ### Create Dimention Table: Date

# COMMAND ----------

from pyspark.sql.functions import col, when

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read Transformed Hourly Data

# COMMAND ----------

hourly_df = spark.read.format("delta").load("abfss://silver@bikesharingdl.dfs.core.windows.net/hourly_full_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select the required columns form Date Dimension Table

# COMMAND ----------

date_df = hourly_df.select(col('date_'), col('season'), col('year'), col('month_'), col('week_day'), col('working_day'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Drop duplicate rows

# COMMAND ----------

date_unique_df = date_df.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Enrich season Column

# COMMAND ----------

date_final_df = date_unique_df.withColumn('season',
when(date_unique_df.season == 1, 'spring') \
.when(date_unique_df.season == 2, 'summer') \
.when(date_unique_df.season == 3, 'fall') \
.when(date_unique_df.season == 4, 'winter') \
)

# COMMAND ----------

date_final_df = date_final_df.withColumn('week_day',
when(col('week_day') == 0, 'Sunday') \
.when(col('week_day') == 1, 'Monday') \
.when(col('week_day') == 2, 'Tuesday') \
.when(col('week_day') == 3, 'Wednesday') \
.when(col('week_day') == 4, 'Thrusday') \
.when(col('week_day') == 5, 'Friday') \
.when(col('week_day') == 6, 'Saturday') \
)

# COMMAND ----------

date_final_df = date_final_df.withColumn('working_day',
when(col('working_day') == 0, 'Holiday') \
.when(col('working_day') == 1, 'Working Day')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 -  Write data to delta lake gold containter

# COMMAND ----------

date_final_df.write.mode("overwrite").format("delta").saveAsTable("bike_sharing_gold.dim_date")