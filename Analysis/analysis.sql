-- Databricks notebook source
SHOW DATABASES;

-- COMMAND ----------

SHOW TABLES in bike_sharing_gold

-- COMMAND ----------

select * from bike_sharing_gold.dim_date limit 5

-- COMMAND ----------

select * from bike_sharing_gold.dim_weather

-- COMMAND ----------

select * from bike_sharing_gold.fact_hour

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Check Data Accuracy

-- COMMAND ----------

-- check if total holidays and total working days count match with total days
select 
  count(*) as total_days,
  sum(holiday) as total_holidays, 
  sum(workingday) as total_working_days 
from bike_sharing_gold.dim_date

-- COMMAND ----------

-- Check if total_riders is accurate
select  
  total_riders,
  casual_riders + registeres_riders as actual_total_riders
from bike_sharing_gold.fact_hour

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Data Insights

-- COMMAND ----------

-- Average Riders by Season Order by Hour
select 
  fh.date_,
  dd.season,
  avg(fh.registeres_riders) over(partition by dd.season order by fh.hour_) as total_registeres_riders,
  avg(fh.casual_riders) over(partition by dd.season order by fh.hour_) as total_casual_riders,
  avg(fh.total_riders) over(partition by dd.season order by fh.hour_) as total_riders
from bike_sharing_gold.fact_hour as fh
  left join bike_sharing_gold.dim_date as dd
    on dd.date_ = fh.date_

-- COMMAND ----------

-- Check the riders count by season and working
select 
  dd.year,
  dd.season,
  dd.working_day,
  sum(fh.registeres_riders) as total_registeres_riders,
  sum(fh.casual_riders) as total_casual_riders,
  sum(fh.total_riders) as total_riders
from bike_sharing_gold.fact_hour as fh
  left join bike_sharing_gold.dim_date as dd
    on dd.date_ = fh.date_
group by dd.year, dd.season, dd.working_day
sort by dd.year, dd.working_day

-- COMMAND ----------

-- Check the riders count by weather conditions
select 
  year(date_) as year,
  dw.weather_condition,
  sum(registeres_riders) as total_registeres_riders,
  sum(casual_riders) as total_casual_riders,
  sum(total_riders) as total_riders
from bike_sharing_gold.fact_hour as fh
  left join bike_sharing_gold.dim_weather as dw
    on fh.weather_id = dw.weather_id
group by year(date_), dw.weather_condition
sort by year(date_), dw.weather_condition

-- COMMAND ----------

-- Check the riders count on week end vs week day
with dim_date_new as (
  select 
    date_,
    if( week_day in ('Saturday', 'Sunday') , true, false) as week_end
  from bike_sharing_gold.dim_date
)
select
  ddn.week_end,
  sum(registeres_riders) as total_registeres_riders,
  sum(casual_riders) as total_casual_riders,
  sum(total_riders) as total_riders
from bike_sharing_gold.fact_hour as fh
  left join dim_date_new as ddn
    on ddn.date_ = fh.date_
group by ddn.week_end
