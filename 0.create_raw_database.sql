-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS bike_sharing_broze

-- COMMAND ----------

DROP TABLE IF EXISTS bike_sharing_broze.hourly_raw_data;
CREATE TABLE IF NOT EXISTS bike_sharing_broze.hourly_raw_data
(instant INT,
dteday DATE,
season INT,
yr INT,
mnth INT,
hr INT,
holiday INT,
weekday INT,
workingday INT,
weathersit INT,
temp DOUBLE,
atemp DOUBLE,
hum DOUBLE,
windspeed DOUBLE,
casual INT,
registered INT,
cnt INT)
USING csv
LOCATION "abfss://bronze@bikesharingdl.dfs.core.windows.net/hour.csv"

-- COMMAND ----------

select * from bike_sharing_broze.hourly_raw_data