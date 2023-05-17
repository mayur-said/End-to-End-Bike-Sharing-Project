-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS bike_sharing_silver
LOCATION "abfss://silver@bikesharingdl.dfs.core.windows.net/"

-- COMMAND ----------

DESC DATABASE EXTENDED bike_sharing_silver