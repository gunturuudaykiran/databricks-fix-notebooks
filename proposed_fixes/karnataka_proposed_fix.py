# Databricks notebook source
# MAGIC %sql
# MAGIC select * from weather_catalog.weather_schema.weather_karnataka;

# COMMAND ----------

df=spark.read.table('weather_catalog.weather_schema.weather_karnataka')
display(df.limit(10))