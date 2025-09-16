# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM weather_catalog.weather_schema.weather_karnataka LIMIT 10;

# COMMAND ----------

df = spark.read.table('weather_catalog.weather_schema.weather_karnataka')
display(df.limit(10))

# COMMAND ----------

df=spark.sql('''SELECT * FROM weather_catalog.weather_schema.weather_karnataka LIMIT 10''')
display(df)