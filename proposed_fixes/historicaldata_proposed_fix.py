# Databricks notebook source
from pyspark.sql import functions as F

original_df=spark.read.table('datatypeschema.datatypeprocessed.sampledata')
display(original_df)

# COMMAND ----------

mismatch_df=spark.read.table('datatypeschema.landingdata.mismatchdatalanding')
display(mismatch_df)

# COMMAND ----------

original_df.printSchema()
mismatch_df.printSchema()

# COMMAND ----------

# Assuming the problematic column is named 'problematic_column', adjust this name as necessary
mismatch_df = mismatch_df.withColumn("problematic_column", F.try_cast(mismatch_df.problematic_column, 'bigint'))

# Append directly 
mismatch_df.write.mode("append").insertInto("datatypeschema.datatypeprocessed.sampledata")

# COMMAND ----------

display(original_df)
display(mismatch_df)

# COMMAND ----------

original_df.describe().show()
mismatch_df.describe().show()