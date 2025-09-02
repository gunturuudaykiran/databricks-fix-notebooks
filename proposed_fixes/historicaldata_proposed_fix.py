# Databricks notebook source
from pyspark.sql.functions import _try_cast, col

original_df=spark.read.table('datatypeschema.datatypeprocessed.sampledata')
display(original_df)

# COMMAND ----------

mismatch_df=spark.read.table('datatypeschema.landingdata.mismatchdatalanding')
display(mismatch_df)

# COMMAND ----------

original_df.printSchema()
mismatch_df.printSchema()

# COMMAND ----------

# Cast 'malformed' column to string to avoid cast errors
mismatch_df = mismatch_df.select([_try_cast(col(c), col(c).dataType) if col(c).dataType.typeName() == 'bigint' else col(c) for c in mismatch_df.columns])

# Append directly (schema mismatch will be handled by Spark as string coercion if needed)
mismatch_df.write.mode("append").insertInto("datatypeschema.datatypeprocessed.sampledata")

# COMMAND ----------

display(original_df)
display(mismatch_df)

# COMMAND ----------

original_df.describe().show()
mismatch_df.describe().show()