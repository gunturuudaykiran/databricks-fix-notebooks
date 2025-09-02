# Databricks notebook source
original_df=spark.read.table('datatypeschema.datatypeprocessed.sampledata')
display(original_df)

# COMMAND ----------

mismatch_df=spark.read.table('datatypeschema.landingdata.mismatchdatalanding')
display(mismatch_df)

# COMMAND ----------

original_df.printSchema()
mismatch_df.printSchema()

# COMMAND ----------

# Cast BIGINT columns to STRING to tolerate malformed input
from pyspark.sql.functions import try_cast
for col in original_df.columns:
    if original_df.schema[col].dataType.typeName() == "bigInt":
        mismatch_df = mismatch_df.withColumn(col, try_cast(mismatch_df[col], "bigint"))

# Append directly (schema mismatch will be handled by Spark as string coercion if needed)
mismatch_df.write.mode("append").insertInto("datatypeschema.datatypeprocessed.sampledata")

# COMMAND ----------

display(original_df)
display(mismatch_df)

# COMMAND ----------

original_df.describe().show()
mismatch_df.describe().show()