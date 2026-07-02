# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest circuits.csv file
# MAGIC 1. Read the file using spark dataframe reader API
# MAGIC 1. Add Metadata Columns 
# MAGIC     - Source File
# MAGIC     - Ingestion Timestamp
# MAGIC 1. Write to bronze delta table    

# COMMAND ----------

# MAGIC %md
# MAGIC ![Incremental Data Processing](../../z-course-images/formula1-incremental-data-processing.png "Incremental Data Processing")

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

source_file = f"{landing_folder_path}/{v_batch_id}/circuits.csv"
table_name = f"{catalog_name}.{bronze_schema}.circuits"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the CSV file using the dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

circuits_schema = StructType([
    StructField('circuitId',   StringType()),
    StructField("url",         StringType()),
    StructField("circuitName", StringType()),
    StructField("lat",         DoubleType()),
    StructField("long",        DoubleType()),
    StructField("locality",    StringType()),
    StructField("country",     StringType())
])

# COMMAND ----------

circuits_df = (
    spark.read
         .format('csv')
         .option('header', 'true')
#         .option('inferSchema', 'true')
         .option('mode', 'FAILFAST')
         .schema(circuits_schema)
         .load(source_file)
)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Add Metadata Columns
# MAGIC - Source File
# MAGIC - Ingestion Timestamp

# COMMAND ----------

circuits_final_df = add_ingestion_metadata(circuits_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write to bronze delta table

# COMMAND ----------

# circuits_final_df = circuits_final_df.withColumn("batch_id", F.lit(v_batch_id))

# COMMAND ----------


# (
#     circuits_final_df
#         .write
#         .format('delta')
#         .mode('overwrite')
#         .partitionBy('batch_id')
#         .option('replaceWhere', f"batch_id = '{v_batch_id}'")
#         .saveAsTable(table_name)
# )

# COMMAND ----------

write_to_bronze (
    input_df = circuits_final_df,
    target_table = table_name,
    batch_id = v_batch_id
)

# COMMAND ----------

display(spark.table(table_name))