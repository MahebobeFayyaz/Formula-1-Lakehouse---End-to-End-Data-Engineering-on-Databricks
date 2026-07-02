# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest races.csv file
# MAGIC 1. Read the file using spark dataframe reader API
# MAGIC 1. Add Metadata Columns 
# MAGIC     - Source File
# MAGIC     - Ingestion Timestamp
# MAGIC 1. Write to bronze delta table    

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

source_file = f"{landing_folder_path}/{v_batch_id}/races.csv"
table_name = f"{catalog_name}.{bronze_schema}.races"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the CSV file using the dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

races_schema = StructType([
    StructField('season',   IntegerType()),
    StructField("round",    IntegerType()),
    StructField("url",      StringType()),
    StructField("raceName", StringType()),
    StructField("date",     DateType()),
    StructField("circuitId", StringType())
])

# COMMAND ----------

races_df = (
    spark.read
         .format('csv')
         .option('header', 'true')
#         .option('inferSchema', 'true')
         .option('mode', 'FAILFAST')
         .schema(races_schema)
         .load(source_file)
)

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Add Metadata Columns
# MAGIC - Source File
# MAGIC - Ingestion Timestamp

# COMMAND ----------

races_final_df = add_ingestion_metadata(races_df)

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write to bronze delta table

# COMMAND ----------

write_to_bronze (
    input_df = races_final_df,
    target_table = table_name,
    batch_id = v_batch_id
)

# COMMAND ----------

display(spark.table(table_name))