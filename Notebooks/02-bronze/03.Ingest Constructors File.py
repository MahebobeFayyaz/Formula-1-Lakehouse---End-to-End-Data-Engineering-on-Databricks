# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest constructors.json file
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

# Define source_file and table_name
source_file = f"{landing_folder_path}/{v_batch_id}/constructors.json"
table_name = f"{catalog_name}.{bronze_schema}.constructors"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the JSON file using the dataframe reader API

# COMMAND ----------

# Define the schema
constructors_schema = """constructorId STRING, 
                         name STRING, 
                         nationality STRING, 
                         url STRING
                         """

# COMMAND ----------

# Read data from the constructors file
constructors_df = (
    spark.read
       .format('json')
       .schema(constructors_schema)
       .option('mode', 'FAILFAST')
       .load(source_file)
)

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Add Metadata Columns
# MAGIC - Source File
# MAGIC - Ingestion Timestamp

# COMMAND ----------

constructors_final_df = add_ingestion_metadata(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write to bronze delta table

# COMMAND ----------

write_to_bronze (
    input_df = constructors_final_df,
    target_table = table_name,
    batch_id = v_batch_id
)

# COMMAND ----------

display(spark.table(table_name))