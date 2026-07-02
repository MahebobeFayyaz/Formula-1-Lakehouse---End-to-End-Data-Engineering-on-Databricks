# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Results Data
# MAGIC 1. Read bronze `results` table
# MAGIC 1. Keep only the columns required for analytics (Drop `url` column)
# MAGIC 1. Standardise column names using snake_case (`constructorId` → `constructor_id`, `driverId` → `driver_id`, `raceName` → `race_name`, `positionText` → `finish_position_text`)
# MAGIC 1. Rename columns to make them more meaningful (`date` → `race_date`, `grid` → `grid_position`, `laps` → `completed_laps`, `number` → `car_number`, `position` → `finish_position`)
# MAGIC 1. Filter out rows where `season`, `round`, `custructor_id` or `driver_id` is null (business key validation)
# MAGIC 1. Remove duplicate records
# MAGIC 1. Transform values of column `race_name` to Title Case
# MAGIC 1. Write the transformed data to silver `results` table

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Entity Relationship Diagram - Formula1 Bronze Schema
# MAGIC
# MAGIC ![Formula1 Raw Data.png](../../z-course-images/formula1-raw-data-erd.png "Formula1 Raw Data.png")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.results"
silver_table = f"{catalog_name}.{silver_schema}.results"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read bronze `results` table

# COMMAND ----------

results_df = spark.table(bronze_table)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Keep only the columns required for analytics (Drop url column)

# COMMAND ----------

results_selected_df = (
  results_df.select("season",
                    "round",
                    "constructorId",
                    "driverId",
                    "date",
                    "raceName",
                    "grid",
                    "laps",
                    "number",
                    "points",
                    "position",
                    "positionText",
                    "status",
                    "ingestion_timestamp",
                    "source_file")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 & 4 - Standardise Column Names
# MAGIC - Standardise column names using snake_case (`constructorId` → `constructor_id`, `driverId` → `driver_id`, `raceName` → `race_name`, `positionText` → `finish_position_text`)
# MAGIC - Rename columns to make them more meaningful (`date` → `race_date`, `grid` → `grid_position`, `laps` → `completed_laps`, `number` → `car_number`, `position` → `finish_position`)

# COMMAND ----------

results_renamed_df = (
    results_selected_df
        .withColumnsRenamed({
            "constructorId": "constructor_id",
            "driverId": "driver_id",
            "raceName": "race_name",
            "date": "race_date",
            "grid": "grid_position",
            "laps": "completed_laps",
            "number": "car_number",
            "position": "final_position",
            "positionText": "final_position_text"
        })
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Filter out rows where `season`, `round`, `custructor_id` or `driver_id` is null (business key validation)

# COMMAND ----------

results_valid_df = (
    results_renamed_df
        .filter(
            F.col("season").isNotNull() &
            F.col("round").isNotNull() &
            F.col("constructor_id").isNotNull() &
            F.col("driver_id").isNotNull() 
        )
)

# COMMAND ----------

display(results_renamed_df.count() - results_valid_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 6 - Remove duplicate records

# COMMAND ----------

results_distinct_df = results_valid_df.dropDuplicates(["season", "round", "constructor_id", "driver_id"])

# COMMAND ----------

display(results_valid_df.count() - results_distinct_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 7 - Transform values of column `race_name` to Title Case

# COMMAND ----------

results_final_df = (
    results_distinct_df
        .withColumn('race_name', F.initcap(F.col("race_name")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 8 - Write the transformed data to silver `results` table

# COMMAND ----------

(
    results_final_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))