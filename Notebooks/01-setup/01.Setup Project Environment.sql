-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Set-up the project environment for Formula1 Project
-- MAGIC 1. Create External Location databricks-course-ext-dl1-formula1-incr
-- MAGIC 1. Create Catalog formula1_incr
-- MAGIC 1. Create Schemas landing, bronze, silver and gold
-- MAGIC 1. Create Volume Files in the landing schema

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Access Cloud Storage

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://formula1-incr@databrickscourseextdl1.dfs.core.windows.net/landing'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create External Location

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS databricks_course_ext_dl1_formula1_incr
URL 'abfss://formula1-incr@databrickscourseextdl1.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `databricks-course-sc`)
COMMENT 'External location for the formula1-incr container';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Catalog formula1

-- COMMAND ----------

SHOW CATALOGS;

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS formula1_incr
   MANAGED LOCATION 'abfss://formula1-incr@databrickscourseextdl1.dfs.core.windows.net/' 
   COMMENT 'This is the main catalog for the formula1 project' ;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Schemas landing, bronze, silver, gold

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS formula1_incr.landing;
CREATE SCHEMA IF NOT EXISTS formula1_incr.bronze
    MANAGED LOCATION 'abfss://formula1-incr@databrickscourseextdl1.dfs.core.windows.net/bronze';
CREATE SCHEMA IF NOT EXISTS formula1_incr.silver
    MANAGED LOCATION 'abfss://formula1-incr@databrickscourseextdl1.dfs.core.windows.net/silver';
CREATE SCHEMA IF NOT EXISTS formula1_incr.gold
    MANAGED LOCATION 'abfss://formula1-incr@databrickscourseextdl1.dfs.core.windows.net/gold';         

-- COMMAND ----------

SELECT current_catalog();

-- COMMAND ----------

USE CATALOG formula1_incr;

-- COMMAND ----------

SHOW SCHEMAS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Volume Files

-- COMMAND ----------

CREATE EXTERNAL VOLUME formula1_incr.landing.files
LOCATION 'abfss://formula1-incr@databrickscourseextdl1.dfs.core.windows.net/landing';

-- COMMAND ----------

-- MAGIC %fs ls /Volumes/formula1_incr/landing/files