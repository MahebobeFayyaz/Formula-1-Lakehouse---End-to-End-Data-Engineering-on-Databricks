# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

def write_to_silver(
    input_df,
    target_table,
    merge_condition,
    columns_to_update
):
    """
    Creates the Delta table if it does not exist.
    Otherwise merges the input DataFrame into the target table.
    """

    final_df = (
        input_df
        .withColumn("created_timestamp", F.current_timestamp())
        .withColumn("updated_timestamp", F.current_timestamp())
    )

    if not spark.catalog.tableExists(target_table):
        (
            final_df.write
                .format("delta")
                .mode("overwrite")
                .saveAsTable(target_table)
        )
    else:
        delta_table = DeltaTable.forName(spark, target_table)
        update_map = {column: f"s.{column}" for column in columns_to_update}
        update_map["updated_timestamp"] = "s.updated_timestamp"

        (
            delta_table.alias("t")
            .merge(
                final_df.alias("s"),
                merge_condition
            )
            .whenMatchedUpdate(
                condition="s.batch_id >= t.batch_id",
                set=update_map
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

