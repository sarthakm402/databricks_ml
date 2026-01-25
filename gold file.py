# Databricks notebook source
from pyspark.sql.functions import col, expr, current_timestamp
import json

config_df = (
    spark.read.table("sarthak_dev.config.silver_to_gold_config")
    .filter(col("is_active") == 1)
)


import ast
for row in config_df.collect():
    df = spark.read.table(
        f"{row.source_catalog}.{row.source_schema}.{row.source_table_name}"
    )
    group_by_cols = ast.literal_eval(row.group_by_columns)
    metrics_dict = json.loads(row.metrics.replace("'", '"'))
    agg_expressions = [
        expr(sql_expr).alias(col_name)
        for col_name, sql_expr in metrics_dict.items()
    ]
    gold_df = (
        df.groupBy(*group_by_cols)
          .agg(*agg_expressions)
    )
    gold_df = gold_df.withColumn(
        "gold_ingestion_ts",
        current_timestamp()
    )
    (
        gold_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(
            f"{row.target_catalog}.{row.target_schema}.{row.target_table_name}"
        )
    )
