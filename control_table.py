# Databricks notebook source
# %sql
# create schema if not exists sarthak_dev.update

#  %sql 
#  create table if not exists sarthak_dev.update.watermark_table(
#    pipeline_id STRING,
#    last_ingestion_ts TIMESTAMP

from pyspark.sql import functions as F

config_df = spark.table("sarthak_dev.config.bronze_to_silver_config")

if "pipeline_id" not in config_df.columns:
    config_df = config_df.withColumn(
        "pipeline_id",
        F.concat_ws(
            "_to_",
            F.concat_ws(".", F.col("source_catalog"), F.col("source_schema"), F.col("source_table_name")),
            F.concat_ws(".", F.col("target_catalog"), F.col("target_schema"), F.col("target_table_name"))
        )
    )

for row in config_df.collect():

    pipeline_id = row.pipeline_id
    bronze_table_name = f"{row.source_catalog}.{row.source_schema}.{row.source_table_name}"
    silver_table_name = f"{row.target_catalog}.{row.target_schema}.{row.target_table_name}"
    if spark.catalog.tableExists(silver_table_name):
        last_ts = spark.table(silver_table_name).agg(F.max("silver_ingestion_ts")).collect()[0][0]
    else:
        last_ts = None

    if last_ts is None:
        last_ts = "1900-01-01 00:00:00"
    exists = (
        spark.table("sarthak_dev.update.watermark_table")
        .filter(F.col("pipeline_id") == pipeline_id)
        .count() > 0
    )

    if not exists:
        spark.sql(f"""
        INSERT INTO sarthak_dev.update.watermark_table
        VALUES ('{pipeline_id}', '{last_ts}')
        """)

