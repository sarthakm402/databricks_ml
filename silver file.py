# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


silver_config_df=spark.read.table("sarthak_dev.config.bronze_to_silver_config").filter(F.col("is_active")==1)


for row in silver_config_df.collect():
    df=spark.read.table(f'{row.source_catalog}.{row.source_schema}.{row.source_table_name}')
    df = df.withColumn(
    row.event_ts_column,
    F.col(row.event_ts_column).cast("timestamp"))
    df = df.filter(
    F.col("merchant").isNotNull() &
    F.col("category").isNotNull()
    )
    window=Window.partitionBy(row.business_key).orderBy(F.col(row.event_ts_column).desc())
    df = df.withColumn("row_number", row_number().over(window))
    df = df.filter(F.col("row_number")==1)
    df = df.drop("row_number")
    df = (
    df
    .withColumn("silver_ingestion_ts", F.current_timestamp())
    )
    (
    df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        f"{row.target_catalog}.{row.target_schema}.{row.target_table_name}"
    )
)

