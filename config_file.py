# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)



spark.sql("create schema if not exists sarthak_dev.config")


schemas=StructType(
    [
        StructField("table_name", StringType(), True),
        StructField("source_format", StringType(), True),
        StructField("source_path", StringType(), True),
        StructField("target_catalog", StringType(), True),
        StructField("target_schema", StringType(), True),
        StructField("target_table", StringType(), True),
        StructField("write_mode", StringType(), True),
        StructField("is_active", IntegerType(), True)
    ]
)



config=spark.createDataFrame([],schemas)



config.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("sarthak_dev.config.bronze_ingestion_config")

#  Updating the config file


config_data = [
    {
        "table_name": "fraud_train",
        "source_format": "csv",
        "source_path":"/Volumes/sarthak_dev/default/fraud_raw/fraudTrain_raw_corrupted_v1.csv",
        "target_catalog": "sarthak_dev",
        "target_schema": "bronze",
        "target_table": "fraud_train",
        "write_mode": "append",
        "is_active": 1
    }
]


config_schema = StructType([
    StructField("table_name", StringType(), True),
    StructField("source_format", StringType(), True),
    StructField("source_path", StringType(), True),
    StructField("target_catalog", StringType(), True),
    StructField("target_schema", StringType(), True),
    StructField("target_table", StringType(),True),
    StructField("write_mode", StringType(), True),
    StructField("is_active", IntegerType(), True)
])


config_df = spark.createDataFrame(config_data, schema=config_schema)

(
    config_df
    .write
    .format("delta")
    .mode("append").option("mergeSchema", "true")
    .saveAsTable("sarthak_dev.config.bronze_ingestion_config")
)

#  bronze to silver conifg


import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)

schema=StructType(
    [   StructField("pipeline_id", IntegerType(), False),
        StructField("source_table_name", StringType(), True),
        StructField("source_catalog", StringType(), True),
        StructField("source_schema", StringType(), True),
        StructField("target_catalog", StringType(), True),
        StructField("target_schema", StringType(), True),
        StructField("target_table_name", StringType(), True),
        StructField("business_key",StringType(),True),
        StructField("dedupe_strategy", StringType(), True),
        StructField("event_ts_column", StringType(), True),
        StructField("scd_type", StringType(), False), 
        StructField("is_active", IntegerType(), True)
    ]
)

bronze_to_silver_config=spark.createDataFrame([],schema)
bronze_to_silver_config.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("sarthak_dev.config.bronze_to_silver_config")

# Updating the config


silver_config_data=[
    {
        "pipeline_id": "1",
        "source_table_name": "fraud_train",
        "source_catalog": "sarthak_dev",
        "source_schema": "bronze",
        "target_catalog": "sarthak_dev",
        "target_schema": "silver",
        "target_table_name": "fraud_train_silver",
        "business_key": "id",
        "dedupe_strategy": "latest_event_ts",
        "event_ts_column": "event_ts",
        "scd_type": "scd_type_2",
        "is_active": 1
    }
]

silver_config_data=[
    {
        "pipeline_id": 1,  
        "source_table_name": "fraud_train",
        "source_catalog": "sarthak_dev",
        "source_schema": "bronze",
        "target_catalog": "sarthak_dev",
        "target_schema": "silver",
        "target_table_name": "fraud_train_silver",
        "business_key": "trans_num",
        "dedupe_strategy": "latest_event_ts",
        "event_ts_column": "event_ts",
        "scd_type": "scd_type_1",
        "is_active": 1
    }
]
silver_schema=StructType(
    [   StructField("pipeline_id", IntegerType(), False),
        StructField("source_table_name", StringType(), True),
        StructField("source_catalog", StringType(), True),
        StructField("source_schema", StringType(), True),
        StructField("target_catalog", StringType(), True),
        StructField("target_schema", StringType(), True),
        StructField("target_table_name", StringType(), True),
        StructField("business_key",StringType(),True),
        StructField("dedupe_strategy", StringType(), True),
        StructField("event_ts_column", StringType(), True),
        StructField("scd_type", StringType(), False), 
        StructField("is_active", IntegerType(), True)
    ])
silver_config_df=spark.createDataFrame(silver_config_data,silver_schema)
silver_config_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("sarthak_dev.config.bronze_to_silver_config")

silver_to_gold_schema=StructType(
    [StructField("pipeline_id", IntegerType(), False),
    StructField("source_table_name", StringType(), True),
    StructField("source_catalog", StringType(), True),
    StructField("source_schema", StringType(), True),
    StructField("target_catalog", StringType(), True),
    StructField("target_schema", StringType(), True),
    StructField("target_table_name", StringType(), True),
    StructField("group_by_columns", StringType(), False),
    StructField("metrics", StringType(), False),
    StructField("is_active", IntegerType(), False)

    ]
)

silver_to_gold_config=spark.createDataFrame([],silver_to_gold_schema)
silver_to_gold_config.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("sarthak_dev.config.silver_to_gold_config")
silver_to_gold_config_data=[
    {
  "pipeline_id": 1,
  "source_catalog": "sarthak_dev",
  "source_schema": "silver",
  "source_table_name": "fraud_train_silver",
  "target_catalog": "sarthak_dev",
  "target_schema": "gold",
  "target_table_name": "fraud_metrics",
  "group_by_columns": ["merchant", "category"],
  "metrics": {
    "total_txn_count": "count(*)",
    "fraud_txn_count": "sum(is_fraud)",
    "total_amount": "sum(amt)",
    "fraud_amount": "sum(case when is_fraud = 1 then amt else 0 end)"
  },
  "is_active": 1
}
]
silver_to_gold_config=spark.createDataFrame(silver_to_gold_config_data,silver_to_gold_schema)
silver_to_gold_config.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("sarthak_dev.config.silver_to_gold_config")