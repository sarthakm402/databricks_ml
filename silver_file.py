from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

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
    silver_table_name = f"{row.target_catalog}.{row.target_schema}.{row.target_table_name}"
    bronze_table_name = f"{row.source_catalog}.{row.source_schema}.{row.source_table_name}"

    last_ts = (
        spark.table("sarthak_dev.update.watermark_table")
        .filter(F.col("pipeline_id") == pipeline_id)
        .select(F.max("last_ingestion_ts"))
        .collect()[0][0]
    )

    if last_ts is None:
        last_ts = "1900-01-01 00:00:00"
    bronze_df = (
        spark.read.table(bronze_table_name)
        .withColumn("ingestion_ts", F.col("ingestion_ts").cast("timestamp"))
        .filter(F.col("ingestion_ts") > F.to_timestamp(F.lit(last_ts)))
    )
    if not spark.catalog.tableExists(silver_table_name):
        bronze_sample = spark.read.table(bronze_table_name).limit(1)
        bronze_schema = bronze_sample.schema
        silver_schema = bronze_schema.add("silver_ingestion_ts", "timestamp", True)
        spark.createDataFrame([], silver_schema).write.format("delta").saveAsTable(silver_table_name)

    if bronze_df.limit(1).count() == 0:
        continue

    bronze_df = bronze_df.filter(
        F.col("merchant").isNotNull() &
        F.col("category").isNotNull() &
        F.col("id").isNotNull()
    )

    bronze_latest = (
        bronze_df
        .withColumn("row_number", row_number().over(
            Window.partitionBy(row.business_key)
                  .orderBy(F.col(row.event_ts_column).desc())
        ))
        .filter(F.col("row_number") == 1)
        .drop("row_number")
        .withColumn("silver_ingestion_ts", F.current_timestamp())
    )

    silver_table = DeltaTable.forName(spark, silver_table_name)
    silver_table.alias("s").merge(
        bronze_latest.alias("b"),
        f"s.{row.business_key} = b.{row.business_key}"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    new_last_ts = bronze_df.agg(F.max("ingestion_ts")).collect()[0][0]

    spark.sql(f"""
    MERGE INTO sarthak_dev.update.watermark_table m
    USING (SELECT '{pipeline_id}' AS pipeline_id, '{new_last_ts}' AS last_ingestion_ts) b
    ON m.pipeline_id = b.pipeline_id
    WHEN MATCHED THEN UPDATE SET m.last_ingestion_ts = b.last_ingestion_ts
    WHEN NOT MATCHED THEN INSERT (pipeline_id, last_ingestion_ts) VALUES (b.pipeline_id, b.last_ingestion_ts)
    """)
