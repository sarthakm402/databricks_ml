from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def bootstrap_pipeline(spark, row):

    pipeline_id = row.pipeline_id
    bronze_table = f"{row.source_catalog}.{row.source_schema}.{row.source_table_name}"
    silver_table = f"{row.target_catalog}.{row.target_schema}.{row.target_table_name}"
    watermark_table = "sarthak_dev.update.watermark_table"

    bronze_df = (
        spark.read.table(bronze_table)
        .withColumn("ingestion_ts", F.col("ingestion_ts").cast("timestamp"))
    )

    if bronze_df.rdd.isEmpty():
        return

    bronze_latest = (
        bronze_df
        .withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy(row.business_key)
                .orderBy(F.col(row.event_ts_column).desc())
            )
        )
        .filter(F.col("rn") == 1)
        .drop("rn")
        .withColumn("silver_ingestion_ts", F.current_timestamp())
    )

    bronze_latest.write.format("delta").mode("overwrite").saveAsTable(silver_table)

    max_ts = bronze_df.agg(F.max("ingestion_ts")).first()[0]

    watermark_df = spark.createDataFrame(
        [(pipeline_id, max_ts)],
        ["pipeline_id", "last_ingestion_ts"]
    )

    if not spark.catalog.tableExists(watermark_table):
        watermark_df.write.format("delta").saveAsTable(watermark_table)
    else:
        DeltaTable.forName(spark, watermark_table) \
            .alias("m") \
            .merge(
                watermark_df.alias("b"),
                "m.pipeline_id = b.pipeline_id"
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()