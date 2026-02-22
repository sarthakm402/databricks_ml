from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from silver_file_creation import bootstrap_pipeline

WATERMARK_TABLE = "sarthak_dev.update.watermark_table"
CONFIG_TABLE = "sarthak_dev.config.bronze_to_silver_config"


def get_watermark_map(spark):
    if not spark.catalog.tableExists(WATERMARK_TABLE):
        return {}

    return {
        row["pipeline_id"]: row["last_ingestion_ts"]
        for row in spark.table(WATERMARK_TABLE).collect()
    }


def update_watermark(spark, pipeline_id, new_ts):
    watermark_df = spark.createDataFrame(
        [(pipeline_id, new_ts)],
        ["pipeline_id", "last_ingestion_ts"]
    )

    DeltaTable.forName(spark, WATERMARK_TABLE) \
        .alias("m") \
        .merge(
            watermark_df.alias("b"),
            "m.pipeline_id = b.pipeline_id"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()


def run_incremental_pipeline(spark, row, last_ts):

    bronze_table = f"{row.source_catalog}.{row.source_schema}.{row.source_table_name}"
    silver_table = f"{row.target_catalog}.{row.target_schema}.{row.target_table_name}"

    bronze_df = (
        spark.read.table(bronze_table)
        .withColumn("ingestion_ts", F.col("ingestion_ts").cast("timestamp"))
        .filter(F.col("ingestion_ts") > F.lit(last_ts))
    )

    if bronze_df.rdd.isEmpty():
        return

    bronze_df = bronze_df.filter(
        F.col("merchant").isNotNull() &
        F.col("category").isNotNull() &
        F.col("id").isNotNull()
    )

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

    DeltaTable.forName(spark, silver_table) \
        .alias("s") \
        .merge(
            bronze_latest.alias("b"),
            f"s.{row.business_key} = b.{row.business_key}"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

    new_last_ts = bronze_df.agg(F.max("ingestion_ts")).first()[0]
    update_watermark(spark, row.pipeline_id, new_last_ts)


def main(spark):

    config_df = spark.table(CONFIG_TABLE)

    if "pipeline_id" not in config_df.columns:
        config_df = config_df.withColumn(
            "pipeline_id",
            F.concat_ws(
                "_to_",
                F.concat_ws(".", "source_catalog", "source_schema", "source_table_name"),
                F.concat_ws(".", "target_catalog", "target_schema", "target_table_name")
            )
        )

    watermark_map = get_watermark_map(spark)

    for row in config_df.collect():

        pipeline_id = row.pipeline_id

        if pipeline_id not in watermark_map:
            bootstrap_pipeline(spark, row)
            continue

        last_ts = watermark_map[pipeline_id]

        if last_ts is None:
            last_ts = "1900-01-01 00:00:00"

        run_incremental_pipeline(spark, row, last_ts)