from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


def bootstrap_pipeline(spark, row):

    pipeline_id = row.pipeline_id
    bronze_table = f"{row.source_catalog}.{row.source_schema}.{row.source_table_name}"
    silver_table = f"{row.target_catalog}.{row.target_schema}.{row.target_table_name}"
    watermark_table_name = "sarthak_dev.update.watermark_table"

    print(f"[BOOTSTRAP] Starting pipeline: {pipeline_id}")

    # 1️⃣ Validate bronze table exists
    if not spark.catalog.tableExists(bronze_table):
        raise Exception(f"Bronze table does not exist: {bronze_table}")

    # 2️⃣ Read full bronze
    bronze_df = (
        spark.read.table(bronze_table)
        .withColumn("ingestion_ts", F.col("ingestion_ts").cast("timestamp"))
    )

    if bronze_df.limit(1).count() == 0:
        print(f"[BOOTSTRAP] Bronze table empty for {pipeline_id}")
        return

    # 3️⃣ Deduplicate latest per business key
    bronze_latest = (
        bronze_df
        .withColumn(
            "row_number",
            row_number().over(
                Window.partitionBy(row.business_key)
                .orderBy(F.col(row.event_ts_column).desc())
            )
        )
        .filter(F.col("row_number") == 1)
        .drop("row_number")
        .withColumn("silver_ingestion_ts", F.current_timestamp())
    )

    # 4️⃣ Create silver table (overwrite safe for bootstrap)
    bronze_latest.write.format("delta").mode("overwrite").saveAsTable(silver_table)

    # 5️⃣ Compute watermark
    max_ts = bronze_df.agg(F.max("ingestion_ts").alias("max_ts")).first()["max_ts"]

    # 6️⃣ Upsert watermark safely using Delta API
    watermark_df = spark.createDataFrame(
        [(pipeline_id, max_ts)],
        ["pipeline_id", "last_ingestion_ts"]
    )

    watermark_table = DeltaTable.forName(spark, watermark_table_name)

    watermark_table.alias("m").merge(
        watermark_df.alias("b"),
        "m.pipeline_id = b.pipeline_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()

    print(f"[BOOTSTRAP] Completed pipeline: {pipeline_id}")
