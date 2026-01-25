
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)



config_df = (
    spark.table("sarthak_dev.config.bronze_ingestion_config")
    .filter("is_active = 1")
)

for row in config_df.collect():

    df_raw = (
        spark.read
        .format(row["source_format"])
        .option("header", True)
        .option("inferSchema", True)
        .load(row["source_path"])
        .withColumnRenamed("Unnamed: 0","id")
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("source_file", F.col("_metadata.file_path"))
    )

    (
        df_raw
        .write
        .format("delta")
        .mode(row["write_mode"])
        .option("mergeSchema", "true")
        .saveAsTable(
            f"{row['target_catalog']}.{row['target_schema']}.{row['target_table']}"
        )
    )




