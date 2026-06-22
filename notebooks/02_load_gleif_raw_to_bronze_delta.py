# Databricks notebook source

from pyspark.sql.functions import current_timestamp, input_file_name

catalog = "gleif_project"
schema = "gleif_db"
volume = "gleif_vl"

lei_path = f"/Volumes/{catalog}/{schema}/{volume}/raw_gleif/lei_cdf/"
rr_path = f"/Volumes/{catalog}/{schema}/{volume}/raw_gleif/rr_cdf/"

lei_df = (
    spark.read
    .option("header", True)
    .option("multiLine", True)
    .option("escape", '"')
    .option("quote", '"')
    .csv(lei_path)
    .withColumn("_source_file", input_file_name())
    .withColumn("_bronze_loaded_at", current_timestamp())
)

rr_df = (
    spark.read
    .option("header", True)
    .option("multiLine", True)
    .option("escape", '"')
    .option("quote", '"')
    .csv(rr_path)
    .withColumn("_source_file", input_file_name())
    .withColumn("_bronze_loaded_at", current_timestamp())
)

lei_df.write.mode("overwrite").format("delta").saveAsTable(f"{catalog}.{schema}.bronze_gleif_lei_cdf")
rr_df.write.mode("overwrite").format("delta").saveAsTable(f"{catalog}.{schema}.bronze_gleif_rr_cdf")

spark.sql(f"ALTER TABLE {catalog}.{schema}.bronze_gleif_lei_cdf SET TBLPROPERTIES ('data_layer'='bronze','source_system'='GLEIF','dataset'='LEI-CDF')")
spark.sql(f"ALTER TABLE {catalog}.{schema}.bronze_gleif_rr_cdf SET TBLPROPERTIES ('data_layer'='bronze','source_system'='GLEIF','dataset'='RR-CDF')")

print("Bronze GLEIF Delta tables created.")
