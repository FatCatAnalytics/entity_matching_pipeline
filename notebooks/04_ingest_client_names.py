# Databricks notebook source

from pyspark.sql.functions import (
    current_timestamp,
    col,
    lit,
    monotonically_increasing_id,
    concat,
    lpad,
    trim,
    input_file_name,
)

catalog = "gleif_project"
schema = "gleif_db"
volume = "gleif_vl"

client_input_path = f"/Volumes/{catalog}/{schema}/{volume}/uploads/incoming/client_entities.csv"
internal_input_path = f"/Volumes/{catalog}/{schema}/{volume}/uploads/internal_risk_entities.csv"

uploaded_clients = (
    spark.read
    .option("header", True)
    .csv(client_input_path)
    .withColumn("_source_file", input_file_name())
)

if "client_name" not in uploaded_clients.columns:
    raise ValueError("Client file must contain a client_name column.")

for optional_col in ["country", "registration_number", "lei", "address"]:
    if optional_col not in uploaded_clients.columns:
        uploaded_clients = uploaded_clients.withColumn(optional_col, lit(None).cast("string"))

client = (
    uploaded_clients
    .select(
        trim(col("client_name")).alias("submitted_name"),
        trim(col("country")).alias("country"),
        trim(col("registration_number")).alias("registration_number"),
        trim(col("lei")).alias("lei"),
        trim(col("address")).alias("address"),
        col("_source_file")
    )
    .filter(col("submitted_name").isNotNull())
    .filter(col("submitted_name") != "")
    .dropDuplicates(["submitted_name", "country", "registration_number", "lei", "address"])
    .withColumn("client_entity_id", concat(lit("C"), lpad(monotonically_increasing_id().cast("string"), 8, "0")))
    .withColumn("client_source", col("_source_file"))
    .withColumn("ingestion_timestamp", current_timestamp())
    .select(
        "client_entity_id",
        "submitted_name",
        "country",
        "registration_number",
        "lei",
        "address",
        "client_source",
        "ingestion_timestamp",
    )
)

client.write.mode("overwrite").format("delta").saveAsTable(f"{catalog}.{schema}.client_entities_raw")

internal = spark.read.option("header", True).csv(internal_input_path)
internal.write.mode("overwrite").format("delta").saveAsTable(f"{catalog}.{schema}.internal_risk_entities_raw")

display(client)
print("Client and internal risk files ingested.")
