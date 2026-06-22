# Databricks notebook source

import re

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
internal_client_input_path = f"/Volumes/{catalog}/{schema}/{volume}/uploads/internal_client_entities.csv"


def normalise_column_name(name: str) -> str:
    """Normalise CSV headers so LEI, 'lei ', 'LEI\ufeff', etc. become 'lei'."""
    cleaned = name.replace("\ufeff", "").strip().lower()
    cleaned = re.sub(r"[^a-z0-9]+", "_", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned


def normalise_headers(df):
    """Return a dataframe with normalised, unique column names."""
    seen = {}
    renamed_cols = []

    for original_name in df.columns:
        base_name = normalise_column_name(original_name)
        if not base_name:
            base_name = "unnamed"

        count = seen.get(base_name, 0)
        seen[base_name] = count + 1

        new_name = base_name if count == 0 else f"{base_name}_{count + 1}"
        renamed_cols.append(col(f"`{original_name}`").alias(new_name))

    return df.select(*renamed_cols)


uploaded_clients = (
    spark.read
    .option("header", True)
    .option("encoding", "UTF-8")
    .csv(client_input_path)
    .withColumn("_source_file", input_file_name())
)

uploaded_clients = normalise_headers(uploaded_clients)

if "client_name" not in uploaded_clients.columns:
    raise ValueError(f"Client file must contain a client_name column. Found columns: {uploaded_clients.columns}")

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

internal_client = (
    spark.read
    .option("header", True)
    .option("encoding", "UTF-8")
    .csv(internal_client_input_path)
)

internal_client = normalise_headers(internal_client)

# Backward-compatible rename if an older template is used.
if "internal_entity_id" not in internal_client.columns and "risk_entity_id" in internal_client.columns:
    internal_client = internal_client.withColumnRenamed("risk_entity_id", "internal_entity_id")

if "internal_rating" not in internal_client.columns and "risk_rating" in internal_client.columns:
    internal_client = internal_client.withColumnRenamed("risk_rating", "internal_rating")

if "legal_name" not in internal_client.columns and "client_name" in internal_client.columns:
    internal_client = internal_client.withColumnRenamed("client_name", "legal_name")

if "legal_name" not in internal_client.columns:
    raise ValueError(f"Internal client file must contain legal_name or client_name. Found columns: {internal_client.columns}")

for optional_col in ["country", "address", "lei", "registration_number", "internal_rating", "business_owner"]:
    if optional_col not in internal_client.columns:
        internal_client = internal_client.withColumn(optional_col, lit(None).cast("string"))

if "internal_entity_id" not in internal_client.columns:
    internal_client = internal_client.withColumn(
        "internal_entity_id",
        concat(lit("I"), lpad(monotonically_increasing_id().cast("string"), 8, "0"))
    )

internal_client = internal_client.select(
    trim(col("internal_entity_id")).alias("internal_entity_id"),
    trim(col("legal_name")).alias("legal_name"),
    trim(col("country")).alias("country"),
    trim(col("address")).alias("address"),
    trim(col("lei")).alias("lei"),
    trim(col("registration_number")).alias("registration_number"),
    trim(col("internal_rating")).alias("internal_rating"),
    trim(col("business_owner")).alias("business_owner"),
)

internal_client.write.mode("overwrite").format("delta").saveAsTable(f"{catalog}.{schema}.internal_client_entities_raw")

display(client)
display(internal_client)
print("Client and internal client files ingested.")
print("Client columns:", uploaded_clients.columns)
print("Internal client columns:", internal_client.columns)
