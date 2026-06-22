# Databricks notebook source

from pyspark.sql.functions import lower, regexp_replace, trim, col, concat_ws, substring, split, upper, lit

catalog = "gleif_project"
schema = "gleif_db"

def clean_name_expr(column_name):
    x = lower(col(column_name))
    x = regexp_replace(x, "&", " and ")
    x = regexp_replace(x, "[^a-z0-9\\s]", " ")
    x = regexp_replace(
        x,
        "\\b(inc|incorporated|ltd|limited|llc|plc|corp|corporation|co|company|ag|sa|bv|nv|gmbh|pte|pty|holdings|holding|group)\\b",
        " "
    )
    x = regexp_replace(x, "\\s+", " ")
    return trim(x)

client = (
    spark.table(f"{catalog}.{schema}.client_entities_raw")
    .withColumn("clean_name", clean_name_expr("submitted_name"))
    .withColumn("country_norm", upper(trim(col("country"))))
    .withColumn("registration_number_norm", upper(trim(col("registration_number"))))
    .withColumn("lei_norm", upper(trim(col("lei"))))
    .withColumn("address_norm", lower(trim(col("address"))))
    .withColumn("first_token", split(col("clean_name"), " ").getItem(0))
    .withColumn("name_prefix_4", substring(col("clean_name"), 1, 4))
    .withColumn("blocking_key", concat_ws("|", col("country_norm"), col("name_prefix_4")))
)

internal_client_raw = spark.table(f"{catalog}.{schema}.internal_client_entities_raw")

if "registration_number" not in internal_client_raw.columns:
    internal_client_raw = internal_client_raw.withColumn("registration_number", lit(None).cast("string"))

internal_client = (
    internal_client_raw
    .selectExpr(
        "internal_entity_id",
        "legal_name",
        "country",
        "address",
        "lei",
        "registration_number",
        "internal_rating",
        "business_owner"
    )
    .withColumn("clean_name", clean_name_expr("legal_name"))
    .withColumn("country_norm", upper(trim(col("country"))))
    .withColumn("registration_number_norm", upper(trim(col("registration_number"))))
    .withColumn("lei_norm", upper(trim(col("lei"))))
    .withColumn("address_norm", lower(trim(col("address"))))
    .withColumn("first_token", split(col("clean_name"), " ").getItem(0))
    .withColumn("name_prefix_4", substring(col("clean_name"), 1, 4))
    .withColumn("blocking_key", concat_ws("|", col("country_norm"), col("name_prefix_4")))
)

gleif_reference = (
    spark.table(f"{catalog}.{schema}.gleif_entities_raw")
    .selectExpr(
        "lei as reference_id",
        "legal_name",
        "country",
        "address",
        "lei",
        "registration_number",
        "'GLEIF' as source"
    )
    .withColumn("clean_name", clean_name_expr("legal_name"))
    .withColumn("country_norm", upper(trim(col("country"))))
    .withColumn("registration_number_norm", upper(trim(col("registration_number"))))
    .withColumn("lei_norm", upper(trim(col("lei"))))
    .withColumn("address_norm", lower(trim(col("address"))))
    .withColumn("first_token", split(col("clean_name"), " ").getItem(0))
    .withColumn("name_prefix_4", substring(col("clean_name"), 1, 4))
    .withColumn("blocking_key", concat_ws("|", col("country_norm"), col("name_prefix_4")))
)

client.write.mode("overwrite").format("delta").saveAsTable(f"{catalog}.{schema}.client_entities_clean")

# Keep the internal client/entity master separate. It is used later in notebook 09.
internal_client.write.mode("overwrite").format("delta").saveAsTable(f"{catalog}.{schema}.internal_client_entities_clean")

# Reference candidates for the first-stage match should be GLEIF only.
# Do not union internal client rows here, otherwise the pipeline may match to internal rows
# without LEIs and fail to enrich hierarchy/direct/ultimate parent relationships.
gleif_reference.write.mode("overwrite").format("delta").saveAsTable(f"{catalog}.{schema}.reference_entities_clean")

print("Clean client, internal client, and GLEIF reference entities created.")
