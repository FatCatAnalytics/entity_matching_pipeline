# Databricks notebook source

from pyspark.sql.functions import col

catalog = "gleif_project"
schema = "gleif_db"

client = spark.table(f"{catalog}.{schema}.client_entities_clean").alias("c")
ref = spark.table(f"{catalog}.{schema}.reference_entities_clean").alias("r")

select_expr = [
    "c.client_entity_id",
    "c.submitted_name",
    "c.clean_name as client_clean_name",
    "c.country_norm as client_country",
    "c.registration_number_norm as client_registration_number",
    "c.lei_norm as client_lei",
    "c.address_norm as client_address",
    "r.reference_id",
    "r.legal_name as candidate_name",
    "r.clean_name as candidate_clean_name",
    "r.country_norm as candidate_country",
    "r.registration_number_norm as candidate_registration_number",
    "r.lei_norm as candidate_lei",
    "r.address_norm as candidate_address",
    "r.source as candidate_source",
]

lei_candidates = (
    client
    .filter(col("c.lei_norm").isNotNull())
    .filter(col("c.lei_norm") != "")
    .join(ref, col("c.lei_norm") == col("r.lei_norm"), "inner")
    .selectExpr(*select_expr, "'EXACT_LEI' as candidate_method")
)

registration_candidates = (
    client
    .filter(col("c.registration_number_norm").isNotNull())
    .filter(col("c.registration_number_norm") != "")
    .join(ref, col("c.registration_number_norm") == col("r.registration_number_norm"), "inner")
    .selectExpr(*select_expr, "'REGISTRATION_NUMBER' as candidate_method")
)

name_country_candidates = (
    client
    .filter(col("c.country_norm").isNotNull())
    .filter(col("c.country_norm") != "")
    .join(
        ref,
        (
            (col("c.country_norm") == col("r.country_norm"))
            & (
                (col("c.first_token") == col("r.first_token"))
                | (col("c.name_prefix_4") == col("r.name_prefix_4"))
            )
        ),
        "inner"
    )
    .selectExpr(*select_expr, "'NAME_COUNTRY_BLOCK' as candidate_method")
)

name_only_candidates = (
    client
    .join(
        ref,
        (
            (col("c.first_token") == col("r.first_token"))
            | (col("c.name_prefix_4") == col("r.name_prefix_4"))
        ),
        "inner"
    )
    .selectExpr(*select_expr, "'NAME_ONLY_BLOCK' as candidate_method")
)

candidates = (
    lei_candidates
    .unionByName(registration_candidates)
    .unionByName(name_country_candidates)
    .unionByName(name_only_candidates)
    .dropDuplicates(["client_entity_id", "reference_id", "candidate_method"])
)

candidates.write.mode("overwrite").format("delta").saveAsTable(f"{catalog}.{schema}.match_candidates")

print("Candidate records created.")
display(candidates.limit(100))
