# Databricks notebook source

from pyspark.sql.functions import col, concat_ws, lit

catalog = "gleif_project"
schema = "gleif_db"

lei_raw = spark.table(f"{catalog}.{schema}.bronze_gleif_lei_cdf")
rr_raw = spark.table(f"{catalog}.{schema}.bronze_gleif_rr_cdf")

def col_if_exists(df, name, alias_name=None):
    alias_name = alias_name or name
    if name in df.columns:
        return col(f"`{name}`").alias(alias_name)
    return lit(None).cast("string").alias(alias_name)

def date_col_if_exists(df, name, alias_name):
    if name in df.columns:
        return col(f"`{name}`").cast("date").alias(alias_name)
    return lit(None).cast("date").alias(alias_name)

silver_entities = (
    lei_raw
    .select(
        col("LEI").alias("lei"),
        col("`Entity.LegalName`").alias("legal_name"),
        col_if_exists(lei_raw, "Entity.LegalName.xmllang", "legal_name_language"),
        col_if_exists(lei_raw, "Entity.LegalAddress.Country", "country"),
        col_if_exists(lei_raw, "Entity.HeadquartersAddress.Country", "hq_country"),
        col_if_exists(lei_raw, "Entity.LegalJurisdiction", "legal_jurisdiction"),
        col_if_exists(lei_raw, "Entity.RegistrationAuthority.RegistrationAuthorityID", "registration_authority_id"),
        col_if_exists(lei_raw, "Entity.RegistrationAuthority.RegistrationAuthorityEntityID", "registration_number"),
        col_if_exists(lei_raw, "Entity.LegalForm.EntityLegalFormCode", "legal_form"),
        col_if_exists(lei_raw, "Entity.EntityStatus", "entity_status"),
        col_if_exists(lei_raw, "Registration.RegistrationStatus", "registration_status"),
        concat_ws(
            " ",
            col_if_exists(lei_raw, "Entity.LegalAddress.FirstAddressLine", "a1"),
            col_if_exists(lei_raw, "Entity.LegalAddress.AdditionalAddressLine.1", "a2"),
            col_if_exists(lei_raw, "Entity.LegalAddress.City", "city"),
            col_if_exists(lei_raw, "Entity.LegalAddress.Region", "region"),
            col_if_exists(lei_raw, "Entity.LegalAddress.PostalCode", "postcode"),
            col_if_exists(lei_raw, "Entity.LegalAddress.Country", "country2")
        ).alias("legal_address"),
        col("_source_file"),
        col("_bronze_loaded_at")
    )
    .filter(col("lei").isNotNull())
    .dropDuplicates(["lei"])
)

silver_entities.write.mode("overwrite").format("delta").saveAsTable(f"{catalog}.{schema}.silver_gleif_entities")

silver_entities.select(
    col("lei"),
    col("legal_name"),
    col("country"),
    col("legal_jurisdiction").alias("jurisdiction"),
    col("registration_number"),
    col("entity_status"),
    col("registration_status"),
    col("legal_form"),
    col("legal_address").alias("address")
).write.mode("overwrite").format("delta").saveAsTable(f"{catalog}.{schema}.gleif_entities_raw")

silver_relationships = (
    rr_raw
    .select(
        col("`Relationship.StartNode.NodeID`").alias("child_lei"),
        col("`Relationship.EndNode.NodeID`").alias("parent_lei"),
        col("`Relationship.RelationshipType`").alias("relationship_type"),
        col("`Relationship.RelationshipStatus`").alias("relationship_status"),
        date_col_if_exists(rr_raw, "Relationship.Period.1.startDate", "period_start"),
        date_col_if_exists(rr_raw, "Relationship.Period.1.endDate", "period_end"),
        col("_source_file"),
        col("_bronze_loaded_at")
    )
    .filter(col("child_lei").isNotNull())
    .filter(col("parent_lei").isNotNull())
    .dropDuplicates(["child_lei", "parent_lei", "relationship_type"])
)

silver_relationships.write.mode("overwrite").format("delta").saveAsTable(f"{catalog}.{schema}.silver_gleif_relationships")

silver_relationships.select(
    "child_lei",
    "parent_lei",
    "relationship_type",
    "relationship_status",
    "period_start",
    "period_end"
).write.mode("overwrite").format("delta").saveAsTable(f"{catalog}.{schema}.gleif_relationships_raw")

print("Silver GLEIF tables created.")
