# Databricks notebook source

from pyspark.sql.functions import col, upper, trim

catalog = "gleif_project"
schema = "gleif_db"

matches = spark.table(f"{catalog}.{schema}.match_results").alias("m")
rels = spark.table(f"{catalog}.{schema}.gleif_relationships_raw")
gleif = spark.table(f"{catalog}.{schema}.gleif_entities_raw")

direct_rels = (
    rels
    .filter(upper(trim(col("relationship_type"))) == "IS_DIRECTLY_CONSOLIDATED_BY")
    .filter(col("parent_lei").isNotNull())
    .select(
        col("child_lei").alias("direct_child_lei"),
        col("parent_lei").alias("direct_parent_lei"),
        col("relationship_status").alias("direct_relationship_status"),
        col("period_start").alias("direct_period_start"),
        col("period_end").alias("direct_period_end")
    )
    .alias("dr")
)

ultimate_rels = (
    rels
    .filter(upper(trim(col("relationship_type"))) == "IS_ULTIMATELY_CONSOLIDATED_BY")
    .filter(col("parent_lei").isNotNull())
    .select(
        col("child_lei").alias("ultimate_child_lei"),
        col("parent_lei").alias("ultimate_parent_lei"),
        col("relationship_status").alias("ultimate_relationship_status"),
        col("period_start").alias("ultimate_period_start"),
        col("period_end").alias("ultimate_period_end")
    )
    .alias("ur")
)

direct_parent_entities = gleif.select(
    col("lei").alias("direct_parent_entity_lei"),
    col("legal_name").alias("direct_parent_name")
).alias("dpe")

ultimate_parent_entities = gleif.select(
    col("lei").alias("ultimate_parent_entity_lei"),
    col("legal_name").alias("ultimate_parent_name")
).alias("upe")

enriched = (
    matches.alias("m")
    .join(direct_rels, col("m.matched_lei") == col("dr.direct_child_lei"), "left")
    .join(ultimate_rels, col("m.matched_lei") == col("ur.ultimate_child_lei"), "left")
    .join(direct_parent_entities, col("dr.direct_parent_lei") == col("dpe.direct_parent_entity_lei"), "left")
    .join(ultimate_parent_entities, col("ur.ultimate_parent_lei") == col("upe.ultimate_parent_entity_lei"), "left")
    .select(
        col("m.run_id"),
        col("m.client_entity_id"),
        col("m.submitted_name"),
        col("m.matched_lei"),
        col("m.matched_name"),
        col("m.matched_source"),
        col("m.candidate_method"),
        col("dr.direct_parent_lei"),
        col("dpe.direct_parent_name"),
        col("dr.direct_relationship_status"),
        col("dr.direct_period_start"),
        col("dr.direct_period_end"),
        col("ur.ultimate_parent_lei"),
        col("upe.ultimate_parent_name"),
        col("ur.ultimate_relationship_status"),
        col("ur.ultimate_period_start"),
        col("ur.ultimate_period_end"),
        col("m.match_score"),
        col("m.confidence_band"),
        col("m.match_reason")
    )
)

enriched.write.mode("overwrite").format("delta").saveAsTable(f"{catalog}.{schema}.entity_hierarchy_results")

display(enriched)
