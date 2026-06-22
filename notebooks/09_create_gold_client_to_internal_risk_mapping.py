# Databricks notebook source

catalog = "gleif_project"
schema = "gleif_db"

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema}.gold_client_to_internal_risk_mapping AS
WITH hierarchy AS (
    SELECT *
    FROM {catalog}.{schema}.entity_hierarchy_results
),
risk AS (
    SELECT
        risk_entity_id,
        legal_name,
        lei,
        risk_rating,
        business_owner
    FROM {catalog}.{schema}.internal_risk_entities_raw
),
mapping_candidates AS (
    SELECT
        h.*,
        r.risk_entity_id,
        r.legal_name AS internal_risk_name,
        r.lei AS internal_risk_lei,
        r.risk_rating,
        r.business_owner,
        'DIRECT_LEI_MATCH_TO_INTERNAL_RISK' AS internal_mapping_type,
        1 AS mapping_priority
    FROM hierarchy h
    INNER JOIN risk r
        ON r.lei = h.matched_lei

    UNION ALL

    SELECT
        h.*,
        r.risk_entity_id,
        r.legal_name AS internal_risk_name,
        r.lei AS internal_risk_lei,
        r.risk_rating,
        r.business_owner,
        'DIRECT_PARENT_LEI_MATCH_TO_INTERNAL_RISK' AS internal_mapping_type,
        2 AS mapping_priority
    FROM hierarchy h
    INNER JOIN risk r
        ON r.lei = h.direct_parent_lei

    UNION ALL

    SELECT
        h.*,
        r.risk_entity_id,
        r.legal_name AS internal_risk_name,
        r.lei AS internal_risk_lei,
        r.risk_rating,
        r.business_owner,
        'ULTIMATE_PARENT_LEI_MATCH_TO_INTERNAL_RISK' AS internal_mapping_type,
        3 AS mapping_priority
    FROM hierarchy h
    INNER JOIN risk r
        ON r.lei = h.ultimate_parent_lei
),
ranked_candidates AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY client_entity_id
            ORDER BY mapping_priority ASC, match_score DESC
        ) AS rn
    FROM mapping_candidates
),
best_internal_match AS (
    SELECT *
    FROM ranked_candidates
    WHERE rn = 1
),
no_internal_match AS (
    SELECT
        h.*,
        CAST(NULL AS STRING) AS risk_entity_id,
        CAST(NULL AS STRING) AS internal_risk_name,
        CAST(NULL AS STRING) AS internal_risk_lei,
        CAST(NULL AS STRING) AS risk_rating,
        CAST(NULL AS STRING) AS business_owner,
        'NO_INTERNAL_RISK_MATCH' AS internal_mapping_type,
        99 AS mapping_priority,
        1 AS rn
    FROM hierarchy h
    LEFT ANTI JOIN best_internal_match b
        ON h.client_entity_id = b.client_entity_id
)
SELECT
    run_id,
    client_entity_id,
    submitted_name,
    matched_lei AS matched_gleif_lei,
    matched_name AS matched_gleif_name,
    matched_source,
    candidate_method,
    direct_parent_lei,
    direct_parent_name,
    direct_relationship_status,
    ultimate_parent_lei,
    ultimate_parent_name,
    ultimate_relationship_status,
    risk_entity_id,
    internal_risk_name,
    internal_risk_lei,
    risk_rating,
    business_owner,
    match_score,
    confidence_band,
    match_reason,
    internal_mapping_type,
    mapping_priority,
    current_timestamp() AS created_timestamp
FROM best_internal_match

UNION ALL

SELECT
    run_id,
    client_entity_id,
    submitted_name,
    matched_lei AS matched_gleif_lei,
    matched_name AS matched_gleif_name,
    matched_source,
    candidate_method,
    direct_parent_lei,
    direct_parent_name,
    direct_relationship_status,
    ultimate_parent_lei,
    ultimate_parent_name,
    ultimate_relationship_status,
    risk_entity_id,
    internal_risk_name,
    internal_risk_lei,
    risk_rating,
    business_owner,
    match_score,
    confidence_band,
    match_reason,
    internal_mapping_type,
    mapping_priority,
    current_timestamp() AS created_timestamp
FROM no_internal_match
""")

display(spark.table(f"{catalog}.{schema}.gold_client_to_internal_risk_mapping"))
