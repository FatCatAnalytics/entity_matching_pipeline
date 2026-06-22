# Databricks notebook source

catalog = "gleif_project"
schema = "gleif_db"

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema}.gold_client_to_internal_client_mapping AS
WITH hierarchy AS (
    SELECT
        *,
        trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(lower(coalesce(matched_name, '')), '&', ' and '), '[^a-z0-9 ]', ' '), '\\b(inc|incorporated|ltd|limited|llc|plc|corp|corporation|co|company|ag|sa|bv|nv|gmbh|pte|pty|holdings|holding|group)\\b', ' '), ' +', ' ')) AS matched_clean_name,
        trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(lower(coalesce(direct_parent_name, '')), '&', ' and '), '[^a-z0-9 ]', ' '), '\\b(inc|incorporated|ltd|limited|llc|plc|corp|corporation|co|company|ag|sa|bv|nv|gmbh|pte|pty|holdings|holding|group)\\b', ' '), ' +', ' ')) AS direct_parent_clean_name,
        trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(lower(coalesce(ultimate_parent_name, '')), '&', ' and '), '[^a-z0-9 ]', ' '), '\\b(inc|incorporated|ltd|limited|llc|plc|corp|corporation|co|company|ag|sa|bv|nv|gmbh|pte|pty|holdings|holding|group)\\b', ' '), ' +', ' ')) AS ultimate_parent_clean_name
    FROM {catalog}.{schema}.entity_hierarchy_results
),
hierarchy_blocked AS (
    SELECT
        *,
        split(matched_clean_name, ' ')[0] AS matched_first_token,
        substring(matched_clean_name, 1, 4) AS matched_prefix_4,
        split(direct_parent_clean_name, ' ')[0] AS direct_parent_first_token,
        substring(direct_parent_clean_name, 1, 4) AS direct_parent_prefix_4,
        split(ultimate_parent_clean_name, ' ')[0] AS ultimate_parent_first_token,
        substring(ultimate_parent_clean_name, 1, 4) AS ultimate_parent_prefix_4
    FROM hierarchy
),
internal_client AS (
    SELECT
        internal_entity_id,
        legal_name,
        nullif(upper(trim(lei)), '') AS lei_norm,
        lei,
        internal_rating,
        business_owner,
        trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(lower(coalesce(legal_name, '')), '&', ' and '), '[^a-z0-9 ]', ' '), '\\b(inc|incorporated|ltd|limited|llc|plc|corp|corporation|co|company|ag|sa|bv|nv|gmbh|pte|pty|holdings|holding|group)\\b', ' '), ' +', ' ')) AS internal_clean_name
    FROM {catalog}.{schema}.internal_client_entities_raw
),
internal_client_blocked AS (
    SELECT
        *,
        split(internal_clean_name, ' ')[0] AS internal_first_token,
        substring(internal_clean_name, 1, 4) AS internal_prefix_4
    FROM internal_client
),
lei_candidates AS (
    SELECT h.*, i.internal_entity_id, i.legal_name AS internal_client_name, i.lei AS internal_client_lei, i.internal_rating, i.business_owner,
           'DIRECT_LEI_MATCH_TO_INTERNAL_CLIENT' AS internal_mapping_type, 1 AS mapping_priority, 1.00 AS internal_mapping_score
    FROM hierarchy_blocked h
    INNER JOIN internal_client_blocked i ON i.lei_norm IS NOT NULL AND i.lei_norm = h.matched_lei

    UNION ALL

    SELECT h.*, i.internal_entity_id, i.legal_name AS internal_client_name, i.lei AS internal_client_lei, i.internal_rating, i.business_owner,
           'DIRECT_PARENT_LEI_MATCH_TO_INTERNAL_CLIENT' AS internal_mapping_type, 2 AS mapping_priority, 1.00 AS internal_mapping_score
    FROM hierarchy_blocked h
    INNER JOIN internal_client_blocked i ON i.lei_norm IS NOT NULL AND i.lei_norm = h.direct_parent_lei

    UNION ALL

    SELECT h.*, i.internal_entity_id, i.legal_name AS internal_client_name, i.lei AS internal_client_lei, i.internal_rating, i.business_owner,
           'ULTIMATE_PARENT_LEI_MATCH_TO_INTERNAL_CLIENT' AS internal_mapping_type, 3 AS mapping_priority, 1.00 AS internal_mapping_score
    FROM hierarchy_blocked h
    INNER JOIN internal_client_blocked i ON i.lei_norm IS NOT NULL AND i.lei_norm = h.ultimate_parent_lei
),
name_candidates AS (
    SELECT h.*, i.internal_entity_id, i.legal_name AS internal_client_name, i.lei AS internal_client_lei, i.internal_rating, i.business_owner,
           'EXACT_NORMALIZED_NAME_MATCH_TO_INTERNAL_CLIENT' AS internal_mapping_type, 4 AS mapping_priority, 0.92 AS internal_mapping_score
    FROM hierarchy_blocked h
    INNER JOIN internal_client_blocked i ON h.matched_clean_name <> '' AND i.internal_clean_name <> '' AND h.matched_clean_name = i.internal_clean_name

    UNION ALL

    SELECT h.*, i.internal_entity_id, i.legal_name AS internal_client_name, i.lei AS internal_client_lei, i.internal_rating, i.business_owner,
           'DIRECT_PARENT_NORMALIZED_NAME_MATCH_TO_INTERNAL_CLIENT' AS internal_mapping_type, 5 AS mapping_priority, 0.90 AS internal_mapping_score
    FROM hierarchy_blocked h
    INNER JOIN internal_client_blocked i ON h.direct_parent_clean_name <> '' AND i.internal_clean_name <> '' AND h.direct_parent_clean_name = i.internal_clean_name

    UNION ALL

    SELECT h.*, i.internal_entity_id, i.legal_name AS internal_client_name, i.lei AS internal_client_lei, i.internal_rating, i.business_owner,
           'ULTIMATE_PARENT_NORMALIZED_NAME_MATCH_TO_INTERNAL_CLIENT' AS internal_mapping_type, 6 AS mapping_priority, 0.88 AS internal_mapping_score
    FROM hierarchy_blocked h
    INNER JOIN internal_client_blocked i ON h.ultimate_parent_clean_name <> '' AND i.internal_clean_name <> '' AND h.ultimate_parent_clean_name = i.internal_clean_name
),
fuzzy_candidates AS (
    SELECT h.*, i.internal_entity_id, i.legal_name AS internal_client_name, i.lei AS internal_client_lei, i.internal_rating, i.business_owner,
           'FUZZY_NAME_MATCH_TO_INTERNAL_CLIENT' AS internal_mapping_type, 7 AS mapping_priority,
           round(1.0 - (levenshtein(h.matched_clean_name, i.internal_clean_name) / greatest(length(h.matched_clean_name), length(i.internal_clean_name))), 4) AS internal_mapping_score
    FROM hierarchy_blocked h
    INNER JOIN internal_client_blocked i
        ON h.matched_clean_name <> ''
       AND i.internal_clean_name <> ''
       AND (h.matched_first_token = i.internal_first_token OR h.matched_prefix_4 = i.internal_prefix_4)
       AND 1.0 - (levenshtein(h.matched_clean_name, i.internal_clean_name) / greatest(length(h.matched_clean_name), length(i.internal_clean_name))) >= 0.82

    UNION ALL

    SELECT h.*, i.internal_entity_id, i.legal_name AS internal_client_name, i.lei AS internal_client_lei, i.internal_rating, i.business_owner,
           'DIRECT_PARENT_FUZZY_NAME_MATCH_TO_INTERNAL_CLIENT' AS internal_mapping_type, 8 AS mapping_priority,
           round(1.0 - (levenshtein(h.direct_parent_clean_name, i.internal_clean_name) / greatest(length(h.direct_parent_clean_name), length(i.internal_clean_name))), 4) AS internal_mapping_score
    FROM hierarchy_blocked h
    INNER JOIN internal_client_blocked i
        ON h.direct_parent_clean_name <> ''
       AND i.internal_clean_name <> ''
       AND (h.direct_parent_first_token = i.internal_first_token OR h.direct_parent_prefix_4 = i.internal_prefix_4)
       AND 1.0 - (levenshtein(h.direct_parent_clean_name, i.internal_clean_name) / greatest(length(h.direct_parent_clean_name), length(i.internal_clean_name))) >= 0.82

    UNION ALL

    SELECT h.*, i.internal_entity_id, i.legal_name AS internal_client_name, i.lei AS internal_client_lei, i.internal_rating, i.business_owner,
           'ULTIMATE_PARENT_FUZZY_NAME_MATCH_TO_INTERNAL_CLIENT' AS internal_mapping_type, 9 AS mapping_priority,
           round(1.0 - (levenshtein(h.ultimate_parent_clean_name, i.internal_clean_name) / greatest(length(h.ultimate_parent_clean_name), length(i.internal_clean_name))), 4) AS internal_mapping_score
    FROM hierarchy_blocked h
    INNER JOIN internal_client_blocked i
        ON h.ultimate_parent_clean_name <> ''
       AND i.internal_clean_name <> ''
       AND (h.ultimate_parent_first_token = i.internal_first_token OR h.ultimate_parent_prefix_4 = i.internal_prefix_4)
       AND 1.0 - (levenshtein(h.ultimate_parent_clean_name, i.internal_clean_name) / greatest(length(h.ultimate_parent_clean_name), length(i.internal_clean_name))) >= 0.82
),
mapping_candidates AS (
    SELECT * FROM lei_candidates
    UNION ALL
    SELECT * FROM name_candidates
    UNION ALL
    SELECT * FROM fuzzy_candidates
),
ranked_candidates AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY client_entity_id ORDER BY mapping_priority ASC, internal_mapping_score DESC, match_score DESC) AS rn
    FROM mapping_candidates
),
best_internal_match AS (
    SELECT * FROM ranked_candidates WHERE rn = 1
),
no_internal_match AS (
    SELECT
        h.*,
        CAST(NULL AS STRING) AS internal_entity_id,
        CAST(NULL AS STRING) AS internal_client_name,
        CAST(NULL AS STRING) AS internal_client_lei,
        CAST(NULL AS STRING) AS internal_rating,
        CAST(NULL AS STRING) AS business_owner,
        'NO_INTERNAL_CLIENT_MATCH' AS internal_mapping_type,
        99 AS mapping_priority,
        CAST(NULL AS DOUBLE) AS internal_mapping_score,
        1 AS rn
    FROM hierarchy_blocked h
    LEFT ANTI JOIN best_internal_match b ON h.client_entity_id = b.client_entity_id
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
    internal_entity_id AS internal_client_entity_id,
    internal_client_name,
    internal_client_lei,
    internal_rating,
    business_owner,
    match_score,
    confidence_band,
    match_reason,
    internal_mapping_type,
    mapping_priority,
    internal_mapping_score,
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
    internal_entity_id AS internal_client_entity_id,
    internal_client_name,
    internal_client_lei,
    internal_rating,
    business_owner,
    match_score,
    confidence_band,
    match_reason,
    internal_mapping_type,
    mapping_priority,
    internal_mapping_score,
    current_timestamp() AS created_timestamp
FROM no_internal_match
""")

display(spark.table(f"{catalog}.{schema}.gold_client_to_internal_client_mapping"))
