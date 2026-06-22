# Databricks notebook source

from pyspark.sql.functions import udf, col, current_timestamp, desc, row_number
from pyspark.sql.types import DoubleType, StringType
from pyspark.sql.window import Window
from difflib import SequenceMatcher

catalog = "gleif_project"
schema = "gleif_db"
run_id = "mvp_run_001"

def ratio(a, b):
    if not a or not b:
        return 0.0
    return float(SequenceMatcher(None, a, b).ratio())

def token_overlap(a, b):
    if not a or not b:
        return 0.0
    sa, sb = set(a.split()), set(b.split())
    if not sa or not sb:
        return 0.0
    return float(len(sa & sb) / len(sa | sb))

def field_match_score(client_value, candidate_value):
    if not client_value:
        return 0.5
    if not candidate_value:
        return 0.3
    return 1.0 if client_value == candidate_value else 0.0

def address_score(client_address, candidate_address):
    if not client_address:
        return 0.5
    if not candidate_address:
        return 0.3
    client_tokens = set(client_address.split())
    candidate_tokens = set(candidate_address.split())
    if not client_tokens or not candidate_tokens:
        return 0.3
    return float(len(client_tokens & candidate_tokens) / len(client_tokens | candidate_tokens))

def score(client_clean_name, candidate_clean_name, client_country, candidate_country, client_registration_number, candidate_registration_number, client_lei, candidate_lei, client_address, candidate_address, candidate_method):
    if client_lei and candidate_lei and client_lei == candidate_lei:
        return 0.99
    if client_registration_number and candidate_registration_number and client_registration_number == candidate_registration_number:
        name_s = ratio(client_clean_name, candidate_clean_name)
        country_s = field_match_score(client_country, candidate_country)
        return round(0.80 + 0.10 * name_s + 0.10 * country_s, 4)
    name_s = ratio(client_clean_name, candidate_clean_name)
    token_s = token_overlap(client_clean_name, candidate_clean_name)
    country_s = field_match_score(client_country, candidate_country)
    address_s = address_score(client_address, candidate_address)
    total = 0.60 * name_s + 0.20 * token_s + 0.15 * country_s + 0.05 * address_s
    if candidate_method == "NAME_COUNTRY_BLOCK":
        total += 0.03
    return round(min(float(total), 0.98), 4)

def band(s):
    if s is None:
        return "NO_MATCH"
    if s >= 0.90:
        return "AUTO_MATCH"
    if s >= 0.75:
        return "REVIEW"
    return "LOW_CONFIDENCE"

def reason(client_country, candidate_country, client_registration_number, candidate_registration_number, client_lei, candidate_lei, candidate_method):
    reasons = [f"candidate_method={candidate_method}"]
    if client_lei and candidate_lei and client_lei == candidate_lei:
        reasons.append("exact_lei_match")
    if client_registration_number and candidate_registration_number and client_registration_number == candidate_registration_number:
        reasons.append("registration_number_match")
    if client_country and candidate_country:
        reasons.append(f"country_match={client_country == candidate_country}")
    else:
        reasons.append("country_not_provided_or_missing")
    return "; ".join(reasons)

score_udf = udf(score, DoubleType())
band_udf = udf(band, StringType())
reason_udf = udf(reason, StringType())

candidates = spark.table(f"{catalog}.{schema}.match_candidates")

scored = (
    candidates
    .withColumn("match_score", score_udf(
        "client_clean_name",
        "candidate_clean_name",
        "client_country",
        "candidate_country",
        "client_registration_number",
        "candidate_registration_number",
        "client_lei",
        "candidate_lei",
        "client_address",
        "candidate_address",
        "candidate_method"
    ))
    .withColumn("confidence_band", band_udf("match_score"))
    .withColumn("match_reason", reason_udf(
        "client_country",
        "candidate_country",
        "client_registration_number",
        "candidate_registration_number",
        "client_lei",
        "candidate_lei",
        "candidate_method"
    ))
    .withColumn("country_match", (col("client_country").isNotNull()) & (col("client_country") == col("candidate_country")))
)

w = Window.partitionBy("client_entity_id").orderBy(desc("match_score"))
ranked = scored.withColumn("candidate_rank", row_number().over(w))

top_results = ranked.filter(col("candidate_rank") == 1).selectExpr(
    f"'{run_id}' as run_id",
    "client_entity_id",
    "submitted_name",
    "reference_id as matched_reference_id",
    "candidate_lei as matched_lei",
    "candidate_name as matched_name",
    "candidate_source as matched_source",
    "candidate_method",
    "match_score",
    "confidence_band",
    "match_reason",
    "country_match",
    "current_timestamp() as created_timestamp"
)

review_queue = ranked.filter(col("candidate_rank") <= 5).selectExpr(
    f"'{run_id}' as run_id",
    "client_entity_id",
    "submitted_name",
    "candidate_rank",
    "reference_id as matched_reference_id",
    "candidate_lei as matched_lei",
    "candidate_name as matched_name",
    "candidate_source as matched_source",
    "candidate_method",
    "match_score",
    "match_reason",
    "'PENDING' as review_status",
    "cast(null as string) as reviewer",
    "cast(null as string) as review_comment",
    "current_timestamp() as created_timestamp"
)

top_results.write.mode("overwrite").format("delta").saveAsTable(f"{catalog}.{schema}.match_results")
review_queue.write.mode("overwrite").format("delta").saveAsTable(f"{catalog}.{schema}.review_queue")

display(top_results)
