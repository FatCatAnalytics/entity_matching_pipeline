# Entity Matching Pipeline

Databricks MVP pipeline for mapping client-submitted corporate names to GLEIF entities and an internal client/entity master list.

## What it does

The pipeline supports:

- Raw GLEIF LEI-CDF and RR-CDF CSV ingestion
- Bronze Delta table creation
- Silver GLEIF entity and relationship tables
- Flexible client uploads where only `client_name` is mandatory
- Optional use of `country`, `registration_number`, `lei`, and `address` to improve matching accuracy
- Candidate generation using LEI, registration number, name/country blocking, and name-only blocking
- Match scoring and review queue creation
- Direct and ultimate parent enrichment from GLEIF RR-CDF
- Gold output mapping submitted client entities to the internal client/entity master
- Internal fallback matching by LEI, normalized name, and fuzzy name similarity
- Excel export for business review

## Data not stored in Git

Do not commit:

- GLEIF CSV/XML/JSON files
- Client files
- Internal client/entity files
- Delta files
- Excel exports
- Secrets or tokens

Only code, SQL setup scripts, and small templates are stored here.

## Required Databricks objects

Catalog and schema:

```sql
CREATE SCHEMA IF NOT EXISTS gleif_project.gleif_db;
```

Volume:

```sql
CREATE VOLUME IF NOT EXISTS gleif_project.gleif_db.gleif_vl;
```

Expected folders:

```text
/Volumes/gleif_project/gleif_db/gleif_vl/raw_gleif/lei_cdf/
/Volumes/gleif_project/gleif_db/gleif_vl/raw_gleif/rr_cdf/
/Volumes/gleif_project/gleif_db/gleif_vl/uploads/incoming/
/Volumes/gleif_project/gleif_db/gleif_vl/uploads/archive/
/Volumes/gleif_project/gleif_db/gleif_vl/uploads/rejected/
/Volumes/gleif_project/gleif_db/gleif_vl/exports/
```

## Files to upload

Upload GLEIF files:

```text
LEI-CDF CSV -> /Volumes/gleif_project/gleif_db/gleif_vl/raw_gleif/lei_cdf/
RR-CDF CSV  -> /Volumes/gleif_project/gleif_db/gleif_vl/raw_gleif/rr_cdf/
```

Upload submitted client file:

```text
/Volumes/gleif_project/gleif_db/gleif_vl/uploads/incoming/client_entities.csv
```

Upload internal client/entity file:

```text
/Volumes/gleif_project/gleif_db/gleif_vl/uploads/internal_client_entities.csv
```

## Client file formats

Minimal format:

```csv
client_name
Apple
Apple Asia Ltd
Barclays Bank
```

Enhanced optional format:

```csv
client_name,country,registration_number,lei,address
Apple,US,,,Cupertino
Apple Asia Ltd,HK,,,Hong Kong
Barclays Bank,GB,,,London
```

Only `client_name` is mandatory.

## Internal client/entity file format

Recommended format:

```csv
internal_entity_id,legal_name,country,address,lei,registration_number,internal_rating,business_owner
I001,Apple Inc,US,Cupertino,HWUPKR0MPOU8FGXBT394,,Low,Technology
I002,Barclays PLC,GB,London,213800LBQA1Y9L22JB70,,Medium,Banking
```

`legal_name` is mandatory. `lei` is recommended but not mandatory; if there is no internal LEI, the pipeline falls back to normalized and fuzzy name matching.

## Workflow A: GLEIF reference refresh

Run once initially, then only when GLEIF is refreshed:

```text
01_setup_volumes_and_schema.py
02_load_gleif_raw_to_bronze_delta.py
03_create_silver_gleif_tables.py
```

## Workflow B: Client matching

Run whenever a client file arrives:

```text
04_ingest_client_names.py
05_normalise_entities.py
06_generate_candidates.py
07_score_and_rank_matches.py
08_enrich_hierarchy.py
09_create_gold_client_to_internal_risk_mapping.py
10_export_excel.py
```

## Final output

Excel export is written to:

```text
/Volumes/gleif_project/gleif_db/gleif_vl/exports/
```

Gold Delta table:

```text
gleif_project.gleif_db.gold_client_to_internal_client_mapping
```
