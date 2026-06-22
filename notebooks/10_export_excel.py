# Databricks notebook source

# If openpyxl is not available on your cluster, uncomment and run once:
# %pip install openpyxl

import pandas as pd
from pathlib import Path
from datetime import datetime

catalog = "gleif_project"
schema = "gleif_db"
volume = "gleif_vl"

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
file_name = f"entity_mapping_export_{timestamp}.xlsx"
output_dir = Path(f"/Volumes/{catalog}/{schema}/{volume}/exports")
output_dir.mkdir(parents=True, exist_ok=True)
output_file = output_dir / file_name

tables_to_export = {
    "gold_mapping": f"{catalog}.{schema}.gold_client_to_internal_client_mapping",
    "review_queue": f"{catalog}.{schema}.review_queue",
    "match_results": f"{catalog}.{schema}.match_results",
    "entity_hierarchy": f"{catalog}.{schema}.entity_hierarchy_results",
}

with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
    for sheet_name, table_name in tables_to_export.items():
        pdf = spark.table(table_name).limit(100000).toPandas()
        pdf.to_excel(writer, sheet_name=sheet_name[:31], index=False)

print("Excel export created successfully.")
print(f"Output file: {output_file}")
print(f"File size: {output_file.stat().st_size:,} bytes")
