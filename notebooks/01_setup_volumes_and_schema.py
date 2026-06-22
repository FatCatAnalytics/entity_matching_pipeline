# Databricks notebook source

catalog = "gleif_project"
schema = "gleif_db"
volume = "gleif_vl"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")

base_path = f"/Volumes/{catalog}/{schema}/{volume}"

for folder in [
    "raw_gleif/lei_cdf",
    "raw_gleif/rr_cdf",
    "uploads/incoming",
    "uploads/archive",
    "uploads/rejected",
    "exports",
]:
    dbutils.fs.mkdirs(f"{base_path}/{folder}/")

print("Schema, Volume, and folders are ready.")
print(base_path)
