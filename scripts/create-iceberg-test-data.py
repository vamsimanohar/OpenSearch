#!/usr/bin/env python3
"""
Creates an Iceberg table in AWS Glue catalog with sample data in S3.
Uses pyiceberg to create proper Iceberg metadata (with Avro manifests).

Usage:
    AWS_PROFILE=personal python scripts/create-iceberg-test-data.py

Defaults (override with env vars):
    AWS_REGION=us-west-2
    S3_BUCKET=opensearch-iceberg-test-263689514295
    GLUE_DATABASE=opensearch_lakehouse
    TABLE_NAME=test_events
"""

import os

import pyarrow as pa
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DoubleType,
    IntegerType,
    NestedField,
    StringType,
)

REGION = os.environ.get("AWS_REGION", "us-west-2")
BUCKET = os.environ.get("S3_BUCKET", "opensearch-iceberg-test-263689514295")
DATABASE = os.environ.get("GLUE_DATABASE", "opensearch_lakehouse")
TABLE_NAME = os.environ.get("TABLE_NAME", "test_events")
WAREHOUSE = f"s3://{BUCKET}/warehouse"

# Connect to Glue catalog
catalog = GlueCatalog(
    "glue",
    **{
        "warehouse": WAREHOUSE,
        "region_name": REGION,
    },
)

# Create namespace if needed
try:
    catalog.create_namespace(DATABASE)
    print(f"Created namespace: {DATABASE}")
except Exception:
    print(f"Namespace exists: {DATABASE}")

# Define schema
schema = Schema(
    NestedField(1, "id", IntegerType(), required=False),
    NestedField(2, "name", StringType(), required=False),
    NestedField(3, "value", DoubleType(), required=False),
    NestedField(4, "category", StringType(), required=False),
)

# Drop and recreate table
table_id = (DATABASE, TABLE_NAME)
try:
    catalog.drop_table(table_id)
    print(f"Dropped existing table: {DATABASE}.{TABLE_NAME}")
except Exception:
    pass

table = catalog.create_table(table_id, schema=schema)
print(f"Created table: {DATABASE}.{TABLE_NAME}")

# Build test data
data = pa.table(
    {
        "id": pa.array([1, 2, 3, 4, 5], type=pa.int32()),
        "name": pa.array(["alice", "bob", "charlie", "diana", "eve"]),
        "value": pa.array([10.5, 20.3, 30.1, 40.7, 50.0]),
        "category": pa.array(["A", "B", "A", "C", "B"]),
    }
)

# Append data (creates proper manifest files)
table.append(data)
print(f"Appended {len(data)} rows")

# Verify
scan = table.scan()
result = scan.to_arrow()
print(f"Verified: {len(result)} rows in table")

print(f"\n=== Iceberg Test Data Created ===")
print(f"Region:      {REGION}")
print(f"Bucket:      {BUCKET}")
print(f"Database:    {DATABASE}")
print(f"Table:       {TABLE_NAME}")
print(f"Location:    {table.location()}")
print(f"Snapshot:    {table.current_snapshot().snapshot_id}")
print(f"Records:     {len(result)}")
print(f"=================================\n")
print(f"Register in OpenSearch:")
print(f'  curl -X PUT localhost:9200/_lakehouse/catalog/glue-{REGION} \\')
print(f'    -H "Content-Type: application/json" \\')
print(f'    -d \'{{"type":"glue","warehouse":"{WAREHOUSE}","region":"{REGION}"}}\'')
print()
print(f'  curl -X PUT localhost:9200/_lakehouse/table/{TABLE_NAME} \\')
print(f'    -H "Content-Type: application/json" \\')
print(f'    -d \'{{"catalog":"glue-{REGION}","namespace":"{DATABASE}","table":"{TABLE_NAME}"}}\'')
print()
print(f'  curl -s -X POST localhost:9200/_analytics/sql \\')
print(f'    -H "Content-Type: application/json" \\')
print(f'    -d \'{{"query":"SELECT * FROM {TABLE_NAME}"}}\'')
