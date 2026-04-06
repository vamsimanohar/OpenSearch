#!/usr/bin/env python3
"""
Creates an Iceberg table in AWS Glue catalog with sample NYC-taxi-like data in S3.
Uses pyiceberg to create proper Iceberg metadata (with Avro manifests).

Usage:
    SSL_CERT_FILE=/etc/pki/tls/certs/ca-bundle.crt AWS_PROFILE=default python scripts/create-iceberg-test-data.py

Defaults (override with env vars):
    AWS_REGION=us-west-2
    S3_BUCKET=opensearch-iceberg-test-263689514295
    GLUE_DATABASE=opensearch_lakehouse
    TABLE_NAME=test_events
"""

import os
import random
from datetime import datetime, timedelta

import pyarrow as pa
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DoubleType,
    IntegerType,
    NestedField,
    StringType,
    TimestampType,
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

# 20-column NYC-taxi-like schema matching integration test expectations
schema = Schema(
    NestedField(1, "vendorid", IntegerType(), required=False),
    NestedField(2, "tpep_pickup_datetime", TimestampType(), required=False),
    NestedField(3, "tpep_dropoff_datetime", TimestampType(), required=False),
    NestedField(4, "passenger_count", IntegerType(), required=False),
    NestedField(5, "trip_distance", DoubleType(), required=False),
    NestedField(6, "ratecodeid", IntegerType(), required=False),
    NestedField(7, "store_and_fwd_flag", StringType(), required=False),
    NestedField(8, "pulocationid", IntegerType(), required=False),
    NestedField(9, "dolocationid", IntegerType(), required=False),
    NestedField(10, "payment_type", IntegerType(), required=False),
    NestedField(11, "fare_amount", DoubleType(), required=False),
    NestedField(12, "extra", DoubleType(), required=False),
    NestedField(13, "mta_tax", DoubleType(), required=False),
    NestedField(14, "tip_amount", DoubleType(), required=False),
    NestedField(15, "tolls_amount", DoubleType(), required=False),
    NestedField(16, "improvement_surcharge", DoubleType(), required=False),
    NestedField(17, "total_amount", DoubleType(), required=False),
    NestedField(18, "congestion_surcharge", DoubleType(), required=False),
    NestedField(19, "airport_fee", DoubleType(), required=False),
    NestedField(20, "cbd_congestion_fee", DoubleType(), required=False),
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

# Generate 50 realistic records with deterministic seed
rng = random.Random(42)
n = 50
base_pickup = datetime(2024, 1, 15, 8, 0, 0)
flags = ["Y", "N"]

vendorids = []
pickups = []
dropoffs = []
passengers = []
distances = []
ratecodes = []
store_flags = []
pu_locs = []
do_locs = []
pay_types = []
fares = []
extras = []
mta_taxes = []
tips = []
tolls_list = []
improvements = []
totals = []
congestions = []
airport_fees = []
cbd_fees = []

for i in range(n):
    vid = (i % 3) + 1
    pcount = rng.randint(1, 6)
    dist = round(0.5 + rng.random() * 25.0, 2)
    rcode = (i % 5) + 1
    flag = flags[i % 2]
    pu = 100 + rng.randint(0, 199)
    do = 100 + rng.randint(0, 199)
    ptype = (i % 4) + 1
    fare = round(5.0 + dist * 2.5 + rng.random() * 10.0, 2)
    ext = [0.5, 1.0, 0.0][i % 3]
    mta = 0.5
    tip = round(fare * (0.1 + rng.random() * 0.2), 2) if ptype == 1 else 0.0
    toll = round(5.0 + rng.random() * 8.0, 2) if i % 7 == 0 else 0.0
    imp = 0.3
    cong = 2.5 if i % 4 == 0 else 0.0
    af = 1.25 if i % 10 == 0 else 0.0
    cbd = 2.75 if i % 8 == 0 else 0.0
    total = round(fare + ext + mta + tip + toll + imp + cong + af + cbd, 2)

    pickup = base_pickup + timedelta(minutes=i * 30 + rng.randint(0, 14))
    dropoff = pickup + timedelta(minutes=5 + rng.randint(0, 39))

    vendorids.append(vid)
    pickups.append(pickup)
    dropoffs.append(dropoff)
    passengers.append(pcount)
    distances.append(dist)
    ratecodes.append(rcode)
    store_flags.append(flag)
    pu_locs.append(pu)
    do_locs.append(do)
    pay_types.append(ptype)
    fares.append(fare)
    extras.append(ext)
    mta_taxes.append(mta)
    tips.append(tip)
    tolls_list.append(toll)
    improvements.append(imp)
    totals.append(total)
    congestions.append(cong)
    airport_fees.append(af)
    cbd_fees.append(cbd)

data = pa.table(
    {
        "vendorid": pa.array(vendorids, type=pa.int32()),
        "tpep_pickup_datetime": pa.array(pickups, type=pa.timestamp("us")),
        "tpep_dropoff_datetime": pa.array(dropoffs, type=pa.timestamp("us")),
        "passenger_count": pa.array(passengers, type=pa.int32()),
        "trip_distance": pa.array(distances, type=pa.float64()),
        "ratecodeid": pa.array(ratecodes, type=pa.int32()),
        "store_and_fwd_flag": pa.array(store_flags, type=pa.string()),
        "pulocationid": pa.array(pu_locs, type=pa.int32()),
        "dolocationid": pa.array(do_locs, type=pa.int32()),
        "payment_type": pa.array(pay_types, type=pa.int32()),
        "fare_amount": pa.array(fares, type=pa.float64()),
        "extra": pa.array(extras, type=pa.float64()),
        "mta_tax": pa.array(mta_taxes, type=pa.float64()),
        "tip_amount": pa.array(tips, type=pa.float64()),
        "tolls_amount": pa.array(tolls_list, type=pa.float64()),
        "improvement_surcharge": pa.array(improvements, type=pa.float64()),
        "total_amount": pa.array(totals, type=pa.float64()),
        "congestion_surcharge": pa.array(congestions, type=pa.float64()),
        "airport_fee": pa.array(airport_fees, type=pa.float64()),
        "cbd_congestion_fee": pa.array(cbd_fees, type=pa.float64()),
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
print(f"Schema:      20 columns (NYC-taxi-like)")
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
print(f'    -d \'{{"query":"SELECT * FROM {TABLE_NAME} LIMIT 5"}}\'')
