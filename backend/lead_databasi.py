#!/usr/bin/env python3
# ==================================================
# NYC TAXI DATABASE LOADER
# File: backend/load_database.py
# Run: python backend/load_database.py
# ==================================================

import pandas as pd
import sqlite3
import os
import json
from datetime import datetime

print("=" * 60)
print("NYC TAXI DATABASE LOADER")
print("=" * 60)

# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------
DB_PATH = 'data/nyc_taxi.db'
CLEAN_PARQUET = 'data/cleaned/trips_clean.parquet'
CLEAN_ZONES = 'data/cleaned/zones_clean.csv'
SAMPLE_CSV = 'data/cleaned/trips_sample.csv'

# --------------------------------------------------
# CHECK FILES EXIST
# --------------------------------------------------
print("\nChecking cleaned data files...")
files = [CLEAN_PARQUET, CLEAN_ZONES, SAMPLE_CSV]
for f in files:
    if os.path.exists(f):
        size = os.path.getsize(f) / (1024 * 1024)
        print(f"  FOUND: {f} ({size:.1f} MB)")
    else:
        print(f"  MISSING: {f} - Run clean_data.py first!")
        exit(1)

# --------------------------------------------------
# LOAD CLEANED DATA
# --------------------------------------------------
print("\nLoading cleaned data...")
trips = pd.read_parquet(CLEAN_PARQUET)
zones = pd.read_csv(CLEAN_ZONES)
print(f"  Loaded {len(trips):,} cleaned trips")
print(f"  Loaded {len(zones):,} zones")

# --------------------------------------------------
# CREATE DATABASE
# --------------------------------------------------
print(f"\nCreating database: {DB_PATH}")
os.makedirs('data', exist_ok=True)

# Remove existing database
if os.path.exists(DB_PATH):
    os.remove(DB_PATH)
    print("  Removed existing database")

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()
print("  Connected to database")

# --------------------------------------------------
# CREATE TABLES
# --------------------------------------------------
print("\nCreating tables...")

# Zones table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS zones (
        location_id INTEGER PRIMARY KEY,
        borough TEXT,
        zone TEXT,
        service_zone TEXT
    )
''')
print("  Created: zones table")

# Trips table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS trips (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        vendor_id INTEGER,
        pickup_datetime TEXT,
        dropoff_datetime TEXT,
        passenger_count INTEGER,
        trip_distance REAL,
        rate_code_id INTEGER,
        pickup_location_id INTEGER,
        dropoff_location_id INTEGER,
        payment_type INTEGER,
        fare_amount REAL,
        tip_amount REAL,
        tolls_amount REAL,
        total_amount REAL,
        trip_duration_mins REAL,
        avg_speed_mph REAL,
        tip_percentage REAL,
        hour_of_day INTEGER,
        time_of_day TEXT,
        day_of_week TEXT,
        is_weekend INTEGER,
        pickup_borough TEXT,
        pickup_zone TEXT,
        dropoff_borough TEXT,
        dropoff_zone TEXT,
        FOREIGN KEY (pickup_location_id) REFERENCES zones(location_id),
        FOREIGN KEY (dropoff_location_id) REFERENCES zones(location_id)
    )
''')
print("  Created: trips table")

# Summary stats table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS summary_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        stat_name TEXT,
        stat_value TEXT,
        created_at TEXT
    )
''')
print("  Created: summary_stats table")

conn.commit()

# --------------------------------------------------
# LOAD ZONES
# --------------------------------------------------
print("\nLoading zones into database...")
zones.to_sql('zones', conn, if_exists='replace', index=False)
print(f"  Inserted {len(zones):,} zones")

# --------------------------------------------------
# LOAD TRIPS (in batches)
# --------------------------------------------------
print("\nLoading trips into database (in batches)...")
BATCH_SIZE = 50000
total_inserted = 0

# Select only needed columns
cols = [
    'vendor_id', 'pickup_datetime', 'dropoff_datetime',
    'passenger_count', 'trip_distance', 'rate_code_id',
    'pickup_location_id', 'dropoff_location_id', 'payment_type',
    'fare_amount', 'tip_amount', 'tolls_amount', 'total_amount',
    'trip_duration_mins', 'avg_speed_mph', 'tip_percentage',
    'hour_of_day', 'time_of_day', 'day_of_week', 'is_weekend',
    'pickup_borough', 'pickup_zone', 'dropoff_borough', 'dropoff_zone'
]

# Filter only columns that exist
available_cols = [c for c in cols if c in trips.columns]
trips_subset = trips[available_cols].copy()

# Convert datetime to string for SQLite
if 'pickup_datetime' in trips_subset.columns:
    trips_subset['pickup_datetime'] = trips_subset[
        'pickup_datetime'].astype(str)
if 'dropoff_datetime' in trips_subset.columns:
    trips_subset['dropoff_datetime'] = trips_subset[
        'dropoff_datetime'].astype(str)

# Convert boolean to int
if 'is_weekend' in trips_subset.columns:
    trips_subset['is_weekend'] = trips_subset['is_weekend'].astype(int)

# Load in batches
for i in range(0, len(trips_subset), BATCH_SIZE):
    batch = trips_subset.iloc[i:i + BATCH_SIZE]
    batch.to_sql('trips', conn, if_exists='append', index=False)
    total_inserted += len(batch)
    print(f"  Inserted {total_inserted:,} / {len(trips_subset):,} trips...")

print(f"  Total inserted: {total_inserted:,} trips")

# --------------------------------------------------
# CREATE INDEXES
# --------------------------------------------------
print("\nCreating indexes for fast queries...")

cursor.execute(
    'CREATE INDEX IF NOT EXISTS idx_pickup_borough '
    'ON trips(pickup_borough)'
)
cursor.execute(
    'CREATE INDEX IF NOT EXISTS idx_dropoff_borough '
    'ON trips(dropoff_borough)'
)
cursor.execute(
    'CREATE INDEX IF NOT EXISTS idx_time_of_day '
    'ON trips(time_of_day)'
)
cursor.execute(
    'CREATE INDEX IF NOT EXISTS idx_day_of_week '
    'ON trips(day_of_week)'
)
cursor.execute(
    'CREATE INDEX IF NOT EXISTS idx_hour_of_day '
    'ON trips(hour_of_day)'
)
cursor.execute(
    'CREATE INDEX IF NOT EXISTS idx_pickup_location '
    'ON trips(pickup_location_id)'
)
cursor.execute(
    'CREATE INDEX IF NOT EXISTS idx_dropoff_location '
    'ON trips(dropoff_location_id)'
)
conn.commit()
print("  Created 7 indexes")

# --------------------------------------------------
# GENERATE SUMMARY STATS
# --------------------------------------------------
print("\nGenerating summary statistics...")

stats = {}

# Total trips
cursor.execute('SELECT COUNT(*) FROM trips')
stats['total_trips'] = cursor.fetchone()[0]

# Average fare
cursor.execute('SELECT AVG(fare_amount) FROM trips')
stats['avg_fare'] = round(cursor.fetchone()[0], 2)

# Average trip distance
cursor.execute('SELECT AVG(trip_distance) FROM trips')
stats['avg_distance'] = round(cursor.fetchone()[0], 2)

# Average trip duration
cursor.execute('SELECT AVG(trip_duration_mins) FROM trips')
stats['avg_duration_mins'] = round(cursor.fetchone()[0], 2)

# Average speed
cursor.execute('SELECT AVG(avg_speed_mph) FROM trips')
stats['avg_speed_mph'] = round(cursor.fetchone()[0], 2)

# Average tip percentage
cursor.execute('SELECT AVG(tip_percentage) FROM trips')
stats['avg_tip_percentage'] = round(cursor.fetchone()[0], 2)

# Most popular pickup borough
cursor.execute('''
    SELECT pickup_borough, COUNT(*) as cnt
    FROM trips
    WHERE pickup_borough IS NOT NULL
    GROUP BY pickup_borough
    ORDER BY cnt DESC
    LIMIT 1
''')
result = cursor.fetchone()
stats['top_pickup_borough'] = result[0] if result else 'Unknown'

# Peak hour
cursor.execute('''
    SELECT hour_of_day, COUNT(*) as cnt
    FROM trips
    GROUP BY hour_of_day
    ORDER BY cnt DESC
    LIMIT 1
''')
result = cursor.fetchone()
stats['peak_hour'] = result[0] if result else 0

# Save stats to database
now = str(datetime.now())
for name, value in stats.items():
    cursor.execute(
        'INSERT INTO summary_stats (stat_name, stat_value, created_at) '
        'VALUES (?, ?, ?)',
        (name, str(value), now)
    )
conn.commit()

# Save stats to JSON
os.makedirs('docs', exist_ok=True)
with open('docs/summary_stats.json', 'w') as f:
    json.dump(stats, f, indent=4)
print("  Saved: docs/summary_stats.json")

# --------------------------------------------------
# VERIFY DATABASE
# --------------------------------------------------
print("\nVerifying database...")
cursor.execute('SELECT COUNT(*) FROM trips')
trip_count = cursor.fetchone()[0]
cursor.execute('SELECT COUNT(*) FROM zones')
zone_count = cursor.fetchone()[0]
print(f"  Trips in DB : {trip_count:,}")
print(f"  Zones in DB : {zone_count:,}")

db_size = os.path.getsize(DB_PATH) / (1024 * 1024)
print(f"  DB size     : {db_size:.1f} MB")

conn.close()

# --------------------------------------------------
# FINAL SUMMARY
# --------------------------------------------------
print("\n" + "=" * 60)
print("DATABASE LOADED SUCCESSFULLY!")
print("=" * 60)
print(f"  Database    : {DB_PATH}")
print(f"  Total trips : {trip_count:,}")
print(f"  Total zones : {zone_count:,}")
print(f"  DB size     : {db_size:.1f} MB")
print("\n  SUMMARY STATS:")
for name, value in stats.items():
    print(f"  {name}: {value}")
print("\n  OUTPUT FILES:")
print(f"  {DB_PATH}")
print("  docs/summary_stats.json")
print("=" * 60)
print("NEXT STEP: python backend/api_server.py")
print("=" * 60)