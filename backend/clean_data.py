# ==================================================
# NYC TAXI DATA CLEANING SCRIPT
# File: backend/clean_data.py
# Run: python backend/clean_data.py
# ==================================================

import pandas as pd
import os
import json
from datetime import datetime

print("=" * 60)
print("NYC TAXI DATA CLEANING PIPELINE")
print("=" * 60)

# --------------------------------------------------
# LOAD FILES
# --------------------------------------------------
print("\nLOADING FILES...")

# Check files exist
files = [
    'data/yellow_tripdata_2024-01.parquet',
    'data/taxi_zone_lookup.csv',
    'data/taxi_zones.geojson'
]

for f in files:
    if os.path.exists(f):
        size = os.path.getsize(f) / (1024 * 1024)
        print(f"  FOUND: {f} ({size:.1f} MB)")
    else:
        print(f"  MISSING: {f} ← Download this file!")

# Load trip data
print("\nLoading trips...")
trips = pd.read_parquet('data/yellow_tripdata_2024-01.parquet')
original_count = len(trips)
print(f"  Loaded {original_count:,} trips")

# Load zone lookup
print("Loading zones...")
zones = pd.read_csv('data/taxi_zone_lookup.csv')
print(f"  Loaded {len(zones):,} zones")

# Load GeoJSON
print("Loading GeoJSON...")
try:
    with open('data/taxi_zones.geojson', 'r') as f:
        geo_zones = json.load(f)
    print(f"  Loaded GeoJSON successfully")
except Exception as e:
    print(f"  GeoJSON warning: {e}")
    geo_zones = None

# --------------------------------------------------
# SHOW RAW DATA INFO
# --------------------------------------------------
print("\n" + "=" * 60)
print("RAW DATA OVERVIEW")
print("=" * 60)
print(f"\nColumn names in trips:")
for col in trips.columns.tolist():
    print(f"  - {col}")

print(f"\nShape: {trips.shape[0]:,} rows x {trips.shape[1]} columns")

print(f"\nMissing values:")
missing = trips.isnull().sum()
for col, count in missing[missing > 0].items():
    print(f"  - {col}: {count:,} missing")

print(f"\nBasic statistics:")
for col in ['fare_amount', 'trip_distance', 'passenger_count']:
    if col in trips.columns:
        print(f"  {col}:")
        print(f"    min  = {trips[col].min()}")
        print(f"    max  = {trips[col].max()}")
        print(f"    mean = {trips[col].mean():.2f}")

# --------------------------------------------------
# CLEANING LOG
# --------------------------------------------------
cleaning_log = {
    "date": str(datetime.now()),
    "original_rows": original_count,
    "removed": {}
}

print("\n" + "=" * 60)
print("CLEANING DATA")
print("=" * 60)

# --------------------------------------------------
# RENAME COLUMNS
# --------------------------------------------------
print("\nStep 1: Renaming columns...")
rename_map = {}
if 'tpep_pickup_datetime'  in trips.columns:
    rename_map['tpep_pickup_datetime']  = 'pickup_datetime'
if 'tpep_dropoff_datetime' in trips.columns:
    rename_map['tpep_dropoff_datetime'] = 'dropoff_datetime'
if 'VendorID'              in trips.columns:
    rename_map['VendorID']              = 'vendor_id'
if 'RatecodeID'            in trips.columns:
    rename_map['RatecodeID']            = 'rate_code_id'
if 'PULocationID'          in trips.columns:
    rename_map['PULocationID']          = 'pickup_location_id'
if 'DOLocationID'          in trips.columns:
    rename_map['DOLocationID']          = 'dropoff_location_id'

trips = trips.rename(columns=rename_map)
print(f"  Renamed {len(rename_map)} columns")

# --------------------------------------------------
# FIX DATA TYPES
# --------------------------------------------------
print("\nStep 2: Fixing data types...")
trips['pickup_datetime']  = pd.to_datetime(
    trips['pickup_datetime'],  errors='coerce'
)
trips['dropoff_datetime'] = pd.to_datetime(
    trips['dropoff_datetime'], errors='coerce'
)

for col in ['fare_amount', 'trip_distance', 'passenger_count',
            'tip_amount', 'total_amount',
            'pickup_location_id', 'dropoff_location_id']:
    if col in trips.columns:
        trips[col] = pd.to_numeric(trips[col], errors='coerce')

print("  Data types fixed")

# --------------------------------------------------
# REMOVE DUPLICATES
# --------------------------------------------------
print("\nStep 3: Removing duplicates...")
before = len(trips)
trips  = trips.drop_duplicates()
removed = before - len(trips)
cleaning_log["removed"]["duplicates"] = int(removed)
print(f"  Removed {removed:,} duplicates")

# --------------------------------------------------
# HANDLE MISSING VALUES
# --------------------------------------------------
print("\nStep 4: Handling missing values...")
trips['passenger_count'] = trips['passenger_count'].fillna(1)

before = len(trips)
trips  = trips.dropna(subset=[
    'pickup_datetime',    'dropoff_datetime',
    'pickup_location_id', 'dropoff_location_id',
    'fare_amount',        'trip_distance'
])
removed = before - len(trips)
cleaning_log["removed"]["missing_critical"] = int(removed)
print(f"  Removed {removed:,} rows with missing critical data")

# --------------------------------------------------
# REMOVE IMPOSSIBLE VALUES
# --------------------------------------------------
print("\nStep 5: Removing impossible values...")

def remove_bad(df, mask, label, log):
    count = int(mask.sum())
    log["removed"][label] = count
    print(f"  Removed {count:,} rows - {label}")
    return df[~mask]

trips = remove_bad(trips,
    trips['fare_amount'] <= 0,
    "negative_or_zero_fare", cleaning_log)

trips = remove_bad(trips,
    trips['trip_distance'] <= 0,
    "zero_or_negative_distance", cleaning_log)

trips = remove_bad(trips,
    trips['trip_distance'] > 200,
    "extreme_distance_over_200mi", cleaning_log)

trips = remove_bad(trips,
    trips['fare_amount'] > 1000,
    "extreme_fare_over_1000", cleaning_log)

trips = remove_bad(trips,
    (trips['passenger_count'] <= 0) | (trips['passenger_count'] > 6),
    "invalid_passenger_count", cleaning_log)

trips = remove_bad(trips,
    trips['dropoff_datetime'] <= trips['pickup_datetime'],
    "dropoff_before_pickup", cleaning_log)

duration = (
    trips['dropoff_datetime'] - trips['pickup_datetime']
).dt.total_seconds()

trips = remove_bad(trips,
    duration > 86400,
    "trip_over_24_hours", cleaning_log)

duration = (
    trips['dropoff_datetime'] - trips['pickup_datetime']
).dt.total_seconds()

trips = remove_bad(trips,
    duration < 60,
    "trip_under_1_minute", cleaning_log)

print(f"\n  Rows after cleaning: {len(trips):,}")

# --------------------------------------------------
# CREATE DERIVED FEATURES
# --------------------------------------------------
print("\n" + "=" * 60)
print("CREATING DERIVED FEATURES")
print("=" * 60)

# Feature 1: Trip duration in minutes
# WHY: Understand how long trips take across city
trips['trip_duration_mins'] = (
    (trips['dropoff_datetime'] - trips['pickup_datetime'])
    .dt.total_seconds() / 60
).round(2)
print("\n  Feature 1 created: trip_duration_mins")
print("  WHY: Helps analyze traffic and efficiency patterns")

# Feature 2: Average speed in mph
# WHY: Reveals traffic congestion across boroughs
trips['avg_speed_mph'] = (
    trips['trip_distance'] /
    (trips['trip_duration_mins'] / 60)
).round(2)
trips = trips[trips['avg_speed_mph'].between(1, 150)]
print("\n  Feature 2 created: avg_speed_mph")
print("  WHY: Low speed = heavy traffic, useful for urban planning")

# Feature 3: Tip percentage
# WHY: Shows tipping culture by location and time
trips['tip_percentage'] = (
    (trips['tip_amount'] / trips['fare_amount']) * 100
).round(2).clip(0, 100).fillna(0)
print("\n  Feature 3 created: tip_percentage")
print("  WHY: Reveals tipping behavior across NYC boroughs")

# Feature 4: Time of day
# WHY: Groups trips into meaningful rush hour periods
def time_of_day(hour):
    if   5  <= hour < 9:  return 'Morning Rush'
    elif 9  <= hour < 12: return 'Mid Morning'
    elif 12 <= hour < 17: return 'Afternoon'
    elif 17 <= hour < 20: return 'Evening Rush'
    elif 20 <= hour < 24: return 'Night'
    else:                 return 'Late Night'

trips['hour_of_day'] = trips['pickup_datetime'].dt.hour
trips['time_of_day'] = trips['hour_of_day'].apply(time_of_day)
print("\n  Feature 4 created: time_of_day")
print("  WHY: Identifies peak demand periods in the city")

# Feature 5: Day of week
# WHY: Weekday vs weekend mobility patterns differ greatly
trips['day_of_week'] = trips['pickup_datetime'].dt.day_name()
trips['is_weekend']  = trips['pickup_datetime'].dt.dayofweek.isin([5, 6])
print("\n  Feature 5 created: day_of_week + is_weekend")
print("  WHY: Weekend vs weekday trips show different patterns")

# --------------------------------------------------
# MERGE ZONE NAMES
# --------------------------------------------------
print("\n" + "=" * 60)
print("MERGING ZONE DATA")
print("=" * 60)

zones = zones.rename(columns={
    'LocationID':   'location_id',
    'Borough':      'borough',
    'Zone':         'zone',
    'service_zone': 'service_zone'
})

# Add pickup borough and zone name
trips = trips.merge(
    zones[['location_id', 'borough', 'zone']],
    left_on='pickup_location_id',
    right_on='location_id',
    how='left'
).rename(columns={
    'borough': 'pickup_borough',
    'zone':    'pickup_zone'
})
if 'location_id' in trips.columns:
    trips.drop(columns=['location_id'], inplace=True)

# Add dropoff borough and zone name
trips = trips.merge(
    zones[['location_id', 'borough', 'zone']],
    left_on='dropoff_location_id',
    right_on='location_id',
    how='left'
).rename(columns={
    'borough': 'dropoff_borough',
    'zone':    'dropoff_zone'
})
if 'location_id' in trips.columns:
    trips.drop(columns=['location_id'], inplace=True)

print("\n  Zone names merged successfully")
print(f"  Sample pickup boroughs: {trips['pickup_borough'].unique()[:5].tolist()}")

# --------------------------------------------------
# SAVE CLEANED DATA
# --------------------------------------------------
print("\n" + "=" * 60)
print("SAVING CLEANED DATA")
print("=" * 60)

os.makedirs('data/cleaned', exist_ok=True)

# Save full cleaned parquet
trips.to_parquet('data/cleaned/trips_clean.parquet', index=False)
print("\n  Saved: data/cleaned/trips_clean.parquet")

# Save cleaned zones
zones.to_csv('data/cleaned/zones_clean.csv', index=False)
print("  Saved: data/cleaned/zones_clean.csv")

# Save 10k sample for quick testing
trips.head(10000).to_csv(
    'data/cleaned/trips_sample.csv', index=False
)
print("  Saved: data/cleaned/trips_sample.csv (10,000 rows)")

# Save cleaning log
os.makedirs('docs', exist_ok=True)
final_count = len(trips)
cleaning_log["final_rows"]      = final_count
cleaning_log["percentage_kept"] = round(
    (final_count / original_count) * 100, 2
)

with open('docs/cleaning_log.json', 'w') as f:
    json.dump(cleaning_log, f, indent=4)
print("  Saved: docs/cleaning_log.json")

# --------------------------------------------------
# FINAL SUMMARY
# --------------------------------------------------
print("\n" + "=" * 60)
print("CLEANING COMPLETE - SUMMARY")
print("=" * 60)
print(f"  Original rows  : {original_count:,}")
print(f"  Final rows     : {final_count:,}")
print(f"  Rows removed   : {original_count - final_count:,}")
print(f"  Data kept      : {cleaning_log['percentage_kept']}%")
print("\n  DERIVED FEATURES:")
print("  1. trip_duration_mins  - Trip length in minutes")
print("  2. avg_speed_mph       - Average speed per trip")
print("  3. tip_percentage      - Tip as % of fare")
print("  4. time_of_day         - Rush hour categorization")
print("  5. day_of_week         - Day name + is_weekend flag")
print("\n  OUTPUT FILES:")
print("  data/cleaned/trips_clean.parquet")
print("  data/cleaned/zones_clean.csv")
print("  data/cleaned/trips_sample.csv")
print("  docs/cleaning_log.json")
print("=" * 60)
print("NEXT STEP: python backend/load_database.py")
print("=" * 60)



## **Step 4: Save the File**