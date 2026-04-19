# berlin-transport-data-pipeline

# Berlin Transport Data Engineering Pipeline

## Overview
Built a data engineering pipeline to process large-scale public transport data from Berlin, integrating planned timetables and real-time updates into a PostgreSQL data warehouse.

## Architecture
- Star Schema:
  - Fact: `fact_train_movement`
  - Dimensions: `dim_station`, `dim_train`, `dim_time`
- ETL Pipeline implemented in Python

## Features
- Processes 100k+ XML timetable files
- Handles real-time updates (delays, cancellations)
- Robust station matching (EVA, name normalization, fallback strategies)
- Incremental updates for efficiency

## Tech Stack
- Python (ETL)
- PostgreSQL
- SQL
- XML / JSON parsing

## Example Analysis
- Average delay per station
- Cancelled trains per time snapshot
- Nearest station (geo query)

## How to Run
```bash
pip install -r requirements.txt
python etl/load_stations.py
python etl/load_time.py
python etl/load_trains.py
python etl/load_fact_timetables.py
python etl/load_timetable_changes.py
