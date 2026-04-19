# Berlin Public Transport Data Integration & Analytics

A data engineering project that builds a PostgreSQL-based analytical warehouse for Berlin public transport data. The project integrates planned timetable data and real-time timetable updates using a modular Python ETL pipeline, enabling large-scale analysis of train movements, delays, cancellations, and station-level patterns.

## Project Overview

This project was developed for the **Data Integration and Large Scale Analysis** course. It focuses on ingesting heterogeneous transport datasets, transforming them into a structured analytical model, and running SQL-based analysis on the integrated data.

The system uses a **star schema** with one fact table and multiple dimension tables:

- **fact_train_movement**: stores train arrival and departure events
- **dim_station**: station metadata such as name, EVA number, and coordinates
- **dim_train**: train-related attributes such as operator, category, and line
- **dim_time**: normalized time dimension for temporal analysis

## Features

- Designed and implemented a **star schema data warehouse**
- Built a **modular ETL pipeline in Python**
- Parsed **large-scale XML and JSON datasets**
- Loaded planned timetable events into PostgreSQL
- Integrated **real-time timetable changes** including delays and cancellations
- Applied **data cleaning and normalization** to resolve inconsistent station names
- Supported analytical SQL queries for:
  - average delay per station
  - cancelled train counts at specific snapshots
  - nearest station by geographic coordinates
  - station metadata lookup

## Tech Stack

- **Python**
- **PostgreSQL**
- **SQL**
- **XML / JSON**
- **psycopg2**

## Example Analysis
- Average delay per station
- Cancelled trains per time snapshot
- Nearest station (geo query)

## How to Run
python etl/load_stations.py
python etl/load_time.py
python etl/load_trains.py
python etl/load_fact_timetables.py
python etl/load_timetable_changes.py
