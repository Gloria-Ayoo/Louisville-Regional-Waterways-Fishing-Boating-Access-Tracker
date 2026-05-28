# Louisville Regional Waterways — Fishing Recommendation System

An ETL pipeline that forecasts fishing conditions at Louisville-area waterway access points by combining weather and river-flood data from Open-Meteo, then writes the results into a Supabase Postgres warehouse.

## What it does

For 6 fishing locations around Louisville, the pipeline:

1. Pulls hourly weather + daily river-discharge forecasts from Open-Meteo (7-day window).
2. Aggregates hourly readings into one row per day per location.
3. Classifies river flow (Low / Normal / High) and computes a fish activity score (0–100) and safety status (Safe / Caution / Unsafe) per location per day.
4. Loads everything into Supabase across 5 normalized tables (3NF).

## Folder structure

```
Fishing_recom_sys_/
├── src/
│   ├── extract_script.py     # Pulls from Open-Meteo, writes CSVs
│   └── load_script.py        # Reads CSVs, loads Supabase, builds recommendations
├── data/                     # Generated CSVs (gitignored)
│   ├── weather_codes.csv
│   ├── weather_data.csv
│   └── river_flood_data.csv
├── .env                      # Database credentials (gitignored)
├── .env.example              # Template for .env
├── .gitignore
├── requirements.txt
└── README.md
```

## Setup

### 1. Clone and install

```bash
git clone <repo-url>
cd Fishing_recom_sys_
python -m venv venv
# Windows:
venv\Scripts\activate
# macOS/Linux:
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Create your `.env`

Copy `.env.example` to `.env` and fill in your Supabase credentials:

```
user=postgres
password=<your_supabase_database_password>
host=db.<your_project_ref>.supabase.co
port=5432
dbname=postgres
```

Find these in the Supabase dashboard:
- **password**: Project Settings → Database → Database password
- **host**: Project Settings → Database → Host

### 3. Run the pipeline

```bash
cd src
python extract_script.py    # Step 1: pull from Open-Meteo, write CSVs
python load_script.py       # Step 2: load CSVs into Supabase
```

The first run will create the schema. By default, `load_script.py` drops and recreates all tables on each run (set `RESET_TABLES=false` in `.env` to disable).

## Database schema

5 tables in approximate 3NF:

| Table | Purpose |
|---|---|
| `weather_code` | WMO weather code lookup |
| `location` | Fishing access points (parks, ramps, docks) |
| `weather_data` | Daily weather per location |
| `river_flood_data` | Daily river discharge per location |
| `fishing_recommendation` | Fact table: safety status + fish activity score |

See `data_schema_doc_2_Fishing_Recommendation_System_Data_Warehouse.docx` for full ER diagram and column definitions.

## Data quality & validation

The pipeline runs **6 validation checks**. Validation failures log a warning and the pipeline stops before writing bad data to the warehouse.

### 1. API response validation (in extract)
- **What:** Verifies each Open-Meteo response contains the expected `hourly` or `daily` keys before parsing.
- **Why:** Open-Meteo may return error responses with no data block for invalid coordinates or temporary outages. Parsing a malformed response silently produces empty tables.
- **On failure:** That location is skipped, logged as a warning, pipeline continues with remaining locations.

### 2. Null value check (in load)
- **What:** Counts nulls in critical not-null columns (`location_id`, `date`) before insert.
- **Why:** A null in a key column breaks foreign-key joins downstream and corrupts the recommendation fact table.
- **On failure:** Pipeline aborts with the offending row count logged.

### 3. Duplicate detection (in load)
- **What:** Checks that `(location_id, date)` is unique in `weather_data` and `river_flood_data`.
- **Why:** The schema enforces a `UNIQUE (location_id, date)` constraint; duplicates would crash the insert mid-load and leave partial data.
- **On failure:** Pipeline aborts with a list of duplicate keys.

### 4. Referential integrity check (in load)
- **What:** Confirms every `location_id` in `weather_data.csv` and `river_flood_data.csv` exists in the hardcoded `LOCATIONS` seed.
- **Why:** An unknown `location_id` would violate the foreign key and crash the insert.
- **On failure:** Pipeline aborts and prints the offending `location_id` values.

### 5. Range validation (in load)
- **What:** Asserts numeric values fall within plausible bounds — temperature_f between -50 and 130, wind_speed_mph between 0 and 200, discharge_m3s ≥ 0, fish_activity_score between 0 and 100.
- **Why:** Catches sensor glitches or unit-conversion bugs (e.g. an API change silently switching from mph to m/s) before they reach analysts.
- **On failure:** Logs a warning per out-of-range row; pipeline continues but flags the data for review.

### 6. Row count verification (in load)
- **What:** Confirms each location produced exactly `FORECAST_DAYS` rows in `weather_data` and `river_flood_data`.
- **Why:** Partial data per location means the recommendation fact table will be incomplete and dashboard charts will have gaps.
- **On failure:** Logs a warning naming the under/over-counted locations; pipeline continues so partial data is still usable.

## Logging & error handling

- All scripts use Python's `logging` module (INFO level by default).
- Logs print to console with timestamps, level, and source function.
- Open-Meteo API calls retry up to 3 times with exponential backoff on transient failures (timeout, 5xx, connection errors).
- Database operations are wrapped in transactions — a failure rolls back rather than leaving partial data.

## Notes

- **Authentication:** Open-Meteo is keyless, so no auth is implemented.
- **Pagination:** Not applicable — the 7-day forecast fits in a single response.
- **Flow categorization, safety status, and fish activity score** are computed in the load script (not extract), so all classification rules live in one place.
- **`fishing_recommendation` is built from the database**, not the CSVs — the load script reads back the `BIGSERIAL` IDs Supabase generates for `weather_data` and `river_flood_data`, then joins them on `(location_id, date)` to populate the fact table.
