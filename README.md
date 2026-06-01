# Louisville Waterways Fishing — ETL Pipeline

An end-to-end ETL pipeline that pulls 7-day weather and river-flood forecasts
from [Open-Meteo](https://open-meteo.com/) for six fishing locations in the
Louisville, KY area, transforms them into a normalized schema, and loads the
results into a Supabase PostgreSQL database using **incremental upsert**
loading. The final fact table (`fishing_recommendation`) joins all sources,
scores each day at each location for fish activity, and flags safety status
(Safe / Caution / Unsafe) — ready to drive a Power BI report or Plotly Dash
dashboard.

The entire pipeline lives in a **single Python script**, `src/etl_pipeline.py`,
which executes start-to-finish without any manual modification.

---

## Repository structure

```
Louisville_Fishing_Recommendation_ETLPipeline/
├── .env                   # Your Supabase credentials — created from .env.example, gitignored
├── .gitignore             # Excludes .env, generated CSVs, venvs, etc.
├── .env.example           # Template for database credentials
├── README.md              # This file
├── requirements.txt       # Python dependencies
├── data/                  # Generated intermediate CSVs (created on first run, gitignored)
├── docs/
│   └── validation.md      # Data quality & validation framework write-up
├── src/
│   └── etl_pipeline.py    # The full ETL pipeline — extract, transform, validate, load
└── screenshots/           # Evidence of successful database loading
```

Note: `.env`, `data/`, and `screenshots/` are at the project root (siblings of
`src/`), not nested inside it. The script in `src/etl_pipeline.py` is
hard-wired to look one level up for `.env` and to create `data/` at the
project root regardless of where you run the script from.

---

## Pipeline stages

The single script `etl_pipeline.py` runs four sequential stages, each marked
with a `STAGE N/4` log banner:

| Stage | What it does |
|-------|--------------|
| 1/4 — Extract + Transform | Pulls weather + flood forecasts from Open-Meteo for each location, retries on transient failures, aggregates hourly readings to daily, derives `flow_category`, standardizes column names, and writes intermediate CSVs |
| 2/4 — Validation | Runs seven data-quality checks before any database write |
| 3/4 — Database Load | Creates the schema if needed and **upserts** (INSERT … ON CONFLICT DO UPDATE) into `weather_code`, `location`, `weather_data`, `river_flood_data` |
| 4/4 — Analytics-ready Fact Table | Builds `fishing_recommendation` with derived `safety_status` and `fish_activity_score` columns; upserts on `(location_id, date)` |

---

## Prerequisites

- **Python 3.12** (3.10+ should work; tested on 3.12)
- A **Supabase project** (free tier is fine) with the database password handy
- Internet access to reach `api.open-meteo.com` and `flood-api.open-meteo.com`

---

## Setup

### 1. Clone the repository

```bash
git clone <repo-url>
cd Louisville_Fishing_Recommendation_ETLPipeline
```

### 2. Create and activate a virtual environment (recommended)

```bash
# Windows (PowerShell)
python -m venv .venv
.venv\Scripts\activate

# macOS / Linux
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure database credentials

From the **project root** (the folder you `cd`'d into in step 1 — not
`src/`), copy `.env.example` to `.env` and fill in your Supabase connection
values:

```bash
# Windows
copy .env.example .env

# macOS / Linux
cp .env.example .env
```

Then edit `.env`:

```
user=postgres
password=YOUR_SUPABASE_PASSWORD
host=db.YOUR_PROJECT_REF.supabase.co
port=5432
dbname=postgres
RESET_TABLES=false
```

You can find these values in the Supabase dashboard under
**Project Settings → Database → Connection info**.

`RESET_TABLES` controls the load mode (see *Loading strategy* below).

---

## Running the pipeline

One command does everything:

```bash
python src/etl_pipeline.py
```

Expected output (abridged):

```
2026-05-30 09:00:00 [INFO] etl: ============================================================
2026-05-30 09:00:00 [INFO] etl: Louisville Waterways Fishing -- ETL Pipeline
2026-05-30 09:00:00 [INFO] etl: ============================================================
2026-05-30 09:00:00 [INFO] etl: STAGE 1/4: Extract + Transform (Open-Meteo APIs)
2026-05-30 09:00:00 [INFO] etl: Fetching: McAlpine Locks & Dam (location_id=1)
...
2026-05-30 09:00:05 [INFO] etl: STAGE 2/4: Data quality validation
2026-05-30 09:00:05 [INFO] etl: [weather_data] schema check passed (columns present: [...])
2026-05-30 09:00:05 [INFO] etl: [weather_data] null check passed on columns ['location_id', 'date']
2026-05-30 09:00:05 [INFO] etl: All validations passed (warnings, if any, listed above).
2026-05-30 09:00:05 [INFO] etl: STAGE 3/4: Database load
2026-05-30 09:00:05 [INFO] etl: INCREMENTAL mode -- preserving existing data, upserting on conflict.
2026-05-30 09:00:06 [INFO] etl: [weather_data] upserted 42 rows (insert + update)
...
2026-05-30 09:00:08 [INFO] etl: STAGE 4/4: Build analytics-ready fact table
2026-05-30 09:00:09 [INFO] etl: fishing_recommendation populated -- ready for Power BI / Plotly Dash. (42 analytics-ready rows)
2026-05-30 09:00:09 [INFO] etl: ETL PIPELINE COMPLETE
```

The script can be re-run as often as you want without manual edits or
duplicate data (see below).

---

## Loading strategy: incremental upsert

This pipeline pulls a **rolling 7-day forecast**, which means every re-run
produces overlapping date ranges with potentially-updated forecast values
for the same `(location_id, date)` pair. The natural fit is **incremental
upsert**, not full refresh.

Each load uses:

```sql
INSERT INTO <table> (...) VALUES (...)
ON CONFLICT (location_id, date) DO UPDATE SET col = EXCLUDED.col, ...
```

This one statement satisfies all three sub-requirements of incremental
loading:

| Requirement | How it's met |
|-------------|--------------|
| Prevent duplicate loads | `UNIQUE (location_id, date)` + `ON CONFLICT` clause |
| Append only new records | New date keys are inserted normally |
| Update existing records on key | Existing keys are overwritten via `DO UPDATE SET col = EXCLUDED.col` |

### Full refresh mode (opt-in)

If you ever need a fresh start (schema changes, corrupted state, etc.), set:

```
RESET_TABLES=true
```

in `.env` and re-run. The script will drop and recreate every table.
Default is `false` so re-runs don't lose history.

---

## Database schema

Five tables in the `public` schema (full details in
[`docs/validation.md`](docs/validation.md) and the schema doc):

| Table | Purpose |
| ----- | ------- |
| `weather_code` | WMO weather-code reference (code, description, icon, category) |
| `location` | Six Louisville-area fishing spots (lat/long) |
| `weather_data` | Daily weather per location (temp, wind, precip, pressure) |
| `river_flood_data` | Daily river discharge per location + `flow_category` |
| `fishing_recommendation` | Analytics-ready fact table with `safety_status` + `fish_activity_score` |

Foreign-key flow:

```
weather_code  <--  weather_data
location      <--  weather_data, river_flood_data, fishing_recommendation
weather_data  <--  fishing_recommendation
river_flood_data <-- fishing_recommendation
```

---

## Derived metrics

| Metric | Rule |
|--------|------|
| `flow_category` | Low < 100, Normal 100–1500, High > 1500 m³/s (Unknown if NULL) |
| `safety_status` | Unsafe if wind > 25 mph OR flow > 3000 m³/s; Caution if precip > 0.7 in; otherwise Safe |
| `fish_activity_score` | 0–100 composite of temp, wind, flow, precip (banded scoring; see code) |

---

## Data quality & validation

The pipeline runs **seven** validation checks before any data is written to
the database. See [`docs/validation.md`](docs/validation.md) for a full
write-up of what each check does, why it matters, and what happens if it
fails.

---

## Analytics consumption

The `fishing_recommendation` table is designed as a Power BI / Plotly Dash
endpoint. Sample query:

```sql
SELECT
  l.location_name,
  r.date,
  r.safety_status,
  r.fish_activity_score,
  w.temperature_f,
  w.wind_speed_mph,
  w.precipitation_in,
  f.discharge_m3s,
  f.flow_category
FROM fishing_recommendation r
JOIN location          l ON l.location_id = r.location_id
JOIN weather_data      w ON w.weather_id  = r.weather_id
JOIN river_flood_data  f ON f.flood_id    = r.flood_id
ORDER BY r.date, l.location_name;
```

This produces a denormalized row per location per day — exactly the grain a
dashboard wants.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `ModuleNotFoundError: No module named 'requests'` | Packages installed into a different Python than the one running the script | Run `python -m pip install -r requirements.txt` using the same Python you launch with |
| `Missing values in .env for: ...` | `.env` file not present or incomplete | Copy `.env.example` to `.env` and fill it in |
| `connection refused` / timeout | Wrong Supabase host or paused project | Verify host in Supabase dashboard |
| `password authentication failed` | Wrong password or special chars unescaped | Script URL-encodes the password automatically; check the value in `.env` |
| `Weather API failed after 3 attempts` | Open-Meteo outage or network blocked | Re-run after a few minutes |
| `No data fetched from Open-Meteo` | All locations failed | Check internet connection |

---

## License

This project was built for an academic ETL assignment and is intended for
educational use.
