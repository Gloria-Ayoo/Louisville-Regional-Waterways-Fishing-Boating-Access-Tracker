# Louisville Waterways Fishing — ETL Pipeline

An end-to-end ETL pipeline that pulls 7-day weather and river-flood forecasts
from [Open-Meteo](https://open-meteo.com/) for six fishing locations in the
Louisville, KY area, transforms them into a normalized schema, and loads the
results into a Supabase PostgreSQL database using **incremental upsert**
loading. The final fact table (`fishing_recommendation`) joins all sources,
scores each day at each location for fish activity, and flags safety status
(Safe / Caution / Unsafe) — ready to drive a Plotly Dash dashboard.

The entire pipeline lives in a **single Python script**, `src/etl_pipeline.py`,
which executes start-to-finish without any manual modification.

---

## Repository Structure

```
Louisville_Fishing_Recommendation_ETLPipeline/
├── .env                        # Your Supabase credentials — created from .env.example, gitignored
├── .env.example                # Template for database credentials
├── .gitignore                  # Excludes .env, generated CSVs, venvs, etc.
├── README.md                   # This file
├── requirements.txt            # Python dependencies
├── data/                       # Generated intermediate CSVs (created on first run, gitignored)
├── docs/
│   ├── validation.md           # Data quality & validation framework write-up
│   ├── data_schema.docx        # Full database schema documentation
│   └── proposal.docx           # Original project proposal
├── sql/
│   └── create_views.sql        # SQL view definitions for dashboard consumption
├── src/
│   ├── etl_pipeline.py         # The full ETL pipeline — extract, transform, validate, load
│   └── dashboard.py            # Plotly Dash web app — analytics consumption layer
└── screenshots/                # Evidence of successful database loading + dashboard demo
    ├── dashboard_overview.png
    ├── dashboard_heatmap.png
    ├── dashboard_map.png
    └── dashboard_table.png
```

> **Note:** `.env`, `data/`, and `screenshots/` are at the project root (siblings of
> `src/`). The script in `src/etl_pipeline.py` is hard-wired to look one level up
> for `.env` and to create `data/` at the project root regardless of where you run it from.

---

## Pipeline Stages

`etl_pipeline.py` runs four sequential stages, each marked with a `STAGE N/4` log banner:

| Stage | What it does |
|-------|--------------|
| **1/4 — Extract + Transform** | Pulls weather + flood forecasts from Open-Meteo for each location, retries on transient failures, aggregates hourly readings to daily, derives `flow_category`, standardizes column names, and writes intermediate CSVs |
| **2/4 — Validation** | Runs six data-quality checks (4 hard, 2 soft) before any database write |
| **3/4 — Database Load** | Creates the schema if needed and **upserts** (`INSERT … ON CONFLICT DO UPDATE`) into `weather_code`, `location`, `weather_data`, `river_flood_data` |
| **4/4 — Analytics-ready Fact Table** | Builds `fishing_recommendation` with derived `safety_status` and `fish_activity_score` columns; upserts on `(location_id, date)` |

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

From the **project root**, copy `.env.example` to `.env` and fill in your Supabase connection values:

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

`RESET_TABLES` controls the load mode — see *Loading Strategy* below.

---

## Running the Pipeline

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
2026-05-30 09:00:05 [INFO] etl: [weather_data] null check passed on columns ['location_id', 'date']
2026-05-30 09:00:05 [INFO] etl: All validations passed (warnings, if any, listed above).
2026-05-30 09:00:05 [INFO] etl: STAGE 3/4: Database load
2026-05-30 09:00:05 [INFO] etl: INCREMENTAL mode -- preserving existing data, upserting on conflict.
2026-05-30 09:00:06 [INFO] etl: [weather_data] upserted 42 rows (insert + update)
...
2026-05-30 09:00:08 [INFO] etl: STAGE 4/4: Build analytics-ready fact table
2026-05-30 09:00:09 [INFO] etl: fishing_recommendation populated -- 42 analytics-ready rows
2026-05-30 09:00:09 [INFO] etl: ETL PIPELINE COMPLETE
```

The script can be re-run as often as needed without manual edits or duplicate data.

---

## Loading Strategy: Incremental Upsert

This pipeline pulls a **rolling 7-day forecast**, meaning every re-run produces
overlapping date ranges with potentially-updated values for the same
`(location_id, date)` pair. The natural fit is **incremental upsert**, not full refresh.

Each load uses:

```sql
INSERT INTO <table> (...) VALUES (...)
ON CONFLICT (location_id, date) DO UPDATE SET col = EXCLUDED.col, ...
```

| Requirement | How it's met |
|-------------|--------------|
| Prevent duplicate loads | `UNIQUE (location_id, date)` + `ON CONFLICT` clause |
| Append only new records | New date keys are inserted normally |
| Update existing records on key match | Existing keys are overwritten via `DO UPDATE SET col = EXCLUDED.col` |

### Full Refresh Mode (opt-in)

To reset all tables (e.g. after a schema change), set `RESET_TABLES=true` in `.env`
and re-run. The script will drop and recreate every table. Default is `false`.

---

## Database Schema

Five tables in the `public` schema (full details in [`docs/data_schema.docx`](docs/data_schema.docx)):

| Table | Purpose |
|-------|---------|
| `weather_code` | WMO weather-code reference (code, description, icon, category) |
| `location` | Six Louisville-area fishing spots with GPS coordinates |
| `weather_data` | Daily weather per location (temp, wind, precip, pressure) |
| `river_flood_data` | Daily river discharge per location + `flow_category` |
| `fishing_recommendation` | Analytics-ready fact table with `safety_status` + `fish_activity_score` |

**Foreign-key flow:**

```
weather_code     <──  weather_data
location         <──  weather_data, river_flood_data, fishing_recommendation
weather_data     <──  fishing_recommendation
river_flood_data <──  fishing_recommendation
```

---

## Derived Metrics

| Metric | Rule |
|--------|------|
| `flow_category` | Low < 100, Normal 100–1500, High > 1500 m³/s (Unknown if NULL) |
| `safety_status` | Unsafe if wind > 25 mph OR flow > 3000 m³/s; Caution if precip > 0.7 in; otherwise Safe |
| `fish_activity_score` | 0–100 composite of temperature, wind, flow, and precipitation (banded scoring) |

---

## Data Quality & Validation

The pipeline runs **six** validation checks before any data is written to the database —
four **hard failures** (abort the pipeline) and two **soft failures** (log a warning and continue).

| # | Check | Stage | Failure Mode |
|---|-------|-------|-------------|
| 1 | API response validation | Extract | Hard (per location) |
| 2 | Null check on critical columns | Load | Hard |
| 3 | Duplicate detection on natural keys | Load | Hard |
| 4 | Referential integrity for `location_id` | Load | Hard |
| 5 | Range validation on numeric columns | Load | Soft (warn) |
| 6 | Row count verification per location | Load | Soft (warn) |

See [`docs/validation.md`](docs/validation.md) for a full write-up of each check.

---

## Running the Dashboard

The project ships with a Plotly Dash web application (`src/dashboard.py`) that
visualizes the `fishing_recommendation` fact table in the browser. It connects to
the same Supabase database the ETL writes to, so it always reflects current data.

### Steps

1. **Run the ETL first** — the dashboard reads from `fishing_recommendation`, so
   the warehouse must be populated:
   ```bash
   python src/etl_pipeline.py
   ```

2. **Create the analytics view** (one-time setup):
   ```bash
   # Run sql/create_views.sql against your Supabase database
   ```

3. **Launch the app:**
   ```bash
   python src/dashboard.py
   ```
   Then open **http://localhost:8050**. Stop with **Ctrl+C**.

The dashboard caches data in memory on launch for instant filter clicks.
Use the **Refresh data** button to re-query Postgres after a fresh ETL run.

### Dashboard Views

The app surfaces four panels designed to answer real questions a Louisville-area
angler would ask before heading out:

- **KPI summary** — Best Spot This Week, average safety split, top score
- **Activity heatmap** — location × date grid colored by `fish_activity_score` (0–100)
- **Safety distribution bar** — stacked Safe / Caution / Unsafe counts per location
- **Geographic map** — six spots plotted and colored by safety status for a selected date
- **Detail table** — sortable, filterable row-level data with safety-coded backgrounds

---

## Business Insights

| Question | Where to look |
|----------|---------------|
| When is it safe to fish? | Map + safety bar — Unsafe flags wind > 25 mph or flow > 3000 m³/s |
| Which spots have the best week-long outlook? | Activity heatmap — green rows = strong week |
| What's the single best place to go? | "Best Spot This Week" KPI card |
| How do conditions vary across locations on the same day? | Map date dropdown |

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `ModuleNotFoundError` | Packages installed into wrong Python | Run `python -m pip install -r requirements.txt` with the same Python you launch with |
| `Missing values in .env for: ...` | `.env` not present or incomplete | Copy `.env.example` to `.env` and fill it in |
| `connection refused` / timeout | Wrong Supabase host or paused project | Verify host in Supabase dashboard |
| `password authentication failed` | Wrong password | Script URL-encodes automatically; check the value in `.env` |
| `Weather API failed after 3 attempts` | Open-Meteo outage or network blocked | Re-run after a few minutes |
| `No data fetched from Open-Meteo` | All locations failed | Check internet connection |

---

## License

Built for an academic data engineering course assignment. Intended for educational use.
