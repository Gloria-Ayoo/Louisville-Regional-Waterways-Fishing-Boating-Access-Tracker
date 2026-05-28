"""
Supabase PostgreSQL ETL Loader — Fishing Recommendation System
--------------------------------------------------------------
Creates the PostgreSQL schema in Supabase and loads the following CSVs
produced by the upstream extract/transform script:

- data/weather_codes.csv
- data/weather_data.csv
- data/river_flood_data.csv

Locations are hardcoded as seed data in this script (see LOCATIONS below).

This script does NOT call Open-Meteo. The extract/transform script handles
all API extraction and aggregation. This loader:
  1. connects to Supabase
  2. (optionally) drops and recreates the 5-table schema
  3. reads the CSVs and the hardcoded LOCATIONS seed list
  4. computes flow_category from raw discharge_m3s
  5. writes weather_code, location, weather_data, river_flood_data
  6. computes fishing_recommendation (safety status + fish activity score)
     from the loaded weather + flood data and writes it as the final fact table

Required packages:
    pip install pandas sqlalchemy psycopg2-binary python-dotenv

.env values expected (standard Postgres variable names):
    user=postgres
    password=your_supabase_database_password
    host=db.your_project_ref.supabase.co
    port=5432
    dbname=postgres

Optional:
    RESET_TABLES=true
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.types import Date, Integer, Numeric, String, BigInteger


# Logging configuration
# Console output with timestamp + level + module so issues across the
# extract/load pipeline are easy to correlate.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("load")


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"

WEATHER_CODES_CSV = DATA_DIR / "weather_codes.csv"
WEATHER_DATA_CSV = DATA_DIR / "weather_data.csv"
RIVER_FLOOD_CSV = DATA_DIR / "river_flood_data.csv"


# Seed data — Louisville-area fishing locations
# (location_id, location_name, city, latitude, longitude)
# Kept here rather than in a CSV because the location set is small,
# stable, and is the upstream reference the extract script also reads.
LOCATIONS = [
    (1, "McAlpine Locks & Dam",             "Louisville",   38.2742, -85.7984),
    (2, "Cox Park Boat Ramp",               "Louisville",   38.3017, -85.6483),
    (3, "Riverview Park",                   "Louisville",   38.1531, -85.8736),
    (4, "Taylorsville Lake",                "Taylorsville", 38.0322, -85.2380),
    (5, "Floyds Fork - Beckley Creek Park", "Louisville",   38.2317, -85.5147),
    (6, "Otter Creek Outdoor Recreation",   "Brandenburg",  37.9425, -86.0656),
]


# ---------------------------------------------------------------------
# Connection configuration
# ---------------------------------------------------------------------
# Builds the SQLAlchemy database connection URL from standard Postgres
# environment variables in .env:
#   user=postgres
#   password=<your_supabase_db_password>
#   host=db.<your_project_ref>.supabase.co
#   port=5432
#   dbname=postgres
# URL-encodes the password so special characters (e.g. @, /, :, #) don't
# break the connection string.
# ---------------------------------------------------------------------
def get_database_url() -> str:
    load_dotenv()

    user = os.getenv("user")
    password = os.getenv("password")
    host = os.getenv("host")
    port = os.getenv("port", "5432")
    dbname = os.getenv("dbname")

    missing = [
        name
        for name, value in {
            "user": user,
            "password": password,
            "host": host,
            "dbname": dbname,
        }.items()
        if not value
    ]
    if missing:
        raise RuntimeError(
            f"Missing values in .env for: {', '.join(missing)}. "
            "Expected user, password, host, port, dbname."
        )

    safe_password = quote_plus(password)

    return f"postgresql+psycopg2://{user}:{safe_password}@{host}:{port}/{dbname}"


def table_reset_enabled() -> bool:
    # Allow users to choose whether to drop existing tables before loading data.
    # Default is true so repeated runs do not append duplicate forecast rows.
    return os.getenv("RESET_TABLES", "true").strip().lower() in {"1", "true", "yes", "y"}


# ---------------------------------------------------------------------
# Schema creation
# ---------------------------------------------------------------------
# Defines the 5 tables from data_schema_doc_2 and their relationships.
# Drops in child-to-parent order, then creates in parent-to-child order.
# Every fact-style table has a UNIQUE natural key so reruns with
# RESET_TABLES=false do not append duplicates.
# ---------------------------------------------------------------------
def create_schema(engine) -> None:
    drop_sql = """
    DROP TABLE IF EXISTS public.fishing_recommendation CASCADE;
    DROP TABLE IF EXISTS public.river_flood_data CASCADE;
    DROP TABLE IF EXISTS public.weather_data CASCADE;
    DROP TABLE IF EXISTS public.location CASCADE;
    DROP TABLE IF EXISTS public.weather_code CASCADE;
    """

    create_sql = """
    CREATE TABLE IF NOT EXISTS public.weather_code (
        code        INTEGER PRIMARY KEY,
        description VARCHAR(100) NOT NULL,
        icon        VARCHAR(10),
        category    VARCHAR(30)
    );

    CREATE TABLE IF NOT EXISTS public.location (
        location_id   INTEGER PRIMARY KEY,
        location_name VARCHAR(120) NOT NULL,
        city          VARCHAR(80),
        latitude      DECIMAL(9,6) NOT NULL,
        longitude     DECIMAL(9,6) NOT NULL
    );

    CREATE TABLE IF NOT EXISTS public.weather_data (
        weather_id       BIGSERIAL PRIMARY KEY,
        location_id      INTEGER NOT NULL REFERENCES public.location(location_id),
        date             DATE NOT NULL,
        weather_code     INTEGER REFERENCES public.weather_code(code),
        temperature_f    DECIMAL(5,2),
        wind_speed_mph   DECIMAL(5,2),
        precipitation_in DECIMAL(4,2),
        pressure_hpa     DECIMAL(6,2),
        UNIQUE (location_id, date)
    );

    CREATE TABLE IF NOT EXISTS public.river_flood_data (
        flood_id      BIGSERIAL PRIMARY KEY,
        location_id   INTEGER NOT NULL REFERENCES public.location(location_id),
        date          DATE NOT NULL,
        discharge_m3s DECIMAL(10,2),
        flow_category VARCHAR(30),
        UNIQUE (location_id, date)
    );

    CREATE TABLE IF NOT EXISTS public.fishing_recommendation (
        recommendation_id   BIGSERIAL PRIMARY KEY,
        location_id         INTEGER NOT NULL REFERENCES public.location(location_id),
        weather_id          BIGINT  NOT NULL REFERENCES public.weather_data(weather_id),
        flood_id            BIGINT  NOT NULL REFERENCES public.river_flood_data(flood_id),
        date                DATE    NOT NULL,
        safety_status       VARCHAR(20),
        fish_activity_score INTEGER,
        UNIQUE (location_id, date)
    );
    """

    with engine.begin() as conn:
        if table_reset_enabled():
            log.info("RESET_TABLES is enabled — dropping existing tables...")
            conn.execute(text(drop_sql))
        else:
            log.info("RESET_TABLES is disabled — keeping existing tables.")
        conn.execute(text(create_sql))


# ---------------------------------------------------------------------
# Source ingestion
# ---------------------------------------------------------------------
# Reads the CSVs produced by the extract/transform script and builds
# the location DataFrame from the hardcoded LOCATIONS seed list.
# Raises a clear error if any CSV is missing so a forgotten extract run
# fails fast with a useful message.
# ---------------------------------------------------------------------
def load_source_files() -> tuple[
    pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame
]:
    missing_files = [p for p in (WEATHER_CODES_CSV, WEATHER_DATA_CSV, RIVER_FLOOD_CSV) if not p.exists()]
    if missing_files:
        raise FileNotFoundError(
            "Missing input CSV(s): "
            + ", ".join(str(p) for p in missing_files)
            + ". Run extract.py first."
        )

    weather_code_df = pd.read_csv(WEATHER_CODES_CSV)
    weather_data_df = pd.read_csv(WEATHER_DATA_CSV)
    river_flood_df = pd.read_csv(RIVER_FLOOD_CSV)

    location_df = pd.DataFrame(
        LOCATIONS,
        columns=["location_id", "location_name", "city", "latitude", "longitude"],
    )

    log.info("Source CSV files loaded successfully.")
    return (
        weather_code_df,
        location_df,
        weather_data_df,
        river_flood_df,
    )


# ---------------------------------------------------------------------
# Flow classification (DERIVED METRIC)
# ---------------------------------------------------------------------
# Reference rules from data_schema_doc_2:
#   Low:     discharge < 100 m³/s
#   Normal:  100 - 1500 m³/s
#   High:    > 1500 m³/s
# Missing values are labeled "Unknown" so the column never holds NULL.
# ---------------------------------------------------------------------
def classify_flow(discharge) -> str:
    if discharge is None or pd.isna(discharge):
        return "Unknown"
    if discharge < 100:
        return "Low"
    if discharge <= 1500:
        return "Normal"
    return "High"


# ---------------------------------------------------------------------
# Light type cleanup and derived columns
# ---------------------------------------------------------------------
# The extract script produces raw discharge values; flow_category is derived
# here so the categorization logic lives in one place with the rest of the
# recommendation rules.
# ---------------------------------------------------------------------
def prepare_frames(
    weather_data_df: pd.DataFrame,
    river_flood_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    weather_data_df = weather_data_df.copy()
    weather_data_df["date"] = pd.to_datetime(weather_data_df["date"]).dt.date

    river_flood_df = river_flood_df.copy()
    river_flood_df["date"] = pd.to_datetime(river_flood_df["date"]).dt.date
    river_flood_df["flow_category"] = river_flood_df["discharge_m3s"].apply(classify_flow)

    return weather_data_df, river_flood_df


# =====================================================================
# DATA QUALITY & VALIDATION
# =====================================================================
# Seven checks total run before any data hits Supabase:
#   1. API response validation        (in extract.py)
#   2. Schema/type validation         (this file)
#   3. Null value check               (this file)
#   4. Duplicate detection            (this file)
#   5. Referential integrity          (this file)
#   6. Range validation               (this file)
#   7. Row count verification         (this file)
#
# Hard failures (schema, nulls, duplicates, referential integrity) raise
# and stop the pipeline so bad data is never written. Soft failures
# (range, row count) log warnings but allow the load to proceed with the
# data that's good.
# =====================================================================

# Expected number of forecast days per location.
# Open-Meteo returns 7 days, so each location should produce 7 rows in
# both weather_data and river_flood_data. Tweak if FORECAST_DAYS changes
# in the extract script.
EXPECTED_FORECAST_DAYS = 7


def validate_schema(df: pd.DataFrame, name: str, expected_columns: list[str]) -> None:
    """
    VALIDATION CHECK 2: Schema / type validation.
    Why: confirms that the CSV produced by the extract step actually has
    the columns the load step expects. Catches accidental column renames
    or upstream code changes before they manifest as KeyError far away
    from the cause.
    On failure: raises and aborts the pipeline.
    """
    missing = [c for c in expected_columns if c not in df.columns]
    extra = [c for c in df.columns if c not in expected_columns]
    if missing:
        raise ValueError(
            f"[{name}] schema check failed — missing columns: {missing}; "
            f"got: {list(df.columns)}"
        )
    if extra:
        # Extra columns are not a hard failure (they're ignored by to_sql with
        # explicit dtype maps), but they are worth logging.
        log.warning("[%s] schema has extra columns that will be ignored: %s", name, extra)
    log.info("[%s] schema check passed (columns present: %s)", name, expected_columns)


def validate_null_critical_columns(df: pd.DataFrame, name: str, columns: list[str]) -> None:
    """
    VALIDATION CHECK 3: Null value check on key columns.
    Why: a null in location_id or date breaks foreign keys and corrupts
    the recommendation fact table downstream.
    On failure: raises and aborts the pipeline.
    """
    for col in columns:
        if col not in df.columns:
            raise ValueError(f"[{name}] expected column '{col}' is missing")
        null_count = int(df[col].isna().sum())
        if null_count:
            raise ValueError(
                f"[{name}] {null_count} null value(s) in critical column '{col}'"
            )
    log.info("[%s] null check passed on columns %s", name, columns)


def validate_no_duplicates(df: pd.DataFrame, name: str, keys: list[str]) -> None:
    """
    VALIDATION CHECK 4: Duplicate detection on the natural key.
    Why: weather_data and river_flood_data enforce UNIQUE (location_id, date).
    Duplicates would crash the insert mid-load and leave partial data.
    On failure: raises and aborts the pipeline.
    """
    dups = df[df.duplicated(subset=keys, keep=False)]
    if not dups.empty:
        raise ValueError(
            f"[{name}] {len(dups)} duplicate row(s) on {keys}:\n{dups.head(10)}"
        )
    log.info("[%s] duplicate check passed on keys %s", name, keys)


def validate_referential_integrity(df: pd.DataFrame, name: str, valid_ids: set[int]) -> None:
    """
    VALIDATION CHECK 5: Referential integrity for location_id.
    Why: an unknown location_id would violate the FK constraint when the
    row hits Supabase and the insert would error.
    On failure: raises and aborts the pipeline.
    """
    unknown = set(df["location_id"].unique()) - valid_ids
    if unknown:
        raise ValueError(
            f"[{name}] {len(unknown)} unknown location_id value(s): {sorted(unknown)}"
        )
    log.info("[%s] referential integrity check passed (location_id)", name)


def validate_value_ranges(df: pd.DataFrame, name: str, rules: dict[str, tuple[float, float]]) -> None:
    """
    VALIDATION CHECK 6: Range validation on numeric columns.
    Why: catches sensor glitches or unit-conversion bugs (e.g. an API
    change silently switching wind from mph to m/s) before they pollute
    the warehouse.
    On failure: logs a warning per out-of-range column; pipeline continues
    so good data still loads.
    """
    for col, (lo, hi) in rules.items():
        if col not in df.columns:
            continue
        bad_mask = df[col].notna() & ((df[col] < lo) | (df[col] > hi))
        bad_count = int(bad_mask.sum())
        if bad_count:
            log.warning(
                "[%s] %d row(s) out of range on '%s' (expected %s..%s)",
                name, bad_count, col, lo, hi,
            )
    log.info("[%s] range check completed", name)


def validate_row_count_per_location(
    df: pd.DataFrame, name: str, expected_per_location: int
) -> None:
    """
    VALIDATION CHECK 7: Row count verification per location.
    Why: partial data per location means the recommendation fact table
    will have gaps and dashboards will be misleading.
    On failure: logs a warning naming the under/over-counted locations;
    pipeline continues so partial data is still usable.
    """
    counts = df.groupby("location_id").size()
    off = counts[counts != expected_per_location]
    if not off.empty:
        log.warning(
            "[%s] expected %d rows per location; got: %s",
            name, expected_per_location, off.to_dict(),
        )
    else:
        log.info(
            "[%s] row count check passed (%d rows per location)",
            name, expected_per_location,
        )


def run_validations(
    location_df: pd.DataFrame,
    weather_data_df: pd.DataFrame,
    river_flood_df: pd.DataFrame,
) -> None:
    """Run all pre-load validation checks. Hard failures raise."""
    log.info("Running data quality validations...")

    valid_location_ids = set(location_df["location_id"].astype(int))

    # weather_data
    validate_schema(
        weather_data_df,
        "weather_data",
        [
            "location_id",
            "date",
            "weather_code",
            "temperature_f",
            "wind_speed_mph",
            "precipitation_in",
            "pressure_hpa",
        ],
    )
    validate_null_critical_columns(weather_data_df, "weather_data", ["location_id", "date"])
    validate_no_duplicates(weather_data_df, "weather_data", ["location_id", "date"])
    validate_referential_integrity(weather_data_df, "weather_data", valid_location_ids)
    validate_value_ranges(
        weather_data_df,
        "weather_data",
        {
            "temperature_f":    (-50.0, 130.0),
            "wind_speed_mph":   (0.0, 200.0),
            "precipitation_in": (0.0, 30.0),
            "pressure_hpa":     (800.0, 1100.0),
        },
    )
    validate_row_count_per_location(weather_data_df, "weather_data", EXPECTED_FORECAST_DAYS)

    # river_flood_data
    validate_schema(
        river_flood_df,
        "river_flood_data",
        ["location_id", "date", "discharge_m3s", "flow_category"],
    )
    validate_null_critical_columns(river_flood_df, "river_flood_data", ["location_id", "date"])
    validate_no_duplicates(river_flood_df, "river_flood_data", ["location_id", "date"])
    validate_referential_integrity(river_flood_df, "river_flood_data", valid_location_ids)
    validate_value_ranges(
        river_flood_df,
        "river_flood_data",
        {"discharge_m3s": (0.0, 100000.0)},
    )
    validate_row_count_per_location(river_flood_df, "river_flood_data", EXPECTED_FORECAST_DAYS)

    log.info("All validations passed (warnings, if any, listed above).")


# ---------------------------------------------------------------------
# Data loading helper
# ---------------------------------------------------------------------
# Writes a DataFrame to the target PostgreSQL table using SQLAlchemy.
# Wrapped so error messages clearly name which table failed.
# ---------------------------------------------------------------------
def write_table(df: pd.DataFrame, table_name: str, engine, dtype: dict) -> None:
    log.info("Loading %s table (%d rows)...", table_name, len(df))
    try:
        df.to_sql(
            table_name,
            engine,
            schema="public",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
            dtype=dtype,
        )
    except Exception as e:
        log.error("Failed to load %s: %s", table_name, e)
        raise


# ---------------------------------------------------------------------
# Table loader
# ---------------------------------------------------------------------
# Loads the four input DataFrames into the matching tables in FK-safe
# parent-to-child order:
#   weather_code -> location -> weather_data -> river_flood_data
# fishing_recommendation is built and loaded separately (see below) because
# it depends on the BIGSERIAL weather_id and flood_id values that Supabase
# generates during this step.
# ---------------------------------------------------------------------
def load_tables(
    engine,
    weather_code_df: pd.DataFrame,
    location_df: pd.DataFrame,
    weather_data_df: pd.DataFrame,
    river_flood_df: pd.DataFrame,
) -> None:
    write_table(
        weather_code_df,
        "weather_code",
        engine,
        {
            "code": Integer(),
            "description": String(length=100),
            "icon": String(length=10),
            "category": String(length=30),
        },
    )
    write_table(
        location_df,
        "location",
        engine,
        {
            "location_id": Integer(),
            "location_name": String(length=120),
            "city": String(length=80),
            "latitude": Numeric(9, 6),
            "longitude": Numeric(9, 6),
        },
    )
    write_table(
        weather_data_df,
        "weather_data",
        engine,
        {
            "location_id": Integer(),
            "date": Date(),
            "weather_code": Integer(),
            "temperature_f": Numeric(5, 2),
            "wind_speed_mph": Numeric(5, 2),
            "precipitation_in": Numeric(4, 2),
            "pressure_hpa": Numeric(6, 2),
        },
    )
    write_table(
        river_flood_df,
        "river_flood_data",
        engine,
        {
            "location_id": Integer(),
            "date": Date(),
            "discharge_m3s": Numeric(10, 2),
            "flow_category": String(length=30),
        },
    )


# ---------------------------------------------------------------------
# Recommendation scoring (DERIVED METRIC)
# ---------------------------------------------------------------------
# Buckets per spec:
#   Temperature  -> 30 / 15 / 5
#   Wind         -> 20 / 10 / 0
#   River flow   -> 30 / 15 / 0
#   Rain penalty -> -20 / 0
# ---------------------------------------------------------------------
def fish_activity_score(temp_f, wind_mph, discharge, precip_in) -> int:
    # Temperature (Fahrenheit, fish-friendly band 60-80)
    if temp_f is None or pd.isna(temp_f):
        t = 5
    elif 60 <= temp_f <= 80:
        t = 30
    elif 50 <= temp_f < 60 or 80 < temp_f <= 90:
        t = 15
    else:
        t = 5

    # Wind (calm preferred)
    if wind_mph is None or pd.isna(wind_mph):
        w = 0
    elif wind_mph < 10:
        w = 20
    elif wind_mph < 20:
        w = 10
    else:
        w = 0

    # River flow (moderate flow preferred)
    if discharge is None or pd.isna(discharge):
        f = 0
    elif 200 <= discharge <= 1500:
        f = 30
    elif 50 <= discharge < 200 or 1500 < discharge <= 2500:
        f = 15
    else:
        f = 0

    # Rain penalty (note: distinct from Safety Caution threshold of > 0.7 in)
    p = -20 if (precip_in is not None and not pd.isna(precip_in) and precip_in > 0.5) else 0

    return max(0, t + w + f + p)


# ---------------------------------------------------------------------
# Safety classification (DERIVED METRIC)
# ---------------------------------------------------------------------
#   wind > 25 mph OR discharge > 3000  -> Unsafe
#   precipitation > 0.7 in             -> Caution
#   else                               -> Safe
# ---------------------------------------------------------------------
def safety_status(wind_mph, discharge, precip_in) -> str:
    wind_bad = wind_mph is not None and not pd.isna(wind_mph) and wind_mph > 25
    flow_bad = discharge is not None and not pd.isna(discharge) and discharge > 3000
    if wind_bad or flow_bad:
        return "Unsafe"
    if precip_in is not None and not pd.isna(precip_in) and precip_in > 0.7:
        return "Caution"
    return "Safe"


# ---------------------------------------------------------------------
# Recommendation builder + loader
# ---------------------------------------------------------------------
# Reads back the generated BIGSERIAL keys from weather_data and river_flood_data,
# joins them with the source data on (location_id, date), computes safety status
# and fish activity score for each row, and writes the fact table.
# ---------------------------------------------------------------------
def build_and_load_recommendations(
    engine,
    weather_data_df: pd.DataFrame,
    river_flood_df: pd.DataFrame,
) -> None:
    log.info("Reading back generated weather_id and flood_id values...")
    try:
        weather_keys = pd.read_sql(
            "SELECT weather_id, location_id, date FROM public.weather_data",
            engine,
        )
        flood_keys = pd.read_sql(
            "SELECT flood_id, location_id, date FROM public.river_flood_data",
            engine,
        )
    except Exception as e:
        log.error("Failed to read back generated keys: %s", e)
        raise

    # Make sure all join keys share the same Python date type
    weather_keys["date"] = pd.to_datetime(weather_keys["date"]).dt.date
    flood_keys["date"] = pd.to_datetime(flood_keys["date"]).dt.date

    # Join: weather metrics + weather_id + flood metrics + flood_id, all on (location_id, date)
    merged = (
        weather_data_df.merge(weather_keys, on=["location_id", "date"], how="inner")
        .merge(river_flood_df, on=["location_id", "date"], how="inner")
        .merge(flood_keys, on=["location_id", "date"], how="inner")
    )

    if merged.empty:
        log.warning("No matching weather + flood rows found; skipping recommendations.")
        return

    # Compute scoring columns row-by-row.
    merged["fish_activity_score"] = merged.apply(
        lambda r: fish_activity_score(
            r.get("temperature_f"),
            r.get("wind_speed_mph"),
            r.get("discharge_m3s"),
            r.get("precipitation_in"),
        ),
        axis=1,
    ).astype(int)

    merged["safety_status"] = merged.apply(
        lambda r: safety_status(
            r.get("wind_speed_mph"),
            r.get("discharge_m3s"),
            r.get("precipitation_in"),
        ),
        axis=1,
    )

    recommendation_df = merged[
        [
            "location_id",
            "weather_id",
            "flood_id",
            "date",
            "safety_status",
            "fish_activity_score",
        ]
    ].copy()

    write_table(
        recommendation_df,
        "fishing_recommendation",
        engine,
        {
            "location_id": Integer(),
            "weather_id": BigInteger(),
            "flood_id": BigInteger(),
            "date": Date(),
            "safety_status": String(length=20),
            "fish_activity_score": Integer(),
        },
    )


# ---------------------------------------------------------------------
# Main workflow orchestration
# ---------------------------------------------------------------------
# Ties the load steps together: connect, read CSVs, prepare frames,
# (re)create schema, load tables in FK-safe order, then compute and load
# the fishing_recommendation fact table.
# ---------------------------------------------------------------------
def main() -> None:
    engine = create_engine(get_database_url())

    (
        weather_code_df,
        location_df,
        weather_data_df,
        river_flood_df,
    ) = load_source_files()

    weather_data_df, river_flood_df = prepare_frames(weather_data_df, river_flood_df)

    # Validation gate — runs all data quality checks before writing anything.
    # Hard failures raise here and the script exits with no database changes.
    run_validations(location_df, weather_data_df, river_flood_df)

    log.info("Creating Supabase PostgreSQL schema...")
    create_schema(engine)

    load_tables(
        engine,
        weather_code_df,
        location_df,
        weather_data_df,
        river_flood_df,
    )

    log.info("Building fishing_recommendation fact table...")
    build_and_load_recommendations(engine, weather_data_df, river_flood_df)

    log.info("=" * 35)
    log.info("ETL LOAD COMPLETE")
    log.info("=" * 35)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.exception("Load pipeline failed: %s", e)
        raise