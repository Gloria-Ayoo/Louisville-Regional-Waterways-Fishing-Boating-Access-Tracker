"""
Louisville Waterways Fishing — End-to-End ETL Pipeline

Single-file ETL: extracts weather + river-flood forecasts from Open-Meteo
for six Louisville-area fishing spots, validates and transforms them, and
upserts the result into Supabase PostgreSQL. The fishing_recommendation
fact table is the analytics-ready endpoint consumed by the dashboard
(via the vw_fishing_outlook view, created separately via sql/create_views.sql).

Run:  python src/etl_pipeline.py

Env (.env at project root):
    user / password / host / port / dbname  -> Supabase connection
    RESET_TABLES=false (default)             -> incremental upsert
    RESET_TABLES=true                        -> drop and recreate
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("etl")


# ---------- Constants ----------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"

FORECAST_API_URL = "https://api.open-meteo.com/v1/forecast"
FLOOD_API_URL = "https://flood-api.open-meteo.com/v1/flood"
TIMEZONE = "America/New_York"
FORECAST_DAYS = 7
EXPECTED_FORECAST_DAYS = FORECAST_DAYS

MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2

# (location_id, location_name, city, latitude, longitude)
LOCATIONS = [
    (1, "McAlpine Locks & Dam",             "Louisville",   38.2742, -85.7984),
    (2, "Cox Park Boat Ramp",               "Louisville",   38.3017, -85.6483),
    (3, "Riverview Park",                   "Louisville",   38.1531, -85.8736),
    (4, "Taylorsville Lake",                "Taylorsville", 38.0322, -85.2380),
    (5, "Floyds Fork - Beckley Creek Park", "Louisville",   38.2317, -85.5147),
    (6, "Otter Creek Outdoor Recreation",   "Brandenburg",  37.9425, -86.0656),
]

# WMO weather codes (code, description, icon, category)
WEATHER_CODES = [
    (0, "Clear sky", "\u2600\ufe0f", "Clear"),
    (1, "Mainly clear", "\U0001F324\ufe0f", "Clear"),
    (2, "Partly cloudy", "\u26C5", "Cloudy"),
    (3, "Overcast", "\u2601\ufe0f", "Cloudy"),
    (45, "Fog", "\U0001F32B\ufe0f", "Fog"),
    (48, "Depositing rime fog", "\U0001F32B\ufe0f", "Fog"),
    (51, "Light drizzle", "\U0001F327\ufe0f", "Rain"),
    (53, "Moderate drizzle", "\U0001F327\ufe0f", "Rain"),
    (55, "Dense drizzle", "\U0001F327\ufe0f", "Rain"),
    (56, "Light freezing drizzle", "\U0001F328\ufe0f", "Snow"),
    (57, "Dense freezing drizzle", "\U0001F328\ufe0f", "Snow"),
    (61, "Slight rain", "\U0001F327\ufe0f", "Rain"),
    (63, "Moderate rain", "\U0001F327\ufe0f", "Rain"),
    (65, "Heavy rain", "\U0001F327\ufe0f", "Rain"),
    (66, "Light freezing rain", "\U0001F328\ufe0f", "Snow"),
    (67, "Heavy freezing rain", "\U0001F328\ufe0f", "Snow"),
    (71, "Slight snow fall", "\u2744\ufe0f", "Snow"),
    (73, "Moderate snow fall", "\u2744\ufe0f", "Snow"),
    (75, "Heavy snow fall", "\u2744\ufe0f", "Snow"),
    (77, "Snow grains", "\u2744\ufe0f", "Snow"),
    (80, "Slight rain showers", "\U0001F326\ufe0f", "Rain"),
    (81, "Moderate rain showers", "\U0001F326\ufe0f", "Rain"),
    (82, "Violent rain showers", "\u26C8\ufe0f", "Storm"),
    (85, "Slight snow showers", "\U0001F328\ufe0f", "Snow"),
    (86, "Heavy snow showers", "\U0001F328\ufe0f", "Snow"),
    (95, "Thunderstorm", "\u26C8\ufe0f", "Storm"),
    (96, "Thunderstorm with slight hail", "\u26C8\ufe0f", "Storm"),
    (99, "Thunderstorm with heavy hail", "\u26C8\ufe0f", "Storm"),
]


# ---------- Configuration ----------

def get_database_url() -> str:
    """SQLAlchemy URL built from .env at project root."""
    load_dotenv(PROJECT_ROOT / ".env")
    creds = {k: os.getenv(k) for k in ("user", "password", "host", "dbname")}
    missing = [k for k, v in creds.items() if not v]
    if missing:
        raise RuntimeError(f"Missing .env values: {', '.join(missing)}")
    port = os.getenv("port", "5432")
    safe_pw = quote_plus(creds["password"])
    return f"postgresql+psycopg2://{creds['user']}:{safe_pw}@{creds['host']}:{port}/{creds['dbname']}"


def reset_tables_enabled() -> bool:
    return os.getenv("RESET_TABLES", "false").strip().lower() in {"1", "true", "yes", "y"}


# ---------- Extract (Open-Meteo APIs) ----------

def _get_with_retry(url: str, params: dict, what: str) -> dict:
    """GET with exponential backoff. Raises RuntimeError after MAX_RETRIES."""
    last_err: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            return r.json()
        except (requests.RequestException, ValueError) as e:
            last_err = e
            wait = RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1))
            log.warning("%s API attempt %d/%d failed: %s. Retrying in %ds.",
                        what, attempt, MAX_RETRIES, e, wait)
            if attempt < MAX_RETRIES:
                time.sleep(wait)
    raise RuntimeError(f"{what} API failed after {MAX_RETRIES} attempts: {last_err}")


def fetch_weather(lat: float, lon: float) -> dict:
    """Hourly weather + daily weather_code for one location."""
    data = _get_with_retry(FORECAST_API_URL, {
        "latitude": lat, "longitude": lon,
        "hourly": "temperature_2m,precipitation,wind_speed_10m,pressure_msl",
        "daily": "weather_code",
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "precipitation_unit": "inch",
        "timezone": TIMEZONE,
        "forecast_days": FORECAST_DAYS,
    }, what="weather")
    # VALIDATION CHECK 1a: API response shape (weather)
    if "hourly" not in data or "daily" not in data:
        raise ValueError(f"Weather response missing hourly/daily block: {data}")
    required = {"time", "temperature_2m", "precipitation", "wind_speed_10m", "pressure_msl"}
    if missing := required - set(data["hourly"].keys()):
        raise ValueError(f"Weather response missing fields: {missing}")
    if not data["hourly"]["time"]:
        raise ValueError("Weather response has empty hourly time array")
    return data


def fetch_flood(lat: float, lon: float) -> dict:
    """Daily river discharge forecast (GloFAS model)."""
    data = _get_with_retry(FLOOD_API_URL, {
        "latitude": lat, "longitude": lon,
        "daily": "river_discharge",
        "forecast_days": FORECAST_DAYS,
    }, what="flood")
    # VALIDATION CHECK 1b: API response shape (flood)
    if "daily" not in data:
        raise ValueError(f"Flood response missing daily block: {data}")
    if "river_discharge" not in data["daily"] or "time" not in data["daily"]:
        raise ValueError("Flood response missing river_discharge/time")
    return data


# ---------- Transform ----------

def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase, strip, snake_case column names."""
    df = df.copy()
    df.columns = (df.columns.astype(str).str.strip().str.lower()
                  .str.replace(r"\s+", "_", regex=True))
    return df


def classify_flow(discharge) -> str:
    """DERIVED METRIC: bucket river discharge into Low/Normal/High/Unknown."""
    if discharge is None or pd.isna(discharge):
        return "Unknown"
    if discharge < 100:
        return "Low"
    if discharge <= 1500:
        return "Normal"
    return "High"


def aggregate_weather_daily(weather_json: dict, location_id: int) -> pd.DataFrame:
    """Roll hourly weather to daily: mean temp/pressure, max wind, sum precip."""
    h = weather_json["hourly"]
    hourly_df = pd.DataFrame({
        "ts": pd.to_datetime(h["time"], errors="coerce"),
        "temperature_f": pd.to_numeric(h["temperature_2m"], errors="coerce"),
        "precipitation_in": pd.to_numeric(h["precipitation"], errors="coerce"),
        "wind_speed_mph": pd.to_numeric(h["wind_speed_10m"], errors="coerce"),
        "pressure_hpa": pd.to_numeric(h["pressure_msl"], errors="coerce"),
    }).dropna(subset=["ts"])
    hourly_df["date"] = hourly_df["ts"].dt.date

    daily = (hourly_df.groupby("date", as_index=False)
             .agg(temperature_f=("temperature_f", "mean"),
                  wind_speed_mph=("wind_speed_mph", "max"),
                  precipitation_in=("precipitation_in", "sum"),
                  pressure_hpa=("pressure_hpa", "mean"))
             .round(2))

    code_df = pd.DataFrame({
        "date": pd.to_datetime(pd.Series(weather_json["daily"]["time"]), errors="coerce").dt.date,
        "weather_code": weather_json["daily"]["weather_code"],
    }).dropna(subset=["date"])

    daily = daily.merge(code_df, on="date", how="left")
    daily.insert(0, "location_id", int(location_id))
    daily["weather_code"] = pd.to_numeric(daily["weather_code"], errors="coerce").astype("Int64")

    daily = daily[["location_id", "date", "weather_code", "temperature_f",
                   "wind_speed_mph", "precipitation_in", "pressure_hpa"]]
    return standardize_column_names(daily)


def parse_flood_daily(flood_json: dict, location_id: int) -> pd.DataFrame:
    """Flatten flood API response and compute flow_category."""
    d = flood_json["daily"]
    df = pd.DataFrame({
        "date": pd.to_datetime(pd.Series(d["time"]), errors="coerce").dt.date,
        "discharge_m3s": pd.to_numeric(d["river_discharge"], errors="coerce"),
    }).dropna(subset=["date"])
    df.insert(0, "location_id", int(location_id))
    df["flow_category"] = df["discharge_m3s"].apply(classify_flow)
    return standardize_column_names(df)


def build_weather_codes_df() -> pd.DataFrame:
    return standardize_column_names(pd.DataFrame(WEATHER_CODES,
                                                  columns=["code", "description", "icon", "category"]))


def build_locations_df() -> pd.DataFrame:
    return standardize_column_names(pd.DataFrame(LOCATIONS,
                                                  columns=["location_id", "location_name",
                                                           "city", "latitude", "longitude"]))


def process_location(loc_id: int, name: str, lat: float, lon: float):
    """Fetch + transform one location. Returns (weather_df, flood_df) or None on failure."""
    log.info("Fetching: %s (location_id=%d)", name, loc_id)
    try:
        return (aggregate_weather_daily(fetch_weather(lat, lon), loc_id),
                parse_flood_daily(fetch_flood(lat, lon), loc_id))
    except (requests.RequestException, RuntimeError, ValueError) as e:
        log.error("Skipping %s (location_id=%d): %s", name, loc_id, e)
        return None


def write_intermediate_csvs(weather_codes, weather_data, river_flood) -> None:
    """Persist transformed frames to data/ for inspection."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    weather_codes.to_csv(DATA_DIR / "weather_codes.csv", index=False)
    weather_data.to_csv(DATA_DIR / "weather_data.csv", index=False)
    river_flood.to_csv(DATA_DIR / "river_flood_data.csv", index=False)
    log.info("Intermediate CSVs written to %s", DATA_DIR)


# ---------- Validation ----------
# Seven checks. Hard failures (schema, nulls, duplicates, referential integrity)
# raise; soft failures (range, row count) warn and continue.

def validate_schema(df: pd.DataFrame, name: str, expected: list[str]) -> None:
    """VALIDATION CHECK 2: schema/type validation — every expected column is present."""
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValueError(f"[{name}] schema check failed — missing: {missing}")
    extra = [c for c in df.columns if c not in expected]
    if extra:
        log.warning("[%s] schema has extra columns (ignored): %s", name, extra)
    log.info("[%s] schema check passed", name)


def validate_null_critical_columns(df: pd.DataFrame, name: str, cols: list[str]) -> None:
    """VALIDATION CHECK 3: nulls in critical (PK/FK/UNIQUE) columns."""
    for col in cols:
        nulls = int(df[col].isna().sum())
        if nulls:
            raise ValueError(f"[{name}] {nulls} null(s) in '{col}'")
    log.info("[%s] null check passed on %s", name, cols)


def validate_no_duplicates(df: pd.DataFrame, name: str, keys: list[str]) -> None:
    """VALIDATION CHECK 4: no duplicates on the natural key."""
    dups = df[df.duplicated(subset=keys, keep=False)]
    if not dups.empty:
        raise ValueError(f"[{name}] {len(dups)} duplicate row(s) on {keys}")
    log.info("[%s] duplicate check passed on %s", name, keys)


def validate_referential_integrity(df: pd.DataFrame, name: str, valid_ids: set[int]) -> None:
    """VALIDATION CHECK 5: referential integrity for location_id."""
    unknown = set(df["location_id"].unique()) - valid_ids
    if unknown:
        raise ValueError(f"[{name}] unknown location_id values: {sorted(unknown)}")
    log.info("[%s] referential integrity check passed", name)


def validate_value_ranges(df: pd.DataFrame, name: str,
                          rules: dict[str, tuple[float, float]]) -> None:
    """VALIDATION CHECK 6: range validation (soft — warns)."""
    for col, (lo, hi) in rules.items():
        if col not in df.columns:
            continue
        bad = int((df[col].notna() & ((df[col] < lo) | (df[col] > hi))).sum())
        if bad:
            log.warning("[%s] %d out-of-range value(s) on '%s' (%s..%s)", name, bad, col, lo, hi)
    log.info("[%s] range check completed", name)


def validate_row_count_per_location(df: pd.DataFrame, name: str, expected: int) -> None:
    """VALIDATION CHECK 7: row count per location (soft — warns)."""
    counts = df.groupby("location_id").size()
    off = counts[counts != expected]
    if not off.empty:
        log.warning("[%s] expected %d rows/location; got %s", name, expected, off.to_dict())
    else:
        log.info("[%s] row count check passed", name)


def run_validations(location_df: pd.DataFrame, weather_df: pd.DataFrame,
                    flood_df: pd.DataFrame) -> None:
    """Run all pre-load validation checks. Hard failures raise."""
    log.info("Running data quality validations...")
    valid_ids = set(location_df["location_id"].astype(int))

    validate_schema(weather_df, "weather_data", [
        "location_id", "date", "weather_code", "temperature_f",
        "wind_speed_mph", "precipitation_in", "pressure_hpa"])
    validate_null_critical_columns(weather_df, "weather_data", ["location_id", "date"])
    validate_no_duplicates(weather_df, "weather_data", ["location_id", "date"])
    validate_referential_integrity(weather_df, "weather_data", valid_ids)
    validate_value_ranges(weather_df, "weather_data", {
        "temperature_f": (-50.0, 130.0),
        "wind_speed_mph": (0.0, 200.0),
        "precipitation_in": (0.0, 30.0),
        "pressure_hpa": (800.0, 1100.0),
    })
    validate_row_count_per_location(weather_df, "weather_data", EXPECTED_FORECAST_DAYS)

    validate_schema(flood_df, "river_flood_data",
                    ["location_id", "date", "discharge_m3s", "flow_category"])
    validate_null_critical_columns(flood_df, "river_flood_data", ["location_id", "date"])
    validate_no_duplicates(flood_df, "river_flood_data", ["location_id", "date"])
    validate_referential_integrity(flood_df, "river_flood_data", valid_ids)
    validate_value_ranges(flood_df, "river_flood_data", {"discharge_m3s": (0.0, 100000.0)})
    validate_row_count_per_location(flood_df, "river_flood_data", EXPECTED_FORECAST_DAYS)

    log.info("All validations passed.")


# ---------- Load (incremental upsert) ----------
# Re-runs use INSERT ... ON CONFLICT (location_id, date) DO UPDATE SET ... = EXCLUDED.* —
# this single clause prevents duplicates, appends new dates, and updates existing dates
# when forecasts change. Set RESET_TABLES=true in .env to opt out and do a full refresh.

def create_schema(engine, reset: bool) -> None:
    """Create the five tables. View vw_fishing_outlook lives in sql/create_views.sql."""
    if reset:
        log.info("RESET mode — dropping existing tables...")
        with engine.begin() as conn:
            conn.execute(text("""
                DROP TABLE IF EXISTS public.fishing_recommendation CASCADE;
                DROP TABLE IF EXISTS public.river_flood_data CASCADE;
                DROP TABLE IF EXISTS public.weather_data CASCADE;
                DROP TABLE IF EXISTS public.location CASCADE;
                DROP TABLE IF EXISTS public.weather_code CASCADE;
            """))
    else:
        log.info("INCREMENTAL mode — preserving existing data, upserting on conflict.")

    with engine.begin() as conn:
        conn.execute(text("""
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
                date                DATE NOT NULL,
                safety_status       VARCHAR(20),
                fish_activity_score INTEGER,
                UNIQUE (location_id, date)
            );
        """))
    log.info("Schema ready (5 tables).")


def upsert_dataframe(engine, df: pd.DataFrame, table: str,
                     conflict_cols: list[str], update_cols: list[str]) -> int:
    """Bulk INSERT ... ON CONFLICT DO UPDATE (or DO NOTHING if no update_cols)."""
    if df.empty:
        log.info("[%s] no rows to upsert", table)
        return 0

    df_clean = df.astype(object).where(pd.notnull(df), None)
    cols = list(df_clean.columns)
    placeholders = ", ".join(f":{c}" for c in cols)
    if update_cols:
        action = "DO UPDATE SET " + ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    else:
        action = "DO NOTHING"

    sql = text(f"""
        INSERT INTO public.{table} ({", ".join(cols)})
        VALUES ({placeholders})
        ON CONFLICT ({", ".join(conflict_cols)}) {action}
    """)
    try:
        with engine.begin() as conn:
            conn.execute(sql, df_clean.to_dict(orient="records"))
    except Exception as e:
        log.error("Failed to upsert %s: %s", table, e)
        raise
    log.info("[%s] upserted %d rows", table, len(df_clean))
    return len(df_clean)


def load_dimensions_and_facts(engine, weather_code_df, location_df,
                              weather_data_df, river_flood_df) -> None:
    """Load reference dimensions, then weather + flood facts, in FK-safe order."""
    upsert_dataframe(engine, weather_code_df, "weather_code", ["code"], [])
    upsert_dataframe(engine, location_df, "location", ["location_id"],
                     ["location_name", "city", "latitude", "longitude"])
    upsert_dataframe(engine, weather_data_df, "weather_data", ["location_id", "date"],
                     ["weather_code", "temperature_f", "wind_speed_mph",
                      "precipitation_in", "pressure_hpa"])
    upsert_dataframe(engine, river_flood_df, "river_flood_data", ["location_id", "date"],
                     ["discharge_m3s", "flow_category"])


# ---------- Analytics-ready fact table ----------

def fish_activity_score(temp_f, wind_mph, discharge, precip_in) -> int:
    """DERIVED METRIC: 0–100 composite score."""
    def isna(v): return v is None or pd.isna(v)

    if isna(temp_f):
        t = 5
    elif 60 <= temp_f <= 80:
        t = 30
    elif 50 <= temp_f < 60 or 80 < temp_f <= 90:
        t = 15
    else:
        t = 5

    if isna(wind_mph):
        w = 0
    elif wind_mph < 10:
        w = 20
    elif wind_mph < 20:
        w = 10
    else:
        w = 0

    if isna(discharge):
        f = 0
    elif 200 <= discharge <= 1500:
        f = 30
    elif 50 <= discharge < 200 or 1500 < discharge <= 2500:
        f = 15
    else:
        f = 0

    p = -20 if (not isna(precip_in) and precip_in > 0.5) else 0
    return max(0, t + w + f + p)


def safety_status(wind_mph, discharge, precip_in) -> str:
    """DERIVED METRIC: Safe / Caution / Unsafe."""
    def isna(v): return v is None or pd.isna(v)
    if (not isna(wind_mph) and wind_mph > 25) or (not isna(discharge) and discharge > 3000):
        return "Unsafe"
    if not isna(precip_in) and precip_in > 0.7:
        return "Caution"
    return "Safe"


def build_and_load_recommendations(engine, weather_data_df, river_flood_df) -> None:
    """Read back BIGSERIAL keys, compute derived columns, upsert into fishing_recommendation."""
    log.info("Building fishing_recommendation (analytics-ready fact table)...")
    try:
        weather_keys = pd.read_sql(
            "SELECT weather_id, location_id, date FROM public.weather_data", engine)
        flood_keys = pd.read_sql(
            "SELECT flood_id, location_id, date FROM public.river_flood_data", engine)
    except Exception as e:
        log.error("Failed to read back generated keys: %s", e)
        raise

    weather_keys["date"] = pd.to_datetime(weather_keys["date"]).dt.date
    flood_keys["date"] = pd.to_datetime(flood_keys["date"]).dt.date

    merged = (weather_data_df
              .merge(weather_keys, on=["location_id", "date"], how="inner")
              .merge(river_flood_df, on=["location_id", "date"], how="inner")
              .merge(flood_keys, on=["location_id", "date"], how="inner"))

    if merged.empty:
        log.warning("No matching weather+flood rows; skipping recommendations.")
        return

    merged["fish_activity_score"] = merged.apply(
        lambda r: fish_activity_score(r.get("temperature_f"), r.get("wind_speed_mph"),
                                       r.get("discharge_m3s"), r.get("precipitation_in")),
        axis=1).astype(int)
    merged["safety_status"] = merged.apply(
        lambda r: safety_status(r.get("wind_speed_mph"), r.get("discharge_m3s"),
                                 r.get("precipitation_in")),
        axis=1)

    rec_df = merged[["location_id", "weather_id", "flood_id", "date",
                     "safety_status", "fish_activity_score"]].copy()

    upsert_dataframe(engine, rec_df, "fishing_recommendation",
                     ["location_id", "date"],
                     ["weather_id", "flood_id", "safety_status", "fish_activity_score"])
    log.info("fishing_recommendation populated — ready for Power BI / Plotly Dash (%d rows)",
             len(rec_df))


# ---------- Main ----------

def main() -> None:
    log.info("=" * 60)
    log.info("Louisville Waterways Fishing — ETL Pipeline")
    log.info("=" * 60)

    log.info("STAGE 1/4: Extract + Transform (Open-Meteo APIs)")
    weather_frames, flood_frames = [], []
    for loc_id, name, _city, lat, lon in LOCATIONS:
        result = process_location(loc_id, name, lat, lon)
        if result:
            weather_frames.append(result[0])
            flood_frames.append(result[1])

    if not weather_frames:
        raise RuntimeError("No data fetched from Open-Meteo; aborting.")

    weather_data_df = pd.concat(weather_frames, ignore_index=True)
    river_flood_df = pd.concat(flood_frames, ignore_index=True)
    weather_codes_df = build_weather_codes_df()
    location_df = build_locations_df()
    write_intermediate_csvs(weather_codes_df, weather_data_df, river_flood_df)

    log.info("STAGE 2/4: Data quality validation")
    run_validations(location_df, weather_data_df, river_flood_df)

    log.info("STAGE 3/4: Database load")
    engine = create_engine(get_database_url())
    create_schema(engine, reset=reset_tables_enabled())
    load_dimensions_and_facts(engine, weather_codes_df, location_df,
                              weather_data_df, river_flood_df)

    log.info("STAGE 4/4: Build analytics-ready fact table")
    build_and_load_recommendations(engine, weather_data_df, river_flood_df)

    log.info("=" * 60)
    log.info("ETL PIPELINE COMPLETE")
    log.info("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.exception("ETL pipeline failed: %s", e)
        raise