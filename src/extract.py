"""
Open-Meteo Extract & Transform Script — Fishing Recommendation System
---------------------------------------------------------------------
Pulls weather and river-flood forecasts from Open-Meteo for each Louisville-area
fishing location, transforms the responses into clean per-day rows, and writes
three CSVs into the data/ folder for the load script to consume:

- data/weather_codes.csv     -> WMO code reference (code, description, icon, category)
- data/weather_data.csv      -> daily weather per location/date
- data/river_flood_data.csv  -> daily river discharge per location/date
                                (flow_category is computed by the load script)

The load script handles all database work: schema creation, CSV loading,
flow categorization, fish activity scoring, and safety classification.

Required packages:
    pip install pandas requests
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import pandas as pd
import requests


# Logging configuration
# Console output with timestamp, level, and module so problems are easy to
# trace when running the full pipeline.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("extract")


# Retry configuration
# Open-Meteo is generally reliable but occasionally returns 5xx or times out;
# retrying with exponential backoff handles transient failures cleanly.
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"

WEATHER_CODES_CSV = DATA_DIR / "weather_codes.csv"
WEATHER_DATA_CSV = DATA_DIR / "weather_data.csv"
RIVER_FLOOD_CSV = DATA_DIR / "river_flood_data.csv"

FORECAST_API_URL = "https://api.open-meteo.com/v1/forecast"
FLOOD_API_URL = "https://flood-api.open-meteo.com/v1/flood"
TIMEZONE = "America/New_York"
FORECAST_DAYS = 7


# Seed data — Louisville-area fishing locations
# Must match the LOCATIONS list in load_script.py so that location_id values
# in the generated CSVs reference valid rows in the location table.
LOCATIONS = [
    (1, "McAlpine Locks & Dam",             "Louisville",   38.2742, -85.7984),
    (2, "Cox Park Boat Ramp",               "Louisville",   38.3017, -85.6483),
    (3, "Riverview Park",                   "Louisville",   38.1531, -85.8736),
    (4, "Taylorsville Lake",                "Taylorsville", 38.0322, -85.2380),
    (5, "Floyds Fork - Beckley Creek Park", "Louisville",   38.2317, -85.5147),
    (6, "Otter Creek Outdoor Recreation",   "Brandenburg",  37.9425, -86.0656),
]


# WMO weather code reference
# Built-in lookup so weather_codes.csv has all four columns the load script
# expects (code, description, icon, category). Source: Open-Meteo docs.
WEATHER_CODES = [
    (0,  "Clear sky",                          "\u2600\ufe0f",      "Clear"),
    (1,  "Mainly clear",                       "\U0001F324\ufe0f",  "Clear"),
    (2,  "Partly cloudy",                      "\u26C5",            "Cloudy"),
    (3,  "Overcast",                           "\u2601\ufe0f",      "Cloudy"),
    (45, "Fog",                                "\U0001F32B\ufe0f",  "Fog"),
    (48, "Depositing rime fog",                "\U0001F32B\ufe0f",  "Fog"),
    (51, "Light drizzle",                      "\U0001F327\ufe0f",  "Rain"),
    (53, "Moderate drizzle",                   "\U0001F327\ufe0f",  "Rain"),
    (55, "Dense drizzle",                      "\U0001F327\ufe0f",  "Rain"),
    (56, "Light freezing drizzle",             "\U0001F328\ufe0f",  "Snow"),
    (57, "Dense freezing drizzle",             "\U0001F328\ufe0f",  "Snow"),
    (61, "Slight rain",                        "\U0001F327\ufe0f",  "Rain"),
    (63, "Moderate rain",                      "\U0001F327\ufe0f",  "Rain"),
    (65, "Heavy rain",                         "\U0001F327\ufe0f",  "Rain"),
    (66, "Light freezing rain",                "\U0001F328\ufe0f",  "Snow"),
    (67, "Heavy freezing rain",                "\U0001F328\ufe0f",  "Snow"),
    (71, "Slight snow fall",                   "\u2744\ufe0f",      "Snow"),
    (73, "Moderate snow fall",                 "\u2744\ufe0f",      "Snow"),
    (75, "Heavy snow fall",                    "\u2744\ufe0f",      "Snow"),
    (77, "Snow grains",                        "\u2744\ufe0f",      "Snow"),
    (80, "Slight rain showers",                "\U0001F326\ufe0f",  "Rain"),
    (81, "Moderate rain showers",              "\U0001F326\ufe0f",  "Rain"),
    (82, "Violent rain showers",               "\u26C8\ufe0f",      "Storm"),
    (85, "Slight snow showers",                "\U0001F328\ufe0f",  "Snow"),
    (86, "Heavy snow showers",                 "\U0001F328\ufe0f",  "Snow"),
    (95, "Thunderstorm",                       "\u26C8\ufe0f",      "Storm"),
    (96, "Thunderstorm with slight hail",      "\u26C8\ufe0f",      "Storm"),
    (99, "Thunderstorm with heavy hail",       "\u26C8\ufe0f",      "Storm"),
]


# ---------------------------------------------------------------------
# Open-Meteo API calls (EXTRACT)
# ---------------------------------------------------------------------
# Two separate endpoints: the forecast API for weather, the flood API for
# river discharge. Both are queried per location. Open-Meteo is an open API
# with no authentication required, and returns 7 days in a single response
# so no pagination is needed either.
# ---------------------------------------------------------------------
def _get_with_retry(url: str, params: dict, *, what: str) -> dict:
    """
    GET with exponential backoff retries. Raises RuntimeError if all retries fail.

    `what` is just a short label for log messages (e.g. "weather", "flood").
    """
    last_error: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as e:
            last_error = e
            wait = RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1))
            log.warning(
                "%s API call failed (attempt %d/%d): %s. Retrying in %ds...",
                what, attempt, MAX_RETRIES, e, wait,
            )
            if attempt < MAX_RETRIES:
                time.sleep(wait)

    raise RuntimeError(f"{what} API failed after {MAX_RETRIES} attempts: {last_error}")


def _validate_weather_response(data: dict) -> None:
    """
    VALIDATION CHECK 1: API response validation (weather).
    Confirms expected keys and arrays are present before downstream code
    tries to access them. Open-Meteo can return error responses with no
    hourly/daily blocks for invalid coordinates or partial outages.
    """
    if "hourly" not in data or "daily" not in data:
        raise ValueError(f"Weather response missing 'hourly' or 'daily' block: {data}")
    required_hourly = {"time", "temperature_2m", "precipitation", "wind_speed_10m", "pressure_msl"}
    missing_hourly = required_hourly - set(data["hourly"].keys())
    if missing_hourly:
        raise ValueError(f"Weather response missing hourly fields: {missing_hourly}")
    if not data["hourly"]["time"]:
        raise ValueError("Weather response contains an empty hourly time array")


def _validate_flood_response(data: dict) -> None:
    """
    VALIDATION CHECK 1 (continued): API response validation (flood).
    """
    if "daily" not in data:
        raise ValueError(f"Flood response missing 'daily' block: {data}")
    if "river_discharge" not in data["daily"] or "time" not in data["daily"]:
        raise ValueError("Flood response missing 'river_discharge' or 'time' field")


def fetch_weather(lat: float, lon: float) -> dict:
    """Hourly weather + daily weather_code for one location, with retry + validation."""
    params = {
        "latitude":           lat,
        "longitude":          lon,
        "hourly":             "temperature_2m,precipitation,wind_speed_10m,pressure_msl",
        "daily":              "weather_code",
        "temperature_unit":   "fahrenheit",
        "wind_speed_unit":    "mph",
        "precipitation_unit": "inch",
        "timezone":           TIMEZONE,
        "forecast_days":      FORECAST_DAYS,
    }
    data = _get_with_retry(FORECAST_API_URL, params, what="weather")
    _validate_weather_response(data)
    return data


def fetch_flood(lat: float, lon: float) -> dict:
    """Daily river discharge forecast (GloFAS model), with retry + validation."""
    params = {
        "latitude":      lat,
        "longitude":     lon,
        "daily":         "river_discharge",
        "forecast_days": FORECAST_DAYS,
    }
    data = _get_with_retry(FLOOD_API_URL, params, what="flood")
    _validate_flood_response(data)
    return data


# ---------------------------------------------------------------------
# Naming + type normalization helpers (TRANSFORM)
# ---------------------------------------------------------------------
# Centralized helpers that standardize column naming conventions and
# normalize data types across every DataFrame the extract step produces.
# Keeping these in one place means there is exactly one rule for what
# "clean" looks like.
# ---------------------------------------------------------------------
def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize column naming: lowercase, stripped of whitespace, and
    underscores instead of spaces. Idempotent — safe to call repeatedly.
    """
    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", "_", regex=True)
    )
    return df


def clean_numeric_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """
    Coerce columns to numeric. Values that cannot be parsed become NaN so the
    load-side range and null checks can flag them rather than crashing the
    pipeline mid-transform.
    """
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


# ---------------------------------------------------------------------
# Weather transformation (TRANSFORM)
# ---------------------------------------------------------------------
# Open-Meteo gives hourly observations; the schema stores one row per day,
# so hourly readings are rolled up here. Wind uses max (worst-case for the
# safety classifier downstream); temperature and pressure use mean;
# precipitation is summed across the day.
# ---------------------------------------------------------------------
def aggregate_weather_daily(weather_json: dict, location_id: int) -> pd.DataFrame:
    hourly = weather_json["hourly"]
    hourly_df = pd.DataFrame(
        {
            "ts":               pd.to_datetime(hourly["time"], errors="coerce"),
            "temperature_f":    hourly["temperature_2m"],
            "precipitation_in": hourly["precipitation"],
            "wind_speed_mph":   hourly["wind_speed_10m"],
            "pressure_hpa":     hourly["pressure_msl"],
        }
    )

    # CLEAN: drop rows where the timestamp couldn't be parsed; coerce
    # numeric columns so non-numeric junk becomes NaN instead of raising.
    hourly_df = hourly_df.dropna(subset=["ts"])
    hourly_df = clean_numeric_columns(
        hourly_df,
        ["temperature_f", "precipitation_in", "wind_speed_mph", "pressure_hpa"],
    )
    hourly_df["date"] = hourly_df["ts"].dt.date

    # AGGREGATE: hourly -> daily. min_count=1 keeps the result NaN if every
    # hourly value for a day was missing, rather than silently producing 0.
    daily_df = (
        hourly_df.groupby("date", as_index=False)
        .agg(
            temperature_f=("temperature_f", "mean"),
            wind_speed_mph=("wind_speed_mph", "max"),
            precipitation_in=("precipitation_in", "sum"),
            pressure_hpa=("pressure_hpa", "mean"),
        )
        .round(2)
    )

    # Bring in the daily weather_code from the API's daily block.
    daily_api = weather_json["daily"]
    code_df = pd.DataFrame(
        {
            "date": pd.to_datetime(pd.Series(daily_api["time"]), errors="coerce").dt.date,
            "weather_code": daily_api["weather_code"],
        }
    ).dropna(subset=["date"])

    daily_df = daily_df.merge(code_df, on="date", how="left")
    daily_df.insert(0, "location_id", location_id)

    # NORMALIZE types: weather_code is integer (nullable Int64 so a missing
    # code doesn't get silently coerced to 0).
    daily_df["weather_code"] = pd.to_numeric(
        daily_df["weather_code"], errors="coerce"
    ).astype("Int64")
    daily_df["location_id"] = daily_df["location_id"].astype(int)

    daily_df = daily_df[
        [
            "location_id",
            "date",
            "weather_code",
            "temperature_f",
            "wind_speed_mph",
            "precipitation_in",
            "pressure_hpa",
        ]
    ]

    # STANDARDIZE: ensure consistent column naming convention.
    return standardize_column_names(daily_df)


# ---------------------------------------------------------------------
# Flood transformation (TRANSFORM)
# ---------------------------------------------------------------------
# Flatten the flood API response into rows of (location_id, date, discharge_m3s).
# Note: flow_category is intentionally NOT computed here — the load script
# applies classify_flow so all categorization rules live in one place.
# ---------------------------------------------------------------------
def parse_flood_daily(flood_json: dict, location_id: int) -> pd.DataFrame:
    daily = flood_json["daily"]
    flood_df = pd.DataFrame(
        {
            "date":          pd.to_datetime(pd.Series(daily["time"]), errors="coerce").dt.date,
            "discharge_m3s": daily["river_discharge"],
        }
    )

    # CLEAN: drop rows with unparseable dates; coerce discharge to numeric.
    flood_df = flood_df.dropna(subset=["date"])
    flood_df = clean_numeric_columns(flood_df, ["discharge_m3s"])

    flood_df.insert(0, "location_id", location_id)
    flood_df["location_id"] = flood_df["location_id"].astype(int)

    # STANDARDIZE: ensure consistent column naming convention.
    return standardize_column_names(flood_df)


# Weather codes reference table
# Writes the built-in WMO code lookup to CSV so the load script can read it
# without needing the Excel sheet from the lecturer's sample.
def build_weather_codes_df() -> pd.DataFrame:
    df = pd.DataFrame(
        WEATHER_CODES,
        columns=["code", "description", "icon", "category"],
    )
    return standardize_column_names(df)


# ---------------------------------------------------------------------
# Per-location processing
# ---------------------------------------------------------------------
# Calls both APIs, transforms the responses, and returns the two per-location
# DataFrames so the caller can stack them across all locations.
# ---------------------------------------------------------------------
def process_location(
    location_id: int, name: str, lat: float, lon: float
) -> tuple[pd.DataFrame, pd.DataFrame] | None:
    log.info("Fetching: %s (location_id=%d)", name, location_id)
    try:
        weather_json = fetch_weather(lat, lon)
        flood_json = fetch_flood(lat, lon)
    except (requests.RequestException, RuntimeError, ValueError) as e:
        # Skip this location but keep going with the others. The pipeline
        # is still useful with partial coverage; row-count validation in the
        # load script will flag the gap.
        log.error("Skipping %s (location_id=%d): %s", name, location_id, e)
        return None

    weather_df = aggregate_weather_daily(weather_json, location_id)
    flood_df = parse_flood_daily(flood_json, location_id)
    return weather_df, flood_df


# CSV writers
# Centralized so the output directory and indexing behavior stay consistent.
def write_csv(df: pd.DataFrame, path: Path) -> None:
    df.to_csv(path, index=False)
    log.info("Wrote %s (%d rows)", path.name, len(df))


# Main workflow orchestration
# Ensures the data/ directory exists, iterates over LOCATIONS, calls Open-Meteo,
# stitches the results together, and writes the three CSVs the load script reads.
def main() -> None:
    log.info("=" * 60)
    log.info("Louisville Waterways Fishing — Extract & Transform")
    log.info("=" * 60)

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    weather_frames: list[pd.DataFrame] = []
    flood_frames: list[pd.DataFrame] = []

    for location_id, name, _city, lat, lon in LOCATIONS:
        result = process_location(location_id, name, lat, lon)
        if result is None:
            continue
        weather_df, flood_df = result
        weather_frames.append(weather_df)
        flood_frames.append(flood_df)

    if not weather_frames or not flood_frames:
        raise RuntimeError("No data fetched from Open-Meteo; aborting.")

    weather_data_df = pd.concat(weather_frames, ignore_index=True)
    river_flood_df = pd.concat(flood_frames, ignore_index=True)
    weather_codes_df = build_weather_codes_df()

    log.info("Writing CSVs...")
    write_csv(weather_codes_df, WEATHER_CODES_CSV)
    write_csv(weather_data_df, WEATHER_DATA_CSV)
    write_csv(river_flood_df, RIVER_FLOOD_CSV)

    log.info("=" * 60)
    log.info("EXTRACT & TRANSFORM COMPLETE")
    log.info("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.exception("Extract pipeline failed: %s", e)
        raise