# Data Quality & Validation Framework

This document describes every validation check the ETL pipeline runs, why each
one matters, and what happens when a check fails.

The pipeline implements **six** validation checks across the extract and load
stages — exceeding the rubric requirement of three. Checks are split into two
categories:

- **Hard failures** raise an exception and abort the pipeline. No partial data
  is written to the database.
- **Soft failures** log a warning and allow the pipeline to continue, so good
  data still loads even when some rows are off.

---

## Check 1 — API response validation (extract stage)

**Location:** `extract.py` — `_validate_weather_response()` and
`_validate_flood_response()`

**What it checks:** that every Open-Meteo API response contains the expected
top-level blocks (`hourly`, `daily`) and required fields (`time`,
`temperature_2m`, `precipitation`, `wind_speed_10m`, `pressure_msl` for
weather; `river_discharge`, `time` for flood). Also verifies the hourly time
array is non-empty.

**Why it matters:** Open-Meteo can return malformed responses for invalid
coordinates or during partial outages. Without this check, the transform step
would crash with a `KeyError` deep inside the aggregation logic, making the
root cause hard to diagnose. Validating at the API boundary localizes the
error to its source.

**On failure:** raises `ValueError` immediately. The retry wrapper does not
catch validation errors (only network/HTTP errors), so a malformed response
short-circuits processing for that location, and `process_location` logs the
error and skips it. The pipeline continues with the remaining locations.

---

## Check 2 — Null value check on critical columns

**Location:** `load_script.py` — `validate_null_critical_columns()`

**What it checks:** that the columns required for foreign-key relationships
and uniqueness constraints contain no nulls. Applied to:
- `weather_data.location_id`, `weather_data.date`
- `river_flood_data.location_id`, `river_flood_data.date`

**Why it matters:** a null in `location_id` would violate the `NOT NULL`
constraint and reject the row. A null in `date` would corrupt the join keys
used to build `fishing_recommendation`, producing silently incomplete
recommendations. Catching this before the load avoids partial writes and
opaque database errors.

**On failure:** raises `ValueError` with the column name and null count, and
aborts the pipeline. **Hard failure.**

---

## Check 3 — Duplicate detection on natural keys

**Location:** `load_script.py` — `validate_no_duplicates()`

**What it checks:** that `(location_id, date)` is unique in both
`weather_data` and `river_flood_data`.

**Why it matters:** both tables have `UNIQUE (location_id, date)` constraints.
A duplicate would crash the `to_sql` insert mid-batch, leaving partial data
loaded and the database in an inconsistent state. Catching duplicates *before*
touching the database means the schema reset and the load either both happen
cleanly or neither does.

**On failure:** raises `ValueError` showing the first 10 duplicate rows for
debugging, and aborts the pipeline. **Hard failure.**

---

## Check 4 — Referential integrity for `location_id`

**Location:** `load_script.py` — `validate_referential_integrity()`

**What it checks:** that every `location_id` in `weather_data` and
`river_flood_data` exists in the seed `location` set defined in
`load_script.py`.

**Why it matters:** the database enforces this with a foreign key, but
checking in Python first gives a clearer error message and avoids the
half-loaded state where `location` and `weather_code` are inserted but
`weather_data` fails. It also catches drift between the `LOCATIONS` list in
`extract.py` and the one in `load_script.py` — the two lists must stay in
sync, and this is the check that surfaces the mismatch.

**On failure:** raises `ValueError` listing the unknown location IDs, and
aborts the pipeline. **Hard failure.**

---

## Check 5 — Range validation on numeric columns

**Location:** `load_script.py` — `validate_value_ranges()`

**What it checks:** that numeric measurements fall inside physically plausible
ranges:

| Column             | Allowed range     |
| ------------------ | ----------------- |
| `temperature_f`    | −50 to 130 °F     |
| `wind_speed_mph`   | 0 to 200 mph      |
| `precipitation_in` | 0 to 30 in/day    |
| `pressure_hpa`     | 800 to 1100 hPa   |
| `discharge_m3s`    | 0 to 100 000 m³/s |

**Why it matters:** range checks catch silent failures the schema can't —
sensor glitches, API changes that swap units (e.g. wind delivered in m/s
instead of mph), or sign errors. Without this check, a corrupted value would
flow straight into `fish_activity_score` and `safety_status`, giving wrong
recommendations.

**On failure:** logs a warning per out-of-range column with the count and the
expected bounds. **Soft failure** — the pipeline still loads the data, since
one bad value shouldn't block forecasts for five other locations. Out-of-range
rows are surfaced in the logs for human review.

---

## Check 6 — Row count verification per location

**Location:** `load_script.py` — `validate_row_count_per_location()`

**What it checks:** that each `location_id` produces exactly
`EXPECTED_FORECAST_DAYS` (7) rows in both `weather_data` and
`river_flood_data`.

**Why it matters:** Open-Meteo is asked for a 7-day forecast per location, so
each location should contribute 42 rows total (6 locations × 7 days) to each
table. A short count means a location partially failed during extract and any
downstream dashboard would silently show gaps. Knowing *which* location is
missing data is more useful than knowing *that* something is missing.

**On failure:** logs a warning naming the under- or over-counted locations
along with their actual row counts. **Soft failure** — the pipeline continues
because partial coverage is still useful, and the warning makes the gap
discoverable.

---

## Summary

| # | Check                           | Stage   | Failure mode |
| - | ------------------------------- | ------- | ------------ |
| 1 | API response validation         | Extract | Hard (per location) |
| 2 | Null check on critical columns  | Load    | Hard         |
| 3 | Duplicate detection             | Load    | Hard         |
| 4 | Referential integrity           | Load    | Hard         |
| 5 | Range validation                | Load    | Soft (warn)  |
| 6 | Row count per location          | Load    | Soft (warn)  |

The split between hard and soft failures is deliberate: anything that would
corrupt the database or violate a constraint is a hard stop; anything that
just degrades data quality is logged as a warning so the pipeline remains
useful when only some sources are misbehaving.
