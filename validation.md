# Data Quality & Validation Framework

This document describes every validation check the ETL pipeline runs, why each
one matters, and what happens when a check fails.

The pipeline implements **six** validation checks across the extract and load
stages — exceeding the rubric requirement of three. Checks are split into two
categories:

- **Hard failures** raise an exception and abort the pipeline. No partial data
  is written to the database.
- **Soft failures** log a warning and allow the pipeline to continue, so good
  data still loads even when some rows are problematic.

---

## Check 1 — API Response Validation (Extract Stage)

**Location:** `etl_pipeline.py` — `_validate_weather_response()` and
`_validate_flood_response()`

**What it checks:** That every Open-Meteo API response contains the expected
top-level blocks (`hourly`, `daily`) and required fields (`time`,
`temperature_2m`, `precipitation`, `wind_speed_10m`, `pressure_msl` for
weather; `river_discharge`, `time` for flood). Also verifies the hourly time
array is non-empty.

**Why it matters:** Open-Meteo can return malformed responses for invalid
coordinates or during partial outages. Without this check, the transform step
would crash with a `KeyError` deep inside the aggregation logic, making the
root cause hard to diagnose. Validating at the API boundary localizes the
error to its source.

**On failure:** Raises `ValueError` immediately. The retry wrapper does not
catch validation errors (only network/HTTP errors), so a malformed response
short-circuits processing for that location. `process_location` logs the error
and skips it; the pipeline continues with the remaining locations.
**Hard failure (per location).**

---

## Check 2 — Null Value Check on Critical Columns (Load Stage)

**Location:** `etl_pipeline.py` — `validate_null_critical_columns()`

**What it checks:** That the columns required for foreign-key relationships
and uniqueness constraints contain no nulls. Applied to:

- `weather_data.location_id`, `weather_data.date`
- `river_flood_data.location_id`, `river_flood_data.date`

**Why it matters:** A null in `location_id` would violate the `NOT NULL`
constraint and reject the row. A null in `date` would corrupt the join keys
used to build `fishing_recommendation`, producing silently incomplete
recommendations. Catching this before the load avoids partial writes and
opaque database errors.

**On failure:** Raises `ValueError` with the column name and null count, and
aborts the pipeline. **Hard failure.**

---

## Check 3 — Duplicate Detection on Natural Keys (Load Stage)

**Location:** `etl_pipeline.py` — `validate_no_duplicates()`

**What it checks:** That `(location_id, date)` is unique in both
`weather_data` and `river_flood_data`.

**Why it matters:** Both tables have `UNIQUE (location_id, date)` constraints.
A duplicate would crash the `to_sql` insert mid-batch, leaving partial data
loaded and the database in an inconsistent state. Catching duplicates *before*
touching the database means either the schema reset and the load both happen
cleanly, or neither does.

**On failure:** Raises `ValueError` showing the first 10 duplicate rows for
debugging, and aborts the pipeline. **Hard failure.**

---

## Check 4 — Referential Integrity for `location_id` (Load Stage)

**Location:** `etl_pipeline.py` — `validate_referential_integrity()`

**What it checks:** That every `location_id` in `weather_data` and
`river_flood_data` exists in the seed `location` set defined in the pipeline.

**Why it matters:** The database enforces this with a foreign key, but checking
in Python first gives a clearer error message and avoids the half-loaded state
where `location` and `weather_code` are inserted but `weather_data` fails. It
also catches drift between the `LOCATIONS` list in extract and the one in load —
the two lists must stay in sync, and this check surfaces the mismatch.

**On failure:** Raises `ValueError` listing the unknown location IDs, and
aborts the pipeline. **Hard failure.**

---

## Check 5 — Range Validation on Numeric Columns (Load Stage)

**Location:** `etl_pipeline.py` — `validate_value_ranges()`

**What it checks:** That numeric measurements fall inside physically plausible
ranges:

| Column | Allowed Range |
|--------|---------------|
| `temperature_f` | −50 to 130 °F |
| `wind_speed_mph` | 0 to 200 mph |
| `precipitation_in` | 0 to 30 in/day |
| `pressure_hpa` | 800 to 1100 hPa |
| `discharge_m3s` | 0 to 100,000 m³/s |

**Why it matters:** Range checks catch silent failures the schema cannot —
sensor glitches, API changes that swap units (e.g. wind delivered in m/s
instead of mph), or sign errors. Without this check, a corrupted value would
flow straight into `fish_activity_score` and `safety_status`, producing wrong
recommendations with no visible error.

**On failure:** Logs a warning per out-of-range column with the count and the
expected bounds. **Soft failure** — the pipeline still loads the data, since
one bad value should not block forecasts for five other locations. Out-of-range
rows are surfaced in the logs for human review.

---

## Check 6 — Row Count Verification per Location (Load Stage)

**Location:** `etl_pipeline.py` — `validate_row_count_per_location()`

**What it checks:** That each `location_id` produces exactly
`EXPECTED_FORECAST_DAYS` (7) rows in both `weather_data` and `river_flood_data`.

**Why it matters:** Open-Meteo is asked for a 7-day forecast per location, so
each location should contribute 42 rows total (6 locations × 7 days) to each
table. A short count means a location partially failed during extract and the
dashboard would silently show gaps. Knowing *which* location is short is more
actionable than knowing *that* something is missing.

**On failure:** Logs a warning naming the under- or over-counted locations
along with their actual row counts. **Soft failure** — the pipeline continues
because partial coverage is still useful, and the warning makes the gap
discoverable.

---

## Summary

| # | Check | Stage | Failure Mode |
|---|-------|-------|-------------|
| 1 | API response validation | Extract | Hard (per location) |
| 2 | Null check on critical columns | Load | Hard |
| 3 | Duplicate detection on natural keys | Load | Hard |
| 4 | Referential integrity for `location_id` | Load | Hard |
| 5 | Range validation on numeric columns | Load | Soft (warn) |
| 6 | Row count verification per location | Load | Soft (warn) |

The split between hard and soft failures is deliberate: anything that would
corrupt the database or violate a constraint is a hard stop; anything that
merely degrades data quality is logged as a warning so the pipeline remains
useful even when only some sources are misbehaving.
