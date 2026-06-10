-- =====================================================================
-- vw_fishing_outlook — denormalized analytics view
-- =====================================================================
-- One row per (location, date) joining the fact table with every
-- dimension needed by the dashboard, Power BI, and ad-hoc queries.
--
-- HOW TO USE
--   1. Make sure etl_pipeline.py has been run at least once so the
--      underlying tables exist and are populated.
--   2. Open Supabase -> SQL Editor -> paste this whole file -> Run.
--   3. You only need to do this once.
--
-- WHEN TO RE-RUN
--   - After a fresh setup on a new Supabase project
--   - After running the ETL with RESET_TABLES=true (the CASCADE drop
--     removes this view along with the underlying tables)
--
-- The view is a regular VIEW (not materialized), so it reflects the
-- current state of the tables on every query -- no manual refresh
-- needed when the ETL upserts new rows.
-- =====================================================================

CREATE OR REPLACE VIEW public.vw_fishing_outlook AS
SELECT
    l.location_id,
    l.location_name,
    l.city,
    l.latitude,
    l.longitude,
    r.date,
    r.safety_status,
    r.fish_activity_score,
    w.temperature_f,
    w.wind_speed_mph,
    w.precipitation_in,
    w.pressure_hpa,
    w.weather_code,
    wc.description AS weather_description,
    wc.category    AS weather_category,
    f.discharge_m3s,
    f.flow_category
FROM public.fishing_recommendation r
JOIN public.location          l  ON l.location_id  = r.location_id
JOIN public.weather_data      w  ON w.weather_id   = r.weather_id
LEFT JOIN public.weather_code wc ON wc.code        = w.weather_code
JOIN public.river_flood_data  f  ON f.flood_id     = r.flood_id;


-- Quick sanity check after creating the view:
--   SELECT COUNT(*) FROM vw_fishing_outlook;
--   SELECT * FROM vw_fishing_outlook ORDER BY date DESC LIMIT 10;
