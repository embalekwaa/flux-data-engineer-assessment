-- =============================================================================
-- Flux Data Engineer assessment: Part A (SQL & modelling). BigQuery dialect.
--
-- Tables assumed in dataset `flux` (all produced/loaded by the Python pipeline):
--   flux.raw_bookings  every source row as STRING, both batches, + _batch_id,
--                      _batch_date, _source_row  (landing table; used by A6)
--   flux.stg_bookings  cleaned, typed, *valid* rows: every version, all batches
--                      ("the cleaned bookings table" that A1-A5 build on)
--   flux.fx_rates      currency, rate_to_zar, effective_from
--   flux.properties    property dimension (country already standardised)
--
-- Each statement is tagged `-- @name:` so scripts/run_sql_duckdb.py can run the
-- file locally (via sqlglot BigQuery->DuckDB) and save every query's results.
--
-- CANCELLED / PENDING POLICY (applies to every reporting query below):
--   Revenue = *recognised* revenue = confirmed bookings only.
--   * cancelled: excluded. The guest isn't staying and the revenue won't be
--     earned. Kept in the model (is_cancelled flag) for cancellation analysis.
--   * pending: excluded. Not yet earned; it's pipeline, not revenue, and many
--     pending bookings never convert. Visible in revenue_zar (gross booked value, any status).
-- =============================================================================


-- -----------------------------------------------------------------------------
-- A1. Current version of each booking: one row per booking_id, latest updated_at.
-- updated_at is the source's version marker. The extra ORDER BY keys make ties
-- deterministic (later extract, then later file row), matching the pipeline's
-- merge rule, so SQL and Python agree on which version is current.
-- `WHERE TRUE` keeps QUALIFY valid in BigQuery editions that require a WHERE.
-- -----------------------------------------------------------------------------
-- @name: a1_v_bookings_current
CREATE OR REPLACE VIEW flux.v_bookings_current AS
SELECT *
FROM flux.stg_bookings
WHERE TRUE
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY booking_id
  ORDER BY updated_at DESC, _batch_date DESC, _source_row DESC
) = 1;


-- -----------------------------------------------------------------------------
-- A2. Point-in-time revenue in ZAR.
-- Turn each FX row into a validity window [effective_from, effective_to) with
-- LEAD, then range-join on the booking's created_at DATE. fx_rates is tiny, so
-- BigQuery broadcasts it and the join stays cheap at any booking volume.
-- LEFT JOIN: a booking with no applicable rate stays visible with
-- revenue_zar = NULL (surfaced by the A6 check) instead of silently vanishing.
-- -----------------------------------------------------------------------------
-- @name: a2_v_bookings_zar
CREATE OR REPLACE VIEW flux.v_bookings_zar AS
WITH fx_windows AS (
  SELECT
    currency,
    rate_to_zar,
    effective_from,
    LEAD(effective_from) OVER (PARTITION BY currency ORDER BY effective_from) AS effective_to
  FROM flux.fx_rates
)
SELECT
  b.*,
  fx.rate_to_zar                         AS fx_rate_to_zar,
  fx.effective_from                      AS fx_effective_from,
  ROUND(b.revenue * fx.rate_to_zar, 2)   AS revenue_zar
FROM flux.v_bookings_current AS b
LEFT JOIN fx_windows AS fx
  ON  fx.currency = b.currency
  AND DATE(b.created_at) >= fx.effective_from
  AND (fx.effective_to IS NULL OR DATE(b.created_at) < fx.effective_to);

-- @name: a2_sample_revenue_zar
SELECT booking_id, currency, revenue, DATE(created_at) AS created_date,
       fx_effective_from, fx_rate_to_zar, revenue_zar
FROM flux.v_bookings_zar
ORDER BY booking_id
LIMIT 10;


-- -----------------------------------------------------------------------------
-- A3. Reporting model.
--
-- PARTITION BY DATE_TRUNC(check_in_date, MONTH)
--   * Revenue is reported by stay month, so the dominant filter ("2025",
--     "last 6 months") prunes whole partitions; only those months are scanned
--     and billed.
--   * MONTH not DAY: a portfolio of lodges produces far less than the ~1 GB a
--     day that makes daily partitions worthwhile. Daily partitions would be
--     tiny (metadata overhead, poor clustering) and burn through the partition
--     limit. Monthly gives about 120 partitions per decade.
--   * Not created_at: analysts slice by when guests stay, not when they booked.
--     The trade-off is that the MERGE has to find a booking's old row (and its
--     check_in may have been amended), so the load prunes the target with the
--     batch's min/max check_in window (see Part C).
--
-- CLUSTER BY property_id, booking_channel, booking_status
--   * Order = most selective / most filtered first. Per-property is the main
--     client cut, then channel mix; status is low-cardinality but almost every
--     query filters `booking_status = 'confirmed'`, so blocks of cancelled rows
--     are skipped.
--   * Clustering is free to maintain (automatic re-clustering) and cuts bytes
--     scanned inside each monthly partition.
-- -----------------------------------------------------------------------------
-- @name: a3_ddl_fct_bookings
CREATE TABLE IF NOT EXISTS flux.fct_bookings (
  booking_id          STRING    NOT NULL,
  property_id         STRING    NOT NULL,
  booking_channel     STRING,
  booking_status      STRING,
  check_in_date       DATE      NOT NULL,
  check_out_date      DATE,
  nights              INT64,
  num_guests          INT64,
  currency            STRING,
  room_rate           NUMERIC,
  revenue             NUMERIC,           -- local currency, as booked
  fx_rate_to_zar      NUMERIC,           -- rate effective on created_at date
  revenue_zar         NUMERIC,
  created_at          TIMESTAMP,
  updated_at          TIMESTAMP,         -- version marker used by the MERGE
  _batch_id           STRING             -- lineage: extract that supplied this version
)
PARTITION BY DATE_TRUNC(check_in_date, MONTH)
CLUSTER BY property_id, booking_channel, booking_status
OPTIONS (description = 'One row per booking (current version), revenue at point-in-time FX.');

-- Incremental upsert (the production load). Only a strictly newer updated_at
-- may overwrite, so re-sends and stale out-of-order rows are no-ops and the
-- statement is idempotent. Cancellations are an update like any other.
-- @name: a3_merge_fct_bookings
MERGE flux.fct_bookings AS t
USING (
  SELECT booking_id, property_id, booking_channel, booking_status, check_in_date,
         check_out_date, nights, num_guests, currency, room_rate, revenue,
         fx_rate_to_zar, revenue_zar, created_at, updated_at, _batch_id
  FROM flux.v_bookings_zar
) AS s
ON t.booking_id = s.booking_id
WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET
  property_id = s.property_id, booking_channel = s.booking_channel,
  booking_status = s.booking_status, check_in_date = s.check_in_date,
  check_out_date = s.check_out_date, nights = s.nights, num_guests = s.num_guests,
  currency = s.currency, room_rate = s.room_rate, revenue = s.revenue,
  fx_rate_to_zar = s.fx_rate_to_zar, revenue_zar = s.revenue_zar,
  created_at = s.created_at, updated_at = s.updated_at, _batch_id = s._batch_id
WHEN NOT MATCHED THEN INSERT (
  booking_id, property_id, booking_channel, booking_status, check_in_date,
  check_out_date, nights, num_guests, currency, room_rate, revenue,
  fx_rate_to_zar, revenue_zar, created_at, updated_at, _batch_id
) VALUES (
  s.booking_id, s.property_id, s.booking_channel, s.booking_status, s.check_in_date,
  s.check_out_date, s.nights, s.num_guests, s.currency, s.room_rate, s.revenue,
  s.fx_rate_to_zar, s.revenue_zar, s.created_at, s.updated_at, s._batch_id
);

-- Reusable reporting view: booking grain plus property attributes, with the
-- status policy encoded once so every downstream query applies it the same way.
-- Analysts aggregate this view instead of re-deriving the logic.
-- @name: a3_v_bookings_reporting
CREATE OR REPLACE VIEW flux.v_bookings_reporting AS
SELECT
  f.booking_id,
  f.property_id,
  p.property_name,
  p.country,
  p.region,
  p.property_status,
  f.booking_channel,
  f.booking_status,
  f.booking_status = 'confirmed'                         AS is_confirmed,
  f.booking_status = 'pending'                           AS is_pending,
  f.booking_status = 'cancelled'                         AS is_cancelled,
  f.check_in_date,
  f.check_out_date,
  DATE_TRUNC(f.check_in_date, MONTH)                     AS stay_month,
  f.nights,
  f.num_guests,
  f.currency,
  f.revenue,
  f.fx_rate_to_zar,
  f.revenue_zar,                                         -- gross booked value, any status
  IF(f.booking_status = 'confirmed', f.revenue_zar, 0)   AS recognised_revenue_zar,
  f.created_at,
  f.updated_at
FROM flux.fct_bookings AS f
LEFT JOIN flux.properties AS p USING (property_id);


-- -----------------------------------------------------------------------------
-- A4. Month-on-month recognised revenue growth, 2025, ZAR (confirmed only).
-- A calendar spine makes a month with no confirmed stays show 0 instead of
-- disappearing, which would otherwise make LAG compare non-adjacent months.
-- The filter on stay_month prunes the fact table to 2025 partitions.
-- -----------------------------------------------------------------------------
-- @name: a4_mom_revenue_growth_2025
WITH months AS (
  SELECT month
  FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2025-01-01', DATE '2025-12-01', INTERVAL 1 MONTH)) AS month
),
monthly AS (
  SELECT stay_month, SUM(recognised_revenue_zar) AS revenue_zar
  FROM flux.v_bookings_reporting
  WHERE is_confirmed
    AND stay_month BETWEEN DATE '2025-01-01' AND DATE '2025-12-01'
  GROUP BY stay_month
),
spine AS (
  SELECT m.month, COALESCE(r.revenue_zar, 0) AS revenue_zar
  FROM months AS m
  LEFT JOIN monthly AS r ON r.stay_month = m.month
)
SELECT
  month,
  ROUND(revenue_zar, 2)                                       AS revenue_zar,
  ROUND(LAG(revenue_zar) OVER (ORDER BY month), 2)            AS prev_month_revenue_zar,
  -- NULL (not +inf) when the prior month is 0 or missing
  ROUND(100 * SAFE_DIVIDE(revenue_zar - LAG(revenue_zar) OVER (ORDER BY month),
                          LAG(revenue_zar) OVER (ORDER BY month)), 2) AS mom_growth_pct
FROM spine
ORDER BY month;


-- -----------------------------------------------------------------------------
-- A5. Top 2 properties by recognised revenue within each country (ZAR).
-- RANK (not ROW_NUMBER) so tied properties share a rank and neither is
-- arbitrarily dropped. RANK (not DENSE_RANK) so a tie for 1st uses up both
-- places: "top 2" never returns a third, lower-earning property just because
-- two others tied. A country with one property returns one row.
-- -----------------------------------------------------------------------------
-- @name: a5_top2_properties_per_country
WITH property_revenue AS (
  SELECT country, property_id, property_name,
         SUM(recognised_revenue_zar) AS revenue_zar,
         COUNTIF(is_confirmed)       AS confirmed_bookings
  FROM flux.v_bookings_reporting
  WHERE is_confirmed
  GROUP BY country, property_id, property_name
)
SELECT country, revenue_rank, property_id, property_name,
       ROUND(revenue_zar, 2) AS revenue_zar, confirmed_bookings
FROM (
  SELECT *, RANK() OVER (PARTITION BY country ORDER BY revenue_zar DESC) AS revenue_rank
  FROM property_revenue
)
WHERE revenue_rank <= 2
ORDER BY country, revenue_rank, property_id;


-- -----------------------------------------------------------------------------
-- A6. Data-quality assertions against the RAW landing table.
-- One row per check: how many rows/ids fail it. Raw values are parsed with
-- SAFE.* functions so a bad value counts as a failure instead of raising an
-- error. severity: 'error' = would corrupt reporting if loaded; 'warn' =
-- needs attention but is handled.
-- -----------------------------------------------------------------------------
-- @name: a6_dq_assertions_raw
WITH raw AS (
  SELECT
    r.*,
    UPPER(TRIM(property_id)) AS property_id_std,
    COALESCE(SAFE.PARSE_DATE('%Y-%m-%d', TRIM(check_in)),
             SAFE.PARSE_DATE('%d/%m/%Y', TRIM(check_in)),
             SAFE.PARSE_DATE('%d %b %Y', TRIM(check_in)))  AS check_in_d,
    COALESCE(SAFE.PARSE_DATE('%Y-%m-%d', TRIM(check_out)),
             SAFE.PARSE_DATE('%d/%m/%Y', TRIM(check_out)),
             SAFE.PARSE_DATE('%d %b %Y', TRIM(check_out))) AS check_out_d,
    SAFE_CAST(REPLACE(TRIM(revenue), ',', '') AS NUMERIC)  AS revenue_n,
    SAFE_CAST(TRIM(num_guests) AS INT64)                   AS guests_n,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', TRIM(created_at)) AS created_ts,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', TRIM(updated_at)) AS updated_ts,
    CASE UPPER(TRIM(currency))
      WHEN '€' THEN 'EUR' WHEN '£' THEN 'GBP' WHEN '$' THEN 'USD'
      WHEN 'US$' THEN 'USD' WHEN 'R' THEN 'ZAR'
      ELSE UPPER(TRIM(currency))
    END AS currency_std
  FROM flux.raw_bookings AS r
),
fx_windows AS (
  SELECT currency, effective_from,
         LEAD(effective_from) OVER (PARTITION BY currency ORDER BY effective_from) AS effective_to
  FROM flux.fx_rates
),
with_fx AS (
  SELECT raw.*, fx.effective_from AS fx_from
  FROM raw
  LEFT JOIN fx_windows AS fx
    ON  fx.currency = raw.currency_std
    AND DATE(raw.created_ts) >= fx.effective_from
    AND (fx.effective_to IS NULL OR DATE(raw.created_ts) < fx.effective_to)
),
-- ids where more than one raw row shares the latest updated_at, i.e. the
-- naive "latest by updated_at" dedup would still return duplicates
current_ties AS (
  SELECT booking_id,
         COUNT(*) AS n_rows,
         COUNT(DISTINCT CONCAT(IFNULL(revenue, ''), '|', IFNULL(num_guests, ''), '|',
                               booking_status, '|', check_in, '|', check_out)) AS n_versions
  FROM (
    SELECT * FROM raw
    WHERE TRUE
    QUALIFY RANK() OVER (PARTITION BY booking_id ORDER BY updated_ts DESC) = 1
  )
  GROUP BY booking_id
  HAVING COUNT(*) > 1
)
SELECT check_name, severity, failing, description FROM (
  SELECT 1 AS ord, 'raw_row_count' AS check_name, 'info' AS severity,
         COUNT(*) AS failing, 'rows landed across all batches' AS description FROM raw
  UNION ALL
  SELECT 2, 'exact_duplicate_rows', 'warn',
         COUNT(*) - COUNT(DISTINCT TO_JSON_STRING(STRUCT(booking_id, property_id, check_in,
           check_out, num_guests, room_rate, currency, revenue, booking_channel,
           booking_status, created_at, updated_at))),
         'identical re-sent rows (merge treats them as no-ops)' FROM raw
  UNION ALL
  SELECT 3, 'booking_ids_with_multiple_rows', 'info',
         COUNT(*) - COUNT(DISTINCT booking_id), 'extra rows beyond one per booking_id' FROM raw
  UNION ALL
  SELECT 4, 'duplicate_current_version_ids', 'warn', COUNT(*),
         'ids with >1 row at the latest updated_at (dedup needs a tie-break)' FROM current_ties
  UNION ALL
  SELECT 5, 'conflicting_current_versions', 'error', COUNTIF(n_versions > 1),
         'ties at the latest updated_at whose content differs' FROM current_ties
  UNION ALL
  SELECT 6, 'orphan_property_ids', 'error', COUNT(*),
         'property_id not in properties dimension'
  FROM raw WHERE property_id_std NOT IN (SELECT property_id FROM flux.properties)
  UNION ALL
  SELECT 7, 'missing_revenue', 'error', COUNTIF(revenue_n IS NULL),
         'null or unparseable revenue' FROM raw
  UNION ALL
  SELECT 8, 'non_positive_revenue', 'error', COUNTIF(revenue_n <= 0),
         'revenue <= 0' FROM raw
  UNION ALL
  SELECT 9, 'unparseable_stay_dates', 'error',
         COUNTIF(check_in_d IS NULL OR check_out_d IS NULL),
         'check_in/check_out matching none of the known formats' FROM raw
  UNION ALL
  SELECT 10, 'invalid_date_ranges', 'error', COUNTIF(check_out_d <= check_in_d),
         'check_out on or before check_in' FROM raw
  UNION ALL
  SELECT 11, 'invalid_num_guests', 'error',
         COUNTIF(guests_n IS NULL OR guests_n < 1 OR guests_n > 12),
         'guests missing or outside 1-12' FROM raw
  UNION ALL
  SELECT 12, 'updated_before_created', 'error', COUNTIF(updated_ts < created_ts),
         'updated_at earlier than created_at' FROM raw
  UNION ALL
  SELECT 13, 'unknown_currency', 'error',
         COUNTIF(currency_std NOT IN (SELECT DISTINCT currency FROM flux.fx_rates)),
         'currency not in fx_rates after standardisation' FROM raw
  UNION ALL
  SELECT 14, 'no_applicable_fx_rate', 'error', COUNTIF(fx_from IS NULL),
         'no FX rate effective on created_at date (incl. unknown currency)' FROM with_fx
  UNION ALL
  SELECT 15, 'non_canonical_channel_values', 'warn',
         COUNTIF(booking_channel NOT IN ('Direct', 'Website', 'OTA', 'Travel Agent')),
         'channel needs standardising (case/whitespace/punctuation)' FROM raw
  UNION ALL
  SELECT 16, 'non_iso_currency_values', 'warn',
         COUNTIF(currency NOT IN ('USD', 'EUR', 'GBP', 'ZAR')),
         'currency given as symbol or wrong case' FROM raw
  UNION ALL
  SELECT 17, 'fct_duplicate_booking_ids', 'error',
         COUNT(*) - COUNT(DISTINCT booking_id),
         'post-load assertion: fact must have one row per booking' FROM flux.fct_bookings
)
ORDER BY ord;
