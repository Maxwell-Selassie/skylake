-- Business tables (materialized views) built on top of silver/gold tables
-- Requires a Pro or Serverless SQL warehouse for creation and refresh

-- 1. Booking fact, enriched with passenger, flight, and airport details (current state via SCD1 dims)

CREATE OR REPLACE MATERIALIZED VIEW mxs_skylake.mxs_skylake_gold.mv_booking_details
COMMENT 'One row per booking with passenger, flight, and airport attributes'
SCHEDULE EVERY 1 HOUR
AS
SELECT
    b.booking_id,
    b.passenger_id,
    p.name    AS passenger_name,
    p.nationality,
    p.gender,
    b.flight_id,
    f.airline,
    f.origin,
    f.destination,
    f.flight_date,
    b.airport_id,
    a.airport_name,
    a.city            AS airport_city,
    a.country         AS airport_country,
    b.amount,
    b.booking_date
FROM mxs_skylake.mxs_skylake_silver.bookings_silver b
LEFT JOIN mxs_skylake.mxs_skylake_gold.passengers_scd1 p
    ON b.passenger_id = p.passenger_id
LEFT JOIN mxs_skylake.mxs_skylake_gold.flights_scd1 f
    ON b.flight_id = f.flight_id
LEFT JOIN mxs_skylake.mxs_skylake_gold.airport_scd1 a
    ON b.airport_id = a.airport_id;

-- 2. Revenue and booking volume by route and airline
CREATE OR REPLACE MATERIALIZED VIEW mxs_skylake.mxs_skylake_gold.mv_route_performance
COMMENT 'Booking counts and revenue aggregated by airline and route'
SCHEDULE EVERY 1 HOUR
AS
SELECT
    f.airline,
    f.origin,
    f.destination,
    COUNT(b.booking_id)      AS total_bookings,
    SUM(b.amount)            AS total_revenue,
    AVG(b.amount)            AS avg_booking_amount,
    MIN(b.booking_date)      AS first_booking_date,
    MAX(b.booking_date)     AS last_booking_date
FROM mxs_skylake.mxs_skylake_silver.bookings_silver b
INNER JOIN mxs_skylake.mxs_skylake_gold.flights_scd1 f
    ON b.flight_id = f.flight_id
GROUP BY f.airline, f.origin, f.destination;

-- 3. Airport traffic summary (bookings and revenue per airport)
CREATE OR REPLACE MATERIALIZED VIEW mxs_skylake.mxs_skylake_gold.mv_airport_traffic
COMMENT 'Booking volume and revenue per airport'
SCHEDULE EVERY 1 HOUR
AS
SELECT
    a.airport_id,
    a.airport_name,
    a.city,
    a.country,
    COUNT(b.booking_id)  AS total_bookings,
    SUM(b.amount)        AS total_revenue,
    AVG(b.amount)        AS avg_booking_amount
FROM mxs_skylake.mxs_skylake_silver.bookings_silver b
INNER JOIN mxs_skylake.mxs_skylake_gold.airport_scd1 a
    ON b.airport_id = a.airport_id
GROUP BY a.airport_id, a.airport_name, a.city, a.country;

-- 4. Passenger 360: spending and travel behaviour per passenger
CREATE OR REPLACE MATERIALIZED VIEW mxs_skylake.mxs_skylake_gold.mv_passenger_summary
COMMENT 'Booking counts, spend, and travel stats per passenger'
SCHEDULE EVERY 1 HOUR
AS
SELECT
    p.passenger_id,
    p.name,
    p.nationality,
    p.gender,
    COUNT(b.booking_id)                        AS total_bookings,
    SUM(b.amount)                              AS total_spend,
    AVG(b.amount)                              AS avg_booking_amount,
    COUNT(DISTINCT b.flight_id)                AS distinct_flights,
    COUNT(DISTINCT f.destination)              AS distinct_destinations,
    MIN(b.booking_date)                        AS first_booking_date,
    MAX(b.booking_date)                        AS last_booking_date
FROM mxs_skylake.mxs_skylake_silver.bookings_silver b
INNER JOIN mxs_skylake.mxs_skylake_gold.passengers_scd1 p
    ON b.passenger_id = p.passenger_id
LEFT JOIN mxs_skylake.mxs_skylake_gold.flights_scd1 f
    ON b.flight_id = f.flight_id
GROUP BY p.passenger_id, p.name, p.nationality, p.gender;

-- 5. Daily revenue trend
CREATE OR REPLACE MATERIALIZED VIEW mxs_skylake.mxs_skylake_gold.mv_daily_revenue
COMMENT 'Bookings and revenue per booking date'
SCHEDULE EVERY 1 HOUR
AS
SELECT
    booking_date,
    COUNT(booking_id) AS total_bookings,
    SUM(amount)       AS total_revenue,
    AVG(amount)       AS avg_booking_amount
FROM mxs_skylake.mxs_skylake_silver.bookings_silver
GROUP BY booking_date;

-- 6. Airline market share (revenue share across all bookings)
CREATE OR REPLACE MATERIALIZED VIEW mxs_skylake.mxs_skylake_gold.mv_airline_market_share
COMMENT 'Revenue and booking share per airline'
SCHEDULE EVERY 1 HOUR
AS
WITH airline_totals AS (
    SELECT
        f.airline,
        COUNT(b.booking_id) AS bookings,
        SUM(b.amount)       AS revenue
    FROM mxs_skylake.mxs_skylake_silver.bookings_silver b
    INNER JOIN mxs_skylake.mxs_skylake_gold.flights_scd1 f
        ON b.flight_id = f.flight_id
    GROUP BY f.airline
)
SELECT
    airline,
    bookings,
    revenue,
    ROUND(revenue * 100.0 / SUM(revenue) OVER (), 2) AS revenue_share_pct,
    ROUND(bookings * 100.0 / SUM(bookings) OVER (), 2) AS booking_share_pct
FROM airline_totals;

-- Example: point-in-time view using the SCD2 dimension (as-of historical attributes)
-- Swap SCD1 for SCD2 in mv_booking_details and filter current records like this:
--   WHERE p.__END_AT._ingested_at IS NULL

-- SELECT * FROM mxs_skylake.mxs_skylake_gold.mv_booking_details LIMIT 10;