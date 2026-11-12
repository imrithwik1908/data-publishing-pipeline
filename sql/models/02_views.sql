-- Analytic views on top of the star schema.

-- Trips joined to their dimensions: the friendly table for analysts.
CREATE OR REPLACE VIEW v_trips AS
SELECT f.*,
       pz.borough AS pickup_borough, pz.zone_name AS pickup_zone,
       dz.borough AS dropoff_borough, dz.zone_name AS dropoff_zone,
       p.payment_name, v.vendor_name,
       f.tpep_pickup_datetime::DATE AS pickup_date
FROM fact_trips f
LEFT JOIN dim_zone pz ON pz.zone_id = f.PULocationID
LEFT JOIN dim_zone dz ON dz.zone_id = f.DOLocationID
LEFT JOIN dim_payment_type p ON p.payment_type = f.payment_type
LEFT JOIN dim_vendor v ON v.vendor_id = f.VendorID;

-- Monthly revenue per pickup zone, ranked within each month.
CREATE OR REPLACE VIEW v_monthly_zone_revenue AS
SELECT f.pickup_month,
       z.borough, z.zone_name,
       count(*) AS trips,
       round(sum(f.total_amount), 2) AS revenue,
       RANK() OVER (PARTITION BY f.pickup_month ORDER BY sum(f.total_amount) DESC) AS revenue_rank
FROM fact_trips f
JOIN dim_zone z ON z.zone_id = f.PULocationID
GROUP BY f.pickup_month, z.borough, z.zone_name;

-- Top 10 pickup zones per month.
CREATE OR REPLACE VIEW v_top10_zones AS
SELECT * FROM v_monthly_zone_revenue WHERE revenue_rank <= 10;

-- Month-over-month change with LAG().
CREATE OR REPLACE VIEW v_monthly_summary AS
WITH m AS (
    SELECT pickup_month, count(*) AS trips, round(sum(total_amount), 2) AS revenue,
           round(avg(trip_distance), 3) AS avg_distance
    FROM fact_trips GROUP BY pickup_month
)
SELECT *,
       LAG(trips) OVER w AS prev_trips,
       round(100.0 * (trips - LAG(trips) OVER w) / LAG(trips) OVER w, 2) AS trips_mom_pct,
       round(100.0 * (revenue - LAG(revenue) OVER w) / LAG(revenue) OVER w, 2) AS revenue_mom_pct
FROM m
WINDOW w AS (ORDER BY pickup_month);

-- Daily revenue with a running total that restarts each month.
CREATE OR REPLACE VIEW v_daily_running_revenue AS
SELECT d.date_day, d.day_name, d.is_weekend,
       coalesce(t.trips, 0) AS trips,
       coalesce(t.revenue, 0) AS revenue,
       round(sum(coalesce(t.revenue, 0)) OVER (PARTITION BY d.month_start ORDER BY d.date_day
                                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS month_to_date_revenue
FROM dim_date d
LEFT JOIN (
    SELECT tpep_pickup_datetime::DATE AS day, count(*) AS trips, round(sum(total_amount), 2) AS revenue
    FROM fact_trips GROUP BY 1
) t ON t.day = d.date_day;

-- Data-quality history: one row per batch.
CREATE OR REPLACE VIEW v_batch_quality AS
SELECT b.batch_id, b.month, b.status, b.mode, b.row_count, b.rejected_rows,
       round(100.0 * b.rejected_rows / nullif(b.row_count, 0), 4) AS rejected_pct,
       count(*) FILTER (WHERE r.status = 'fail' AND r.severity = 'ERROR') AS errors,
       count(*) FILTER (WHERE r.status = 'fail' AND r.severity = 'WARN') AS warnings,
       b.contract_version, b.created_at, b.finished_at, b.message
FROM batches b LEFT JOIN check_results r USING (batch_id)
GROUP BY ALL;
