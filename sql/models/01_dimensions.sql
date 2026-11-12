-- Dimension tables. Rebuilt inside every publish transaction, so they always match the facts.
-- Codes come from the TLC yellow taxi data dictionary.

CREATE OR REPLACE TABLE dim_zone AS
SELECT LocationID::INTEGER AS zone_id, Borough AS borough, Zone AS zone_name, service_zone
FROM read_csv('{zone_csv}', header = true);

CREATE OR REPLACE TABLE dim_payment_type AS
SELECT * FROM (VALUES
    (0, 'Flex fare trip'), (1, 'Credit card'), (2, 'Cash'), (3, 'No charge'),
    (4, 'Dispute'), (5, 'Unknown'), (6, 'Voided trip')
) t(payment_type, payment_name);

CREATE OR REPLACE TABLE dim_vendor AS
SELECT * FROM (VALUES
    (1, 'Creative Mobile Technologies'), (2, 'Curb Mobility'),
    (6, 'Myle Technologies'), (7, 'Helix')
) t(vendor_id, vendor_name);

CREATE OR REPLACE TABLE dim_rate_code AS
SELECT * FROM (VALUES
    (1, 'Standard rate'), (2, 'JFK'), (3, 'Newark'), (4, 'Nassau or Westchester'),
    (5, 'Negotiated fare'), (6, 'Group ride'), (99, 'Unknown')
) t(rate_code_id, rate_code_name);

-- One row per calendar day covering every published month (empty when nothing is published).
CREATE OR REPLACE TABLE dim_date AS
SELECT d::DATE AS date_day,
       date_trunc('month', d)::DATE AS month_start,
       year(d) AS year, month(d) AS month, day(d) AS day_of_month,
       isodow(d) AS iso_day_of_week, dayname(d) AS day_name,
       isodow(d) IN (6, 7) AS is_weekend
FROM (
    SELECT unnest(generate_series(min(pickup_month), max(pickup_month) + INTERVAL 1 MONTH - INTERVAL 1 DAY,
                                  INTERVAL 1 DAY)) AS d
    FROM fact_trips
    HAVING count(*) > 0
);
