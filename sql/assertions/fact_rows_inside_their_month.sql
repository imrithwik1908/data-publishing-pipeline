SELECT pickup_month, count(*) AS bad
FROM fact_trips
WHERE date_trunc('month', tpep_pickup_datetime)::DATE <> pickup_month
GROUP BY 1;
