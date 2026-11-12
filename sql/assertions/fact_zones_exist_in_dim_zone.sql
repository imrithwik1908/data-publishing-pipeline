SELECT DISTINCT f.PULocationID
FROM fact_trips f LEFT JOIN dim_zone z ON z.zone_id = f.PULocationID
WHERE z.zone_id IS NULL;
