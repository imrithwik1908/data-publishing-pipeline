SELECT 'dim_zone' AS dim, zone_id AS k FROM dim_zone GROUP BY 2 HAVING count(*) > 1
UNION ALL SELECT 'dim_payment_type', payment_type FROM dim_payment_type GROUP BY 2 HAVING count(*) > 1
UNION ALL SELECT 'dim_vendor', vendor_id FROM dim_vendor GROUP BY 2 HAVING count(*) > 1
UNION ALL SELECT 'dim_date', year(date_day) * 10000 + month(date_day) * 100 + day(date_day)
          FROM dim_date GROUP BY 2 HAVING count(*) > 1;
