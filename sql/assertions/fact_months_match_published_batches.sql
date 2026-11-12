-- Every month in the fact table has a published batch, and the row counts agree.
SELECT coalesce(f.m, b.m) AS month, f.n AS fact_rows, b.n AS batch_rows
FROM (SELECT strftime(pickup_month, '%Y-%m') AS m, count(*) AS n FROM fact_trips GROUP BY 1) f
FULL JOIN (SELECT month AS m, row_count - rejected_rows AS n FROM batches WHERE status = 'published') b
  ON f.m = b.m
WHERE f.n IS DISTINCT FROM b.n;
