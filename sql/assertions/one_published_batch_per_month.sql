SELECT month, count(*) AS published_batches
FROM batches WHERE status = 'published'
GROUP BY 1 HAVING count(*) <> 1;
