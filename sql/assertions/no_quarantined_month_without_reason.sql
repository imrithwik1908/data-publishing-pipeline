SELECT b.batch_id
FROM batches b
WHERE b.status = 'quarantined'
  AND NOT EXISTS (SELECT 1 FROM check_results r
                  WHERE r.batch_id = b.batch_id AND r.status = 'fail' AND r.severity = 'ERROR');
