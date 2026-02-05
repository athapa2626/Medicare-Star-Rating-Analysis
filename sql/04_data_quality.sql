-- Data quality checks to run after ETL

-- Check for contracts with no overall rating
SELECT COUNT(*) as contracts_missing_rating 
FROM contracts 
WHERE overall_star_rating IS NULL;

-- Check for orphaned measure scores (measures without metadata)
SELECT COUNT(*) as orphaned_scores
FROM measure_scores ms
LEFT JOIN measure_metadata mm ON ms.measure_id = mm.measure_id
WHERE mm.measure_id IS NULL;

-- Verify expected record counts
SELECT 
    'contracts' as table_name, COUNT(*) as record_count 
FROM contracts
UNION ALL
SELECT 'measure_metadata', COUNT(*) FROM measure_metadata
UNION ALL
SELECT 'measure_scores', COUNT(*) FROM measure_scores
UNION ALL
SELECT 'cut_points', COUNT(*) FROM cut_points;