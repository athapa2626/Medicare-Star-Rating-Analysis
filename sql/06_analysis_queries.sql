
-- QUERY 1: Find all 3.5-star plans 
SELECT 
    contract_id,
    contract_name,
    overall_star_rating
FROM contracts
WHERE overall_star_rating = 3.5
ORDER BY contract_name;

-- QUERY 2: Show worst performing measures for 3.5-star plans
SELECT 
    c.contract_id,
    c.contract_name,
    ms.measure_id,
    mm.measure_name,
    ms.score,
    mm.weight
FROM contracts c
JOIN measure_scores ms ON c.contract_id = ms.contract_id
JOIN measure_metadata mm ON ms.measure_id = mm.measure_id
WHERE c.overall_star_rating = 3.5
  AND ms.score IS NOT NULL
  AND ms.score < 4.0
ORDER BY c.contract_id, mm.weight DESC, ms.score ASC
LIMIT 50;

-- QUERY 3: Prioritization Matrix
WITH plan_opportunities AS (
    SELECT 
        ms.measure_id,
        mm.measure_name,
        mm.weight,
        AVG(ms.score) as avg_score,
        COUNT(*) as num_plans_struggling
    FROM measure_scores ms
    JOIN measure_metadata mm ON ms.measure_id = mm.measure_id
    JOIN contracts c ON ms.contract_id = c.contract_id
    WHERE c.overall_star_rating = 3.5
      AND ms.score IS NOT NULL
      AND ms.score < 4.0
    GROUP BY ms.measure_id, mm.measure_name, mm.weight
)
SELECT 
    measure_id,
    measure_name,
    weight,
    ROUND(avg_score, 2) as avg_score,
    num_plans_struggling,
    ROUND(weight * (4.0 - avg_score), 2) as improvement_potential
FROM plan_opportunities
ORDER BY improvement_potential DESC
LIMIT 20;

-- QUERY 4: Specific plan recommendation (Arizona Physicians IPA)
SELECT 
    c.contract_name,
    ms.measure_id,
    mm.measure_name,
    ms.score as current_score,
    mm.weight,
    ROUND(mm.weight * (4.0 - ms.score), 2) as impact_if_improved_to_4,
    CASE 
        WHEN ms.score <= 2.0 THEN 'CRITICAL'
        WHEN ms.score < 4.0 THEN 'HIGH'
        ELSE 'MEDIUM'
    END as priority
FROM contracts c
JOIN measure_scores ms ON c.contract_id = ms.contract_id
JOIN measure_metadata mm ON ms.measure_id = mm.measure_id
WHERE c.contract_id = 'H0107'  -- Health Care Service Corporation
  AND ms.score IS NOT NULL
  AND ms.score < 4.0
ORDER BY impact_if_improved_to_4 DESC
LIMIT 10;

SELECT 
    c.contract_name,
    ms.measure_id,
    mm.measure_name,
    ms.score as current_score,
    mm.weight,
    ROUND(mm.weight * (4.0 - ms.score), 2) as impact_if_improved_to_4,
    CASE 
        WHEN ms.score <= 2.0 THEN 'CRITICAL - Immediate action needed'
        WHEN ms.score < 3.0 THEN 'HIGH - Major improvement opportunity'
        WHEN ms.score < 4.0 THEN 'MEDIUM - Incremental gains'
        ELSE 'LOW - Already strong'
    END as priority
FROM contracts c
JOIN measure_scores ms ON c.contract_id = ms.contract_id
LEFT JOIN measure_metadata mm ON ms.measure_id = mm.measure_id
WHERE TRIM(c.contract_id) = 'H0907'
  AND ms.score IS NOT NULL
  AND ms.score < 4.0
ORDER BY impact_if_improved_to_4 DESC
LIMIT 10;
