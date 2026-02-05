-- Update measure weights based on CMS methodology
-- Weight = 3 for outcome measures
-- Weight = 1 for process measures

UPDATE measure_metadata SET weight = 3.0 
WHERE measure_id IN ('C09', 'C10', 'C11', 'C15', 'C16', 'D08', 'D09', 'D10');

-- Weight = 1.5 or 4 for CAHPS (patient experience)
UPDATE measure_metadata SET weight = 1.5
WHERE measure_id IN ('C19', 'C20', 'C21', 'C22', 'C23', 'C24');