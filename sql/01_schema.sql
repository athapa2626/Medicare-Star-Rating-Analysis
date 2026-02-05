
CREATE TABLE IF NOT EXISTS contracts (
    contract_id TEXT PRIMARY KEY,
    organization_type TEXT,
    contract_name TEXT,
    marketing_name TEXT,
    parent_organization TEXT,
    snp_flag TEXT,
    part_c_summary_star NUMERIC(2,1),
    part_d_summary_star NUMERIC(2,1),
    overall_star_rating NUMERIC(2,1),
    year INT
);

CREATE TABLE IF NOT EXISTS measure_metadata (
    measure_id TEXT PRIMARY KEY,
    measure_name TEXT,
    domain TEXT,
    measure_type TEXT,
    weight NUMERIC(3,1)
);

CREATE TABLE IF NOT EXISTS measure_scores (
    contract_id TEXT REFERENCES contracts(contract_id),
    measure_id TEXT REFERENCES measure_metadata(measure_id),
    score NUMERIC(3,1),
    year INT,
    PRIMARY KEY (contract_id, measure_id, year)
);

CREATE TABLE IF NOT EXISTS cut_points (
    measure_id TEXT REFERENCES measure_metadata(measure_id),
    star_level NUMERIC(2,1),
    cut_point NUMERIC(5,2),
    year INT,
    PRIMARY KEY (measure_id, star_level, year)
);


CREATE TABLE IF NOT EXISTS staging_summary_ratings (
    contract_id TEXT,
    organization_type TEXT,
    contract_name TEXT,
    marketing_name TEXT,
    parent_organization TEXT,
    snp_flag TEXT,
    part_c_summary_star NUMERIC(2,1),
    part_d_summary_star NUMERIC(2,1),
    overall_star_rating NUMERIC(2,1),
    year INT
);

CREATE TABLE IF NOT EXISTS staging_measure_stars (
    contract_id TEXT,
    organization_type TEXT,
    contract_name TEXT,
    marketing_name TEXT,
    parent_organization TEXT,
    snp_flag TEXT,
    year INT
);

CREATE TABLE IF NOT EXISTS staging_part_c_cutpoints (
    measure_id TEXT,
    star_level NUMERIC(2,1),
    cut_point NUMERIC(5,2),
    year INT
);

CREATE TABLE IF NOT EXISTS staging_part_d_cutpoints (
    measure_id TEXT,
    star_level NUMERIC(2,1),
    cut_point NUMERIC(5,2),
    year INT
);



CREATE INDEX IF NOT EXISTS idx_contracts_overall_rating ON contracts(overall_star_rating);
CREATE INDEX IF NOT EXISTS idx_measure_scores_contract ON measure_scores(contract_id);
CREATE INDEX IF NOT EXISTS idx_measure_scores_measure ON measure_scores(measure_id);
CREATE INDEX IF NOT EXISTS idx_cut_points_lookup ON cut_points(measure_id, star_level);