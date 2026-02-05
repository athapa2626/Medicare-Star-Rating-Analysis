import pandas as pd
from sqlalchemy import create_engine, text

# Database connection
DB_CONFIG = {
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': '5432',
    'database': 'medicare_star_ratings'
}

def get_db_engine():
    """Create SQLAlchemy engine for database connection"""
    connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    return create_engine(connection_string)

def transform_contracts():
    """Transform staging_summary_ratings into contracts table"""
    print("Transforming contracts...")
    
    engine = get_db_engine()
    
    # Use SQL to insert from staging to production
    query = text("""
        INSERT INTO contracts (
            contract_id, organization_type, contract_name, 
            marketing_name, parent_organization, snp_flag,
            part_c_summary_star, part_d_summary_star, 
            overall_star_rating, year
        )
        SELECT 
            contract_id, organization_type, contract_name,
            marketing_name, parent_organization, snp_flag,
            CASE WHEN part_c_summary_star ~ '^[0-9.]+$' 
                 THEN part_c_summary_star::NUMERIC(2,1) 
                 ELSE NULL END as part_c_summary_star,
            CASE WHEN part_d_summary_star ~ '^[0-9.]+$' 
                 THEN part_d_summary_star::NUMERIC(2,1) 
                 ELSE NULL END as part_d_summary_star,
            CASE WHEN overall_star_rating ~ '^[0-9.]+$' 
                 THEN overall_star_rating::NUMERIC(2,1) 
                 ELSE NULL END as overall_star_rating,
            year
        FROM staging_summary_ratings
        ON CONFLICT (contract_id) DO NOTHING;
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query)
        conn.commit()
        print(f"Inserted {result.rowcount} contracts")
    
    # Verify
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM contracts")).scalar()
        print(f"Total contracts in production table: {count}")

def transform_measure_metadata():
    """Extract measure metadata from staging table columns"""
    print("\nTransforming measure metadata...")
    
    engine = get_db_engine()
    
    # Check if data already exists
    with engine.connect() as conn:
        existing_count = conn.execute(text("SELECT COUNT(*) FROM measure_metadata")).scalar()
        if existing_count > 0:
            print(f"Measure metadata already exists ({existing_count} records), skipping...")
            return
    
    # Get all measure columns from staging
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'staging_measure_stars'
            AND column_name NOT IN ('CONTRACT_ID', 'Organization Type', 'Contract Name', 
                                   'Organization Marketing Name', 'Parent Organization', 
                                   'year')
            AND column_name NOT LIKE 'unused_col_%'
            ORDER BY ordinal_position;
        """))
        measure_columns = [row[0] for row in result]
    
    print(f"Found {len(measure_columns)} measures")
    
    # Build metadata records
    metadata_records = []
    for measure_col in measure_columns:
        # Extract measure ID and name
        if ':' in measure_col:
            parts = measure_col.split(':', 1)
            measure_id = parts[0].strip()
            measure_name = parts[1].strip() if len(parts) > 1 else measure_col
        else:
            measure_id = measure_col
            measure_name = measure_col
        
        # Determine domain and type based on measure ID prefix
        if measure_id.startswith('C'):
            domain = 'Part C'
            measure_type = 'Health Plan'
        elif measure_id.startswith('D'):
            domain = 'Part D'
            measure_type = 'Drug Plan'
        else:
            domain = 'Unknown'
            measure_type = 'Unknown'
        
        metadata_records.append({
            'measure_id': measure_id,
            'measure_name': measure_name,
            'domain': domain,
            'measure_type': measure_type,
            'weight': 1.0  # Default weight, can be updated later
        })
    
    # Convert to DataFrame and load
    df = pd.DataFrame(metadata_records)
    df.to_sql('measure_metadata', engine, if_exists='append', index=False)
    
    print(f"Inserted {len(df)} measure metadata records")
    
    # Verify
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM measure_metadata")).scalar()
        print(f"Total measures in production table: {count}")

def transform_measure_scores():
    """Transform wide measure_stars data into long format measure_scores"""
    print("\nTransforming measure scores (wide → long)...")
    
    engine = get_db_engine()
    
    # First, get the measure column names from staging table
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'staging_measure_stars'
            AND column_name NOT IN ('CONTRACT_ID', 'Organization Type', 'Contract Name', 
                                   'Organization Marketing Name', 'Parent Organization', 
                                   'year', 'unused_col_1', 'unused_col_2', 'unused_col_3')
            AND column_name NOT LIKE 'unused_col_%'
            ORDER BY ordinal_position;
        """))
        measure_columns = [row[0] for row in result]
    
    print(f"Found {len(measure_columns)} measure columns")
    print(f"Sample measures: {measure_columns[:5]}")
    
    # Build dynamic UNPIVOT query
    # For each measure column, create a UNION ALL clause
    union_parts = []
    for measure_col in measure_columns:
        # Extract measure ID (e.g., "C01" from "C01: Breast Cancer Screening")
        measure_id = measure_col.split(':')[0].strip() if ':' in measure_col else measure_col
        
        union_parts.append(f"""
            SELECT 
                "CONTRACT_ID" as contract_id,
                '{measure_id}' as measure_id,
                CASE 
                    WHEN "{measure_col}" ~ '^[0-9.]+$' THEN "{measure_col}"::NUMERIC(3,1)
                    ELSE NULL 
                END as score,
                year
            FROM staging_measure_stars
            WHERE "{measure_col}" IS NOT NULL 
            AND "{measure_col}" != ''
        """)
    
    full_query = " UNION ALL ".join(union_parts)
    
    insert_query = f"""
        INSERT INTO measure_scores (contract_id, measure_id, score, year)
        {full_query}
        ON CONFLICT (contract_id, measure_id, year) DO NOTHING;
    """
    
    print("Executing unpivot transformation...")
    with engine.connect() as conn:
        result = conn.execute(text(insert_query))
        conn.commit()
        print(f"Inserted {result.rowcount} measure scores")
    
    # Verify
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM measure_scores")).scalar()
        print(f"Total measure scores in production table: {count}")

def transform_cut_points():
    """Transform cut points from staging to production"""
    print("\nTransforming cut points...")
    
    engine = get_db_engine()
    
    # Check if data already exists
    with engine.connect() as conn:
        existing_count = conn.execute(text("SELECT COUNT(*) FROM cut_points")).scalar()
        if existing_count > 0:
            print(f"Cut points already exist ({existing_count} records), skipping...")
            return
    
    # Load staging data
    df_c = pd.read_sql("SELECT * FROM staging_part_c_cutpoints", engine)
    df_d = pd.read_sql("SELECT * FROM staging_part_d_cutpoints", engine)
    
    measure_cols_c = [col for col in df_c.columns if col not in ['Number of Stars Displayed on the Plan Finder Tool', 'year'] and not col.startswith('Unnamed')]
    measure_cols_d = [col for col in df_d.columns if col not in ['Org Type', 'Number of Stars Displayed on the Plan Finder Tool', 'year'] and not col.startswith('Unnamed')]
    
    print(f"  Found {len(measure_cols_c)} Part C measures with cut points")
    print(f"  Found {len(measure_cols_d)} Part D measures with cut points")
    
    # Transform Part C
    cut_point_records = []
    for col in measure_cols_c:
        # Extract measure ID
        measure_id = col.split(':')[0].strip() if ':' in col else col
        
        # Skip first 2 rows (headers), process star level rows
        for idx in range(2, len(df_c)):
            star_label = df_c.iloc[idx, 0]  # First column has star labels
            if pd.notna(star_label) and 'star' in str(star_label).lower():
                # Extract numeric star level (e.g., "1star" -> 1.0, "2star" -> 2.0)
                star_num = star_label.strip().lower().replace('star', '').replace(' ', '')
                if star_num:
                    try:
                        star_level = float(star_num)
                    except:
                        continue
                    
                    # Get cut point value
                    cut_val = df_c[col].iloc[idx]
                    if pd.notna(cut_val) and str(cut_val).strip():
                        # Clean the cut point value (remove %, <, >, etc.)
                        cut_str = str(cut_val).strip().replace('%', '').replace('<', '').replace('>', '').replace(' ', '')
                        try:
                            cut_point = float(cut_str)
                            cut_point_records.append({
                                'measure_id': measure_id,
                                'star_level': star_level,
                                'cut_point': cut_point,
                                'year': 2024
                            })
                        except:
                            pass
    
    # Transform Part D (same logic)
    for col in measure_cols_d:
        measure_id = col.split(':')[0].strip() if ':' in col else col
        
        for idx in range(2, len(df_d)):
            star_label = df_d.iloc[idx, 1]  #Second column has star labels for Part D
            if pd.notna(star_label) and 'star' in str(star_label).lower():
                star_num = star_label.strip().lower().replace('star', '').replace(' ', '')
                if star_num:
                    try:
                        star_level = float(star_num)
                    except:
                        continue
                    
                    cut_val = df_d[col].iloc[idx]
                    if pd.notna(cut_val) and str(cut_val).strip():
                        cut_str = str(cut_val).strip().replace('%', '').replace('<', '').replace('>', '').replace(' ', '')
                        try:
                            cut_point = float(cut_str)
                            cut_point_records.append({
                                'measure_id': measure_id,
                                'star_level': star_level,
                                'cut_point': cut_point,
                                'year': 2024
                            })
                        except:
                            pass
    
    # Get valid measure IDs from metadata
    with engine.connect() as conn:
        valid_measures = conn.execute(text("SELECT measure_id FROM measure_metadata")).fetchall()
        valid_measure_ids = [row[0] for row in valid_measures]
    
    if cut_point_records:
        df_cut = pd.DataFrame(cut_point_records)
        
        # Filter to only keep cut points for measures that exist in measure_metadata
        df_cut = df_cut[df_cut['measure_id'].isin(valid_measure_ids)]
        print(f"  Filtered to {len(df_cut)} valid cut points (matching existing measures)")
        
        # Remove duplicates before inserting
        df_cut = df_cut.drop_duplicates(subset=['measure_id', 'star_level', 'year'])
        
        if len(df_cut) > 0:
            df_cut.to_sql('cut_points', engine, if_exists='append', index=False)
            print(f"Inserted {len(df_cut)} cut point records")
        else:
            print("No valid cut points after filtering")
    else:
        print("No cut points extracted")
    
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM cut_points")).scalar()
        print(f"Total cut points in production table: {count}")

if __name__ == "__main__":
    transform_contracts()
    transform_measure_metadata()
    transform_measure_scores()
    transform_cut_points()