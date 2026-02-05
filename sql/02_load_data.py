import pandas as pd
from sqlalchemy import create_engine

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

def load_summary_ratings():
    """Load Summary Rating data into staging table"""
    print("Loading Summary Ratings...")
    
    file_path = "data/raw/2024 Star Ratings Data Table - (Jul 2 2024)_0/2024 Star Ratings Data Table - Summary Rating (Jul 2 2024).csv"
    
    # Skip the first row (title row) and use row 1 as header
    df = pd.read_csv(file_path, skiprows=1)
    
    # Rename columns to match our staging table
    df = df.rename(columns={
        'Contract Number': 'contract_id',
        'Organization Type': 'organization_type',
        'Contract Name': 'contract_name',
        'Organization Marketing Name': 'marketing_name',
        'Parent Organization': 'parent_organization',
        'SNP': 'snp_flag',
        '2024 Part C Summary': 'part_c_summary_star',
        '2024 Part D Summary': 'part_d_summary_star',
        '2024 Overall': 'overall_star_rating'
    })
    
    # Select only the columns we need
    columns_to_keep = [
        'contract_id', 'organization_type', 'contract_name', 
        'marketing_name', 'parent_organization', 'snp_flag',
        'part_c_summary_star', 'part_d_summary_star', 'overall_star_rating'
    ]
    df = df[columns_to_keep]
    
    df['year'] = 2024  # Add year column
    
    print(f"Loaded {len(df)} rows")
    print(f"\nSample data:")
    print(df.head())
    
    # Load to database
    engine = get_db_engine()
    df.to_sql('staging_summary_ratings', engine, if_exists='replace', index=False)
    
    print("\nSummary ratings loaded successfully!")
    return df

def load_measure_stars():
    """Load Measure Stars data into staging table"""
    print("Loading Measure Stars...")
    
    file_path = "data/raw/2024 Star Ratings Data Table - (Jul 2 2024)_0/2024 Star Ratings Data Table - Measure Stars (Jul 2 2024).csv"
    
    # Read without headers first
    df_raw = pd.read_csv(file_path, encoding='latin-1', header=None)
    
    # Extract column names from multiple rows
    # Row 1 has contract info column names
    contract_cols = df_raw.iloc[1, :5].tolist()
    # Row 2 has measure IDs
    measure_cols = df_raw.iloc[2, 5:].tolist()
    
    # Combine them
    all_columns = contract_cols + measure_cols
    
    # Clean column names - replace NaN with placeholder names
    cleaned_columns = []
    nan_counter = 1
    for col in all_columns:
        if pd.isna(col) or str(col).strip() == '' or str(col) == 'nan':
            cleaned_columns.append(f'unused_col_{nan_counter}')
            nan_counter += 1
        else:
            cleaned_columns.append(str(col))
    
    # Now read the data starting from row 4 (index 4)
    df = pd.read_csv(file_path, encoding='latin-1', skiprows=4, header=None)
    
    # Assign cleaned column names
    df.columns = cleaned_columns
    
    print(f"Loaded {len(df)} rows")
    print(f"Total columns: {len(df.columns)}")
    
    # Show first 10 column names
    print(f"\nFirst 10 columns:")
    for i, col in enumerate(df.columns[:10]):
        print(f"{i}: '{col}'")
    
    print(f"\nSample data:")
    print(df.iloc[:3, :8])
    
    # Add year column
    df['year'] = 2024
    
    # Load to database - keeping it wide for now in staging
    engine = get_db_engine()
    df.to_sql('staging_measure_stars', engine, if_exists='replace', index=False)
    
    print("\nMeasure stars loaded successfully!")
    return df

def load_cut_points():
    """Load Part C and Part D cut points"""
    print("Loading Cut Points...")
    
    engine = get_db_engine()
    
    # Load Part C Cut Points
    print("\n  Loading Part C Cut Points...")
    part_c_file = "data/raw/2024 Star Ratings Data Table - (Jul 2 2024)_0/2024 Star Ratings Data Table - Part C Cut Points (Jul 2 2024).csv"
    df_c = pd.read_csv(part_c_file, skiprows=1, encoding='latin-1')
    df_c['year'] = 2024
    print(f"  Part C columns: {df_c.columns.tolist()}")
    print(f"  Part C sample:\n{df_c.head(3)}")
    df_c.to_sql('staging_part_c_cutpoints', engine, if_exists='replace', index=False)
    print(f"  Loaded {len(df_c)} Part C cut points")
    
    # Load Part D Cut Points
    print("\n  Loading Part D Cut Points...")
    part_d_file = "data/raw/2024 Star Ratings Data Table - (Jul 2 2024)_0/2024 Star Ratings Data Table - Part D Cut Points (Jul 2 2024).csv"
    df_d = pd.read_csv(part_d_file, skiprows=1, encoding='latin-1')
    df_d['year'] = 2024
    print(f"  Part D columns: {df_d.columns.tolist()}")
    print(f"  Part D sample:\n{df_d.head(3)}")
    df_d.to_sql('staging_part_d_cutpoints', engine, if_exists='replace', index=False)
    print(f"Loaded {len(df_d)} Part D cut points")
    
    print("\nCut points loaded successfully!")

if __name__ == "__main__":
    load_summary_ratings()
    load_measure_stars()
    load_cut_points()