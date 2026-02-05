import pandas as pd
from sqlalchemy import create_engine, text
import os

DB_CONFIG = {
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': '5432',
    'database': 'medicare_star_ratings'
}

def get_db_engine():
    connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    return create_engine(connection_string)

def export_data():
    print("Exporting data for Power BI...")
    
    engine = get_db_engine()
    output_dir = "powerbi_data"
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Export 1: All 3.5-star plans
    print("\n1. Exporting 3.5-star contracts...")
    query1 = """
        SELECT contract_id, contract_name, organization_type, 
               overall_star_rating, part_c_summary_star, part_d_summary_star 
        FROM contracts 
        WHERE overall_star_rating = 3.5
    """
    df1 = pd.read_sql(query1, engine)
    df1.to_csv(f"{output_dir}/contracts_35_stars.csv", index=False)
    print(f"   Exported {len(df1)} contracts")
    
    # Export 2: Measure scores for 3.5-star plans
    print("\n2. Exporting measure scores for 3.5-star plans...")
    query2 = """
        SELECT ms.contract_id, c.contract_name, ms.measure_id, mm.measure_name, 
               ms.score, mm.weight, mm.domain
        FROM measure_scores ms
        JOIN contracts c ON TRIM(ms.contract_id) = TRIM(c.contract_id)
        JOIN measure_metadata mm ON ms.measure_id = mm.measure_id
        WHERE c.overall_star_rating = 3.5 
          AND ms.score IS NOT NULL
    """
    df2 = pd.read_sql(query2, engine)
    df2.to_csv(f"{output_dir}/measure_scores_35_stars.csv", index=False)
    print(f"   Exported {len(df2)} measure scores")
    
    # Export 3: Measure prioritization matrix
    print("\n3. Exporting measure priorities...")
    query3 = """
        SELECT 
            ms.measure_id, 
            mm.measure_name, 
            mm.weight, 
            mm.domain,
            AVG(ms.score) as avg_score,
            COUNT(*) as num_plans_struggling,
            ROUND(mm.weight * (4.0 - AVG(ms.score)), 2) as improvement_potential
        FROM measure_scores ms
        JOIN measure_metadata mm ON ms.measure_id = mm.measure_id
        JOIN contracts c ON TRIM(ms.contract_id) = TRIM(c.contract_id)
        WHERE c.overall_star_rating = 3.5 
          AND ms.score IS NOT NULL 
          AND ms.score < 4.0
        GROUP BY ms.measure_id, mm.measure_name, mm.weight, mm.domain
        ORDER BY improvement_potential DESC
    """
    df3 = pd.read_sql(query3, engine)
    df3.to_csv(f"{output_dir}/measure_priorities.csv", index=False)
    print(f"   Exported {len(df3)} measure priorities")
    
    # Export 4: All contracts summary
    print("\n4. Exporting all contracts summary...")
    query4 = """
        SELECT 
            contract_id, 
            contract_name, 
            organization_type, 
            overall_star_rating,
            CASE 
                WHEN overall_star_rating >= 4.0 THEN 'Bonus Eligible'
                WHEN overall_star_rating = 3.5 THEN 'Near Bonus'
                ELSE 'Below Threshold'
            END as bonus_status
        FROM contracts 
        WHERE overall_star_rating IS NOT NULL
    """
    df4 = pd.read_sql(query4, engine)
    df4.to_csv(f"{output_dir}/all_contracts.csv", index=False)
    print(f"   Exported {len(df4)} contracts")
    
    print(f"\nAll data exported to {output_dir}/ folder!")
    print("\nFiles created:")
    print("  - contracts_35_stars.csv")
    print("  - measure_scores_35_stars.csv")
    print("  - measure_priorities.csv")
    print("  - all_contracts.csv")

if __name__ == "__main__":
    export_data()