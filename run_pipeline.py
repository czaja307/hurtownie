"""
INSTRUCTION FOR RUNNING THE OLIST DATA WAREHOUSE
================================================

STEP 1: Environment Setup
-------------------------
1. Make sure you have the required Python libraries installed:
   pip install pandas numpy pyodbc

2. Check the SQL Server connection (192.168.1.204)
   - User: sa
   - Password: password
   - The database will be created automatically: Olist

STEP 2: ETL Process Execution
-----------------------------
python create_data_warehouse.py

This script will perform the following operations:
- Load data from CSV files (data/olist/ and data/cities/)
- Create the data warehouse schema (dimension and fact tables)
- Populate the dimensions with transformation and enrichment
- Create the fact table with >10,000 records
- Generate views with calculated measures
- Save sample OLAP queries

STEP 3: Additional Analyses
---------------------------
python olap_cube_definition.py

Generates advanced analytical queries.

DATA WAREHOUSE STRUCTURE:
=========================
DIMENSIONS (5):
- DIM_Time (hierarchical: Year → Quarter → Month → Day)
- DIM_Customer (hierarchical: Region → State → City → Customer + economic data)
- DIM_Seller (hierarchical: Region → State → City → Seller + economic data)
- DIM_Payment (payment types, installments, categories)
- DIM_Review (scores, satisfaction)

FACTS:
- FACT_Orders (orders with measures)

MEASURES:
- Additive: Order_Value, Freight_Value, Items_Count
- Non-additive: Delivery_Days (average), Review_Score (average)
- Calculated: Total_Revenue, Revenue_Per_Customer, Freight_Percentage

OUTPUT FILES:
============ =
- sample_olap_queries.sql - basic OLAP queries
- advanced_olap_analysis.sql - advanced multidimensional analyses

EXAMPLE ANALYSES:
=================
1. Sales over time (roll-up/drill-down)
2. Geographic analysis with city GDP and HDI
3. Customer segmentation (RFM)
4. Economic correlations
5. Seasonality analyses
6. Delivery efficiency vs satisfaction
7. Year-over-year trends
8. Cross-dimensional analysis
"""

import subprocess
import sys
import os

def install_requirements():
    """Instalacja wymaganych bibliotek"""
    print("Installing required Python packages...")
    
    packages = ['pandas', 'numpy', 'pyodbc']
    
    for package in packages:
        try:
            __import__(package)
            print(f"  {package} is already installed")
        except ImportError:
            print(f"  Installing {package}...")
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', package])

def check_data_files():
    """Sprawdzenie dostępności plików danych"""
    print("Checking data files...")
    
    data_path = r'c:\Users\Kuba\PycharmProjects\hurtownie\data'
    
    required_files = [
        'olist/olist_orders_dataset.csv',
        'olist/olist_order_items_dataset.csv', 
        'olist/olist_customers_dataset.csv',
        'olist/olist_sellers_dataset.csv',
        'olist/olist_order_payments_dataset.csv',
        'olist/olist_order_reviews_dataset.csv',
        'cities/BRAZIL_CITIES_REV2022.CSV'
    ]
    
    missing_files = []
    
    for file_path in required_files:
        full_path = os.path.join(data_path, file_path)
        if os.path.exists(full_path):
            print(f"  Found: {file_path}")
        else:
            print(f"  Missing: {file_path}")
            missing_files.append(file_path)
    
    if missing_files:
        print(f"\n❌ Brakuje {len(missing_files)} plików danych!")
        return False
    
    print("  All data files are available")
    return True

def run_etl_pipeline():
    """Uruchomienie pełnego pipeline ETL"""
    print("Starting Olist data warehouse ETL pipeline")
    print("=" * 60)
    
    # Krok 1: Sprawdzenie wymagań
    if not check_data_files():
        print("Stopped - missing data files")
        return False
    
    # Krok 2: Instalacja bibliotek
    try:
        install_requirements()
    except Exception as e:
        print(f"❌ Błąd instalacji bibliotek: {e}")
        return False
    
    # Krok 3: Import i uruchomienie ETL
    try:
        print("\nRunning main ETL process...")
        from create_data_warehouse import OlistDataWarehouse
        
        dw = OlistDataWarehouse()
        success = dw.run_etl_process()
        
        if success:
            print("\nGenerating additional analyses...")
            from olap_cube_definition import save_advanced_queries
            save_advanced_queries()
            
            print("\n" + "=" * 60)
            print("Pipeline completed successfully!")
            print("\nNext steps:")
            print("   1. Connect to the OlistDW database on SQL Server")
            print("   2. Run the queries in sample_olap_queries.sql")  
            print("   3. Explore advanced analyses in advanced_olap_analysis.sql")
            print("   4. Create reports in Power BI/Tableau")
            
            return True
        else:
            print("ETL finished with errors")
            return False
            
    except Exception as e:
        print(f"Error during ETL: {e}")
        return False

if __name__ == "__main__":
    run_etl_pipeline()
