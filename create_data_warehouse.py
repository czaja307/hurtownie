import pandas as pd
import pyodbc
import numpy as np
from datetime import datetime
import os

# Database connection configuration
server = '192.168.0.118'
database = 'Olist'
driver = 'SQL Server Native Client 11.0'
username = 'sa'
password = 'password'

connection_string = (
    f'DRIVER={{{driver}}};'
    f'SERVER={server};'
    f'DATABASE={database};'
    f'UID={username};'
    f'PWD={password};'
    f'Encrypt=no;'
)

def create_connection():
    """Creates a connection to the database"""
    try:
        conn = pyodbc.connect(connection_string)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

def load_csv_data():
    """Loads data from CSV files"""
    print("Loading data from CSV files...")
    
    # File paths
    data_dir = "data"
    
    # Load Olist data with appropriate encoding
    customers = pd.read_csv(f"{data_dir}/olist/olist_customers_dataset.csv", encoding='utf-8')
    orders = pd.read_csv(f"{data_dir}/olist/olist_orders_dataset.csv", encoding='utf-8')
    order_items = pd.read_csv(f"{data_dir}/olist/olist_order_items_dataset.csv", encoding='utf-8')
    sellers = pd.read_csv(f"{data_dir}/olist/olist_sellers_dataset.csv", encoding='utf-8')
    products = pd.read_csv(f"{data_dir}/olist/olist_products_dataset.csv", encoding='utf-8')
    
    # Load Brazilian cities data - encoding may vary
    try:
        cities = pd.read_csv(f"{data_dir}/cities/BRAZIL_CITIES_REV2022.CSV", encoding='utf-8')
    except UnicodeDecodeError:
        try:
            cities = pd.read_csv(f"{data_dir}/cities/BRAZIL_CITIES_REV2022.CSV", encoding='latin-1')
        except UnicodeDecodeError:
            cities = pd.read_csv(f"{data_dir}/cities/BRAZIL_CITIES_REV2022.CSV", encoding='cp1252')
    
    print(f"Loaded {len(customers)} customers")
    print(f"Loaded {len(orders)} orders")
    print(f"Loaded {len(order_items)} order items")
    print(f"Loaded {len(sellers)} sellers")
    print(f"Loaded {len(products)} products")
    print(f"Loaded {len(cities)} cities")
    
    return customers, orders, order_items, sellers, products, cities

def create_state_region_mapping():
    """Creates a mapping of states to Brazilian regions"""
    state_region_mapping = {
        # North Region (Norte)
        'AC': 'Norte', 'AP': 'Norte', 'AM': 'Norte', 'PA': 'Norte', 
        'RO': 'Norte', 'RR': 'Norte', 'TO': 'Norte',
        
        # Northeast Region (Nordeste)
        'AL': 'Nordeste', 'BA': 'Nordeste', 'CE': 'Nordeste', 'MA': 'Nordeste',
        'PB': 'Nordeste', 'PE': 'Nordeste', 'PI': 'Nordeste', 'RN': 'Nordeste', 'SE': 'Nordeste',
        
        # Central-West Region (Centro-Oeste)
        'GO': 'Centro-Oeste', 'MT': 'Centro-Oeste', 'MS': 'Centro-Oeste', 'DF': 'Centro-Oeste',
        
        # Southeast Region (Sudeste)
        'ES': 'Sudeste', 'MG': 'Sudeste', 'RJ': 'Sudeste', 'SP': 'Sudeste',
        
        # South Region (Sul)
        'PR': 'Sul', 'RS': 'Sul', 'SC': 'Sul'
    }
    return state_region_mapping

def create_database_schema(conn):
    """Creates the database schema for the data warehouse"""
    print("Creating database schema...")
    
    cursor = conn.cursor()
    
    # Drop existing tables (in reverse order due to foreign keys)
    drop_tables = [
        "DROP TABLE IF EXISTS FactOrders",
        "DROP TABLE IF EXISTS DimTime",
        "DROP TABLE IF EXISTS DimCustomerLocation", 
        "DROP TABLE IF EXISTS DimSellerLocation",
        "DROP TABLE IF EXISTS DimProduct",
        "DROP TABLE IF EXISTS DimOrderStatus"
    ]
    
    for drop_sql in drop_tables:
        try:
            cursor.execute(drop_sql)
        except:
            pass
    
    # Time dimension - hierarchical structure
    cursor.execute("""
    CREATE TABLE DimTime (
        TimeKey INT IDENTITY(1,1) PRIMARY KEY,
        FullDate DATE UNIQUE,
        Year INT,
        Quarter INT,
        Month INT,
        MonthName NVARCHAR(20),
        Day INT,
        DayOfWeek INT,
        DayName NVARCHAR(20),
        WeekOfYear INT
    )
    """)
    
    # Customer location dimension - denormalized, hierarchical
    cursor.execute("""
    CREATE TABLE DimCustomerLocation (
        CustomerLocationKey INT IDENTITY(1,1) PRIMARY KEY,
        CustomerZipCode NVARCHAR(10),
        CustomerCity NVARCHAR(100),
        CustomerState NVARCHAR(5),
        CustomerRegion NVARCHAR(50),
        CityPopulation INT,
        StatePopulation BIGINT,
        GDP_Per_Capita DECIMAL(18,2),
        HDI DECIMAL(5,4),
        Latitude DECIMAL(10,6),
        Longitude DECIMAL(10,6)
    )
    """)
    
    # Seller location dimension - denormalized, hierarchical
    cursor.execute("""
    CREATE TABLE DimSellerLocation (
        SellerLocationKey INT IDENTITY(1,1) PRIMARY KEY,
        SellerZipCode NVARCHAR(10),
        SellerCity NVARCHAR(100),
        SellerState NVARCHAR(5),
        SellerRegion NVARCHAR(50),
        CityPopulation INT,
        StatePopulation BIGINT,
        GDP_Per_Capita DECIMAL(18,2),
        HDI DECIMAL(5,4),
        Latitude DECIMAL(10,6),
        Longitude DECIMAL(10,6)
    )
    """)
    
    # Product dimension
    cursor.execute("""
    CREATE TABLE DimProduct (
        ProductKey INT IDENTITY(1,1) PRIMARY KEY,
        ProductId NVARCHAR(50) UNIQUE,
        ProductCategory NVARCHAR(100),
        ProductWeight DECIMAL(10,2),
        ProductLength DECIMAL(10,2),
        ProductHeight DECIMAL(10,2),
        ProductWidth DECIMAL(10,2),
        ProductVolume AS (ProductLength * ProductHeight * ProductWidth),
        WeightCategory NVARCHAR(20),
        SizeCategory NVARCHAR(20)
    )
    """)
    
    # Order status dimension
    cursor.execute("""
    CREATE TABLE DimOrderStatus (
        OrderStatusKey INT IDENTITY(1,1) PRIMARY KEY,
        OrderStatus NVARCHAR(50) UNIQUE,
        StatusDescription NVARCHAR(200)
    )
    """)
    
    # Fact table
    cursor.execute("""
    CREATE TABLE FactOrders (
        FactKey INT IDENTITY(1,1) PRIMARY KEY,
        OrderId NVARCHAR(50),
        TimeKey INT,
        CustomerLocationKey INT,
        SellerLocationKey INT,
        ProductKey INT,
        OrderStatusKey INT,
        
        -- Additive measures
        TotalPrice DECIMAL(18,2),
        FreightValue DECIMAL(18,2),
        Quantity INT,
        
        -- Non-additive measures (will be aggregated separately)
        DeliveryTimeDays INT,
        ProcessingTimeDays INT,
        
        -- Dates for calculations
        OrderDate DATETIME,
        DeliveryDate DATETIME,
        EstimatedDeliveryDate DATETIME,
        
        FOREIGN KEY (TimeKey) REFERENCES DimTime(TimeKey),
        FOREIGN KEY (CustomerLocationKey) REFERENCES DimCustomerLocation(CustomerLocationKey),
        FOREIGN KEY (SellerLocationKey) REFERENCES DimSellerLocation(SellerLocationKey),
        FOREIGN KEY (ProductKey) REFERENCES DimProduct(ProductKey),
        FOREIGN KEY (OrderStatusKey) REFERENCES DimOrderStatus(OrderStatusKey)
    )
    """)
    
    conn.commit()
    print("Database schema has been created!")

def populate_dim_time(conn, start_date='2016-01-01', end_date='2019-12-31'):
    """Fills the time dimension"""
    print("Filling the time dimension...")
    
    cursor = conn.cursor()
    
    # Generate dates
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    month_names = {
        1: 'January', 2: 'February', 3: 'March', 4: 'April',
        5: 'May', 6: 'June', 7: 'July', 8: 'August',
        9: 'September', 10: 'October', 11: 'November', 12: 'December'
    }
    
    day_names = {
        0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday',
        4: 'Friday', 5: 'Saturday', 6: 'Sunday'
    }
    
    for date in date_range:
        quarter = (date.month - 1) // 3 + 1
        
        cursor.execute("""
        INSERT INTO DimTime (FullDate, Year, Quarter, Month, MonthName, Day, DayOfWeek, DayName, WeekOfYear)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            date.date(),
            date.year,
            quarter,
            date.month,
            month_names[date.month],
            date.day,
            date.weekday(),
            day_names[date.weekday()],
            date.isocalendar()[1]
        ))
    
    conn.commit()
    print(f"Added {len(date_range)} records to the time dimension")

def populate_location_dimensions(conn, customers, sellers, cities):
    """Fills the location dimensions (denormalized)"""
    print("Filling location dimensions...")
    
    cursor = conn.cursor()
    state_region_mapping = create_state_region_mapping()
    
    # Prepare city data with additional information
    cities_clean = cities.copy()
    cities_clean['CITY'] = cities_clean['CITY'].str.strip().str.lower()
    cities_clean['STATE'] = cities_clean['STATE'].str.strip().str.upper()
    
    # Aggregate data at the state level
    state_stats = cities_clean.groupby('STATE').agg({
        'IBGE_POP': 'sum',
        'GDP_CAPITA': 'mean',
        'IDHM': 'mean'
    }).reset_index()
    
    # Fill the customer location dimension
    customer_locations = customers[['customer_zip_code_prefix', 'customer_city', 'customer_state']].drop_duplicates()
    
    for _, row in customer_locations.iterrows():
        zip_code = str(row['customer_zip_code_prefix'])
        city = row['customer_city'].strip().lower() if pd.notna(row['customer_city']) else 'unknown'
        state = row['customer_state'].strip().upper() if pd.notna(row['customer_state']) else 'UNK'
        region = state_region_mapping.get(state, 'Unknown')
        
        # Find city data
        city_data = cities_clean[
            (cities_clean['CITY'] == city) & 
            (cities_clean['STATE'] == state)
        ]
        
        if not city_data.empty:
            city_pop = int(city_data.iloc[0]['IBGE_POP']) if pd.notna(city_data.iloc[0]['IBGE_POP']) else 0
            gdp_per_capita = float(city_data.iloc[0]['GDP_CAPITA']) if pd.notna(city_data.iloc[0]['GDP_CAPITA']) else 0
            hdi = float(city_data.iloc[0]['IDHM']) if pd.notna(city_data.iloc[0]['IDHM']) else 0
            lat = float(city_data.iloc[0]['LAT']) if pd.notna(city_data.iloc[0]['LAT']) else 0
            lon = float(city_data.iloc[0]['LONG']) if pd.notna(city_data.iloc[0]['LONG']) else 0
        else:
            city_pop = 0
            gdp_per_capita = 0
            hdi = 0
            lat = 0
            lon = 0
        
        # State data
        state_data = state_stats[state_stats['STATE'] == state]
        state_pop = int(state_data.iloc[0]['IBGE_POP']) if not state_data.empty and pd.notna(state_data.iloc[0]['IBGE_POP']) else 0
        
        cursor.execute("""
        INSERT INTO DimCustomerLocation 
        (CustomerZipCode, CustomerCity, CustomerState, CustomerRegion, CityPopulation, StatePopulation, GDP_Per_Capita, HDI, Latitude, Longitude)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (zip_code, city, state, region, city_pop, state_pop, gdp_per_capita, hdi, lat, lon))
    
    # Fill the seller location dimension
    seller_locations = sellers[['seller_zip_code_prefix', 'seller_city', 'seller_state']].drop_duplicates()
    
    for _, row in seller_locations.iterrows():
        zip_code = str(row['seller_zip_code_prefix'])
        city = row['seller_city'].strip().lower() if pd.notna(row['seller_city']) else 'unknown'
        state = row['seller_state'].strip().upper() if pd.notna(row['seller_state']) else 'UNK'
        region = state_region_mapping.get(state, 'Unknown')
        
        # Find city data
        city_data = cities_clean[
            (cities_clean['CITY'] == city) & 
            (cities_clean['STATE'] == state)
        ]
        
        if not city_data.empty:
            city_pop = int(city_data.iloc[0]['IBGE_POP']) if pd.notna(city_data.iloc[0]['IBGE_POP']) else 0
            gdp_per_capita = float(city_data.iloc[0]['GDP_CAPITA']) if pd.notna(city_data.iloc[0]['GDP_CAPITA']) else 0
            hdi = float(city_data.iloc[0]['IDHM']) if pd.notna(city_data.iloc[0]['IDHM']) else 0
            lat = float(city_data.iloc[0]['LAT']) if pd.notna(city_data.iloc[0]['LAT']) else 0
            lon = float(city_data.iloc[0]['LONG']) if pd.notna(city_data.iloc[0]['LONG']) else 0
        else:
            city_pop = 0
            gdp_per_capita = 0
            hdi = 0
            lat = 0
            lon = 0
        
        # State data
        state_data = state_stats[state_stats['STATE'] == state]
        state_pop = int(state_data.iloc[0]['IBGE_POP']) if not state_data.empty and pd.notna(state_data.iloc[0]['IBGE_POP']) else 0
        
        cursor.execute("""
        INSERT INTO DimSellerLocation 
        (SellerZipCode, SellerCity, SellerState, SellerRegion, CityPopulation, StatePopulation, GDP_Per_Capita, HDI, Latitude, Longitude)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (zip_code, city, state, region, city_pop, state_pop, gdp_per_capita, hdi, lat, lon))
    
    conn.commit()
    print(f"Added {len(customer_locations)} customer locations")
    print(f"Added {len(seller_locations)} seller locations")

def populate_dim_product(conn, products):
    """Fills the product dimension"""
    print("Filling the product dimension...")
    
    cursor = conn.cursor()
    
    for _, row in products.iterrows():
        product_id = row['product_id']
        category = row['product_category_name'] if pd.notna(row['product_category_name']) else 'unknown'
        weight = float(row['product_weight_g']) if pd.notna(row['product_weight_g']) else 0
        length = float(row['product_length_cm']) if pd.notna(row['product_length_cm']) else 0
        height = float(row['product_height_cm']) if pd.notna(row['product_height_cm']) else 0
        width = float(row['product_width_cm']) if pd.notna(row['product_width_cm']) else 0
        
        # Weight categorization
        if weight == 0:
            weight_category = 'Unknown'
        elif weight <= 100:
            weight_category = 'Light'
        elif weight <= 1000:
            weight_category = 'Medium'
        else:
            weight_category = 'Heavy'
        
        # Size categorization (based on volume)
        volume = length * height * width
        if volume == 0:
            size_category = 'Unknown'
        elif volume <= 1000:
            size_category = 'Small'
        elif volume <= 10000:
            size_category = 'Medium'
        else:
            size_category = 'Large'
        
        cursor.execute("""
        INSERT INTO DimProduct 
        (ProductId, ProductCategory, ProductWeight, ProductLength, ProductHeight, ProductWidth, WeightCategory, SizeCategory)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (product_id, category, weight, length, height, width, weight_category, size_category))
    
    conn.commit()
    print(f"Added {len(products)} products")

def populate_dim_order_status(conn, orders):
    """Fills the order status dimension"""
    print("Filling the order status dimension...")
    
    cursor = conn.cursor()
    
    status_descriptions = {
        'delivered': 'Order delivered',
        'shipped': 'Order shipped',
        'processing': 'Order processing',
        'canceled': 'Order canceled',
        'unavailable': 'Product unavailable',
        'created': 'Order created',
        'approved': 'Order approved',
        'invoiced': 'Order invoiced'
    }
    
    unique_statuses = orders['order_status'].unique()
    
    for status in unique_statuses:
        if pd.notna(status):
            description = status_descriptions.get(status, f'Status: {status}')
            cursor.execute("""
            INSERT INTO DimOrderStatus (OrderStatus, StatusDescription)
            VALUES (?, ?)
            """, (status, description))
    
    conn.commit()
    print(f"Added {len(unique_statuses)} order statuses")

def populate_fact_table(conn, orders, order_items, customers, sellers, products):
    """Fills the fact table"""
    print("Filling the fact table...")
    
    cursor = conn.cursor()
    
    # Join all necessary data
    fact_data = order_items.merge(orders, on='order_id', how='inner')
    fact_data = fact_data.merge(customers, on='customer_id', how='inner')
    fact_data = fact_data.merge(sellers, on='seller_id', how='inner')
    fact_data = fact_data.merge(products, on='product_id', how='inner')
    
    print(f"Prepared {len(fact_data)} records for loading")
    
    # Get dimension keys
    time_keys = {}
    cursor.execute("SELECT TimeKey, FullDate FROM DimTime")
    for row in cursor.fetchall():
        time_keys[row[1]] = row[0]
    
    customer_location_keys = {}
    cursor.execute("SELECT CustomerLocationKey, CustomerZipCode, CustomerCity, CustomerState FROM DimCustomerLocation")
    for row in cursor.fetchall():
        key = f"{row[1]}_{row[2]}_{row[3]}"
        customer_location_keys[key] = row[0]
    
    seller_location_keys = {}
    cursor.execute("SELECT SellerLocationKey, SellerZipCode, SellerCity, SellerState FROM DimSellerLocation")
    for row in cursor.fetchall():
        key = f"{row[1]}_{row[2]}_{row[3]}"
        seller_location_keys[key] = row[0]
    
    product_keys = {}
    cursor.execute("SELECT ProductKey, ProductId FROM DimProduct")
    for row in cursor.fetchall():
        product_keys[row[1]] = row[0]
    
    status_keys = {}
    cursor.execute("SELECT OrderStatusKey, OrderStatus FROM DimOrderStatus")
    for row in cursor.fetchall():
        status_keys[row[1]] = row[0]
    
    # Insert data into the fact table
    inserted_count = 0
    batch_size = 1000
    batch_data = []
    
    for _, row in fact_data.iterrows():
        try:
            # Parse dates
            order_date = pd.to_datetime(row['order_purchase_timestamp'])
            delivery_date = pd.to_datetime(row['order_delivered_customer_date']) if pd.notna(row['order_delivered_customer_date']) else None
            estimated_delivery_date = pd.to_datetime(row['order_estimated_delivery_date']) if pd.notna(row['order_estimated_delivery_date']) else None
            
            # Find keys
            time_key = time_keys.get(order_date.date())
            if not time_key:
                continue
                
            customer_key_str = f"{row['customer_zip_code_prefix']}_{row['customer_city'].strip().lower()}_{row['customer_state'].strip().upper()}"
            customer_location_key = customer_location_keys.get(customer_key_str)
            if not customer_location_key:
                continue
                
            seller_key_str = f"{row['seller_zip_code_prefix']}_{row['seller_city'].strip().lower()}_{row['seller_state'].strip().upper()}"
            seller_location_key = seller_location_keys.get(seller_key_str)
            if not seller_location_key:
                continue
                
            product_key = product_keys.get(row['product_id'])
            if not product_key:
                continue
                
            status_key = status_keys.get(row['order_status'])
            if not status_key:
                continue
            
            # Calculate measures
            total_price = float(row['price']) if pd.notna(row['price']) else 0
            freight_value = float(row['freight_value']) if pd.notna(row['freight_value']) else 0
            quantity = 1  # each row is one item
            
            # Calculate delivery time (non-additive measure)
            delivery_time_days = None
            processing_time_days = None
            
            if delivery_date and order_date:
                delivery_time_days = (delivery_date - order_date).days
            
            if pd.notna(row['order_approved_at']):
                approved_date = pd.to_datetime(row['order_approved_at'])
                processing_time_days = (approved_date - order_date).days
            
            batch_data.append((
                row['order_id'], time_key, customer_location_key, seller_location_key,
                product_key, status_key, total_price, freight_value, quantity,
                delivery_time_days, processing_time_days, order_date,
                delivery_date, estimated_delivery_date
            ))
            
            if len(batch_data) >= batch_size:
                cursor.executemany("""
                INSERT INTO FactOrders 
                (OrderId, TimeKey, CustomerLocationKey, SellerLocationKey, ProductKey, OrderStatusKey,
                 TotalPrice, FreightValue, Quantity, DeliveryTimeDays, ProcessingTimeDays,
                 OrderDate, DeliveryDate, EstimatedDeliveryDate)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, batch_data)
                
                inserted_count += len(batch_data)
                batch_data = []
                
                if inserted_count % 5000 == 0:
                    print(f"Inserted {inserted_count} records...")
                    conn.commit()
        
        except Exception as e:
            print(f"Error processing record: {e}")
            continue
    
    # Insert remaining records
    if batch_data:
        cursor.executemany("""
        INSERT INTO FactOrders 
        (OrderId, TimeKey, CustomerLocationKey, SellerLocationKey, ProductKey, OrderStatusKey,
         TotalPrice, FreightValue, Quantity, DeliveryTimeDays, ProcessingTimeDays,
         OrderDate, DeliveryDate, EstimatedDeliveryDate)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, batch_data)
        inserted_count += len(batch_data)
    
    conn.commit()
    print(f"Added {inserted_count} records to the fact table")

def create_sample_queries(conn):
    """Creates sample analytical queries"""
    print("\nCreating sample analytical queries...")
    
    cursor = conn.cursor()
    
    queries = [
        {
            'name': 'Sales by region and time (hierarchies)',
            'sql': """
            SELECT 
                cl.CustomerRegion,
                cl.CustomerState,
                dt.Year,
                dt.Quarter,
                SUM(f.TotalPrice) as TotalSales,
                SUM(f.FreightValue) as TotalFreight,
                COUNT(*) as OrderCount,
                AVG(CAST(f.DeliveryTimeDays AS FLOAT)) as AvgDeliveryTime
            FROM FactOrders f
            JOIN DimCustomerLocation cl ON f.CustomerLocationKey = cl.CustomerLocationKey
            JOIN DimTime dt ON f.TimeKey = dt.TimeKey
            WHERE f.DeliveryTimeDays IS NOT NULL
            GROUP BY cl.CustomerRegion, cl.CustomerState, dt.Year, dt.Quarter
            ORDER BY TotalSales DESC
            """
        },
        {
            'name': 'Product analysis by category and size',
            'sql': """
            SELECT 
                p.ProductCategory,
                p.WeightCategory,
                p.SizeCategory,
                COUNT(*) as ProductsSold,
                SUM(f.TotalPrice) as Revenue,
                AVG(f.TotalPrice) as AvgPrice,
                SUM(f.Quantity) as TotalQuantity
            FROM FactOrders f
            JOIN DimProduct p ON f.ProductKey = p.ProductKey
            GROUP BY p.ProductCategory, p.WeightCategory, p.SizeCategory
            HAVING COUNT(*) > 10
            ORDER BY Revenue DESC
            """
        },
        {
            'name': 'Delivery performance by seller location',
            'sql': """
            SELECT 
                sl.SellerRegion,
                sl.SellerState,
                COUNT(*) as OrderCount,
                AVG(CAST(f.DeliveryTimeDays AS FLOAT)) as AvgDeliveryTime,
                MIN(f.DeliveryTimeDays) as MinDeliveryTime,
                MAX(f.DeliveryTimeDays) as MaxDeliveryTime,
                SUM(f.TotalPrice) as TotalRevenue
            FROM FactOrders f
            JOIN DimSellerLocation sl ON f.SellerLocationKey = sl.SellerLocationKey
            WHERE f.DeliveryTimeDays IS NOT NULL AND f.DeliveryTimeDays >= 0
            GROUP BY sl.SellerRegion, sl.SellerState
            HAVING COUNT(*) > 50
            ORDER BY AvgDeliveryTime ASC
            """
        }
    ]
    
    for query in queries:
        print(f"\n--- {query['name']} ---")
        try:
            cursor.execute(query['sql'])
            results = cursor.fetchall()
            print(f"Returned {len(results)} results")
            
            # Display the first 5 results
            for i, row in enumerate(results[:5]):
                print(f"  {i+1}: {row}")
            if len(results) > 5:
                print(f"  ... and {len(results)-5} more")
                
        except Exception as e:
            print(f"Query execution error: {e}")

def main():
    """Main program function"""
    print("=== DATA WAREHOUSE CREATION FOR OLIST ===")
    
    # Database connection
    conn = create_connection()
    if not conn:
        return
    
    try:
        # Load CSV data
        customers, orders, order_items, sellers, products, cities = load_csv_data()
        
        # Create database schema
        create_database_schema(conn)
        
        # Fill dimensions
        populate_dim_time(conn)
        populate_location_dimensions(conn, customers, sellers, cities)
        populate_dim_product(conn, products)
        populate_dim_order_status(conn, orders)
        
        # Fill fact table
        populate_fact_table(conn, orders, order_items, customers, sellers, products)
        
        # Check record count
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM FactOrders")
        fact_count = cursor.fetchone()[0]
        print(f"\nFact table contains {fact_count} records")
        
        if fact_count >= 10000:
            print("✓ Minimum requirement of 10,000 records met!")
        else:
            print("⚠ Warning: Fact table contains less than 10,000 records")
        
        # Sample analytical queries
        create_sample_queries(conn)
        
        print("\n=== DATA WAREHOUSE SUCCESSFULLY CREATED ===")
        print("\nData warehouse structure:")
        print("• 5 dimensions (including 3 hierarchical: time, customer location, seller location)")
        print("• 3 measures (2 additive: TotalPrice, FreightValue; 1 non-additive: average delivery time)")
        print("• Denormalized location dimensions with demographic data")
        print("• Combined Olist data with Brazilian city data")
        
    except Exception as e:
        print(f"Error creating data warehouse: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
