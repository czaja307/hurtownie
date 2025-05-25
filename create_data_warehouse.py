"""
Script for creating a data warehouse for the Olist e-commerce system
OLAP cube with 5 dimensions and 3 measures + calculated measures

FACT: Order
DIMENSIONS:
1. DIM_TIME (hierarchical: Year -> Quarter -> Month -> Day)
2. DIM_CUSTOMER (hierarchical: State -> City -> Customer + economic data)
3. DIM_SELLER (hierarchical: State -> City -> Seller + economic data)
4. DIM_PAYMENT (Payment type, installments, categories)
5. DIM_REVIEW (Rating, satisfaction categories)

MEASURES:
- Order_Value (additive)
- Freight_Value (additive)
- Delivery_Days (non-additive - average)
+ CALCULATED MEASURES:
- Avg_Review_Score (non-additive)
- Revenue_Per_Customer (calculated)
- Profit_Margin (calculated)
"""

import pyodbc
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from fuzzywuzzy import fuzz, process
from unidecode import unidecode

class OlistDataWarehouse:
    def __init__(self):
        # Database connection configuration
        self.server = '192.168.1.204'
        self.database = 'Olist'
        self.driver = 'SQL Server Native Client 11.0'
        self.username = 'sa'
        self.password = 'password'
        
        self.connection_string = (
            f'DRIVER={{{self.driver}}};'
            f'SERVER={self.server};'
            f'DATABASE={self.database};'
            f'UID={self.username};'
            f'PWD={self.password};'
            f'Encrypt=no;'
        )
        
        # Paths to data files
        self.data_path = r'c:\\Users\\Kuba\\PycharmProjects\\hurtownie\\data'
    def connect_db(self):
        """Establish connection with the database"""
        try:
            conn = pyodbc.connect(self.connection_string)
            print("Connection with the database established")
            return conn
        except Exception as e:
            print(f"Error connecting to the database: {e}")
            return None
    
    def normalize_city_name(self, city_name):
        """Normalize city name for better matching"""
        if pd.isna(city_name) or not city_name:
            return ""
        
        # Convert to string and normalize
        normalized = str(city_name).lower().strip()
        # Remove accents
        normalized = unidecode(normalized)
        # Remove common words and clean up
        replacements = {
            "\'": "",
            "\"": "",
            "-": " ",
            "_": " ",
        }
        
        for old, new in replacements.items():
            normalized = normalized.replace(old, new)
        
        # Remove multiple spaces
        normalized = " ".join(normalized.split())
        return normalized
    
    def fuzzy_match_city(self, customer_city, customer_state, cities_by_state, threshold=80):
        """Find the best matching city using fuzzy matching"""
        if customer_state not in cities_by_state:
            return {}
        
        normalized_customer_city = self.normalize_city_name(customer_city)
        if not normalized_customer_city:
            return {}
        
        # Get cities for this state
        state_cities = cities_by_state[customer_state]
        
        # First try exact match
        for city_data in state_cities:
            if normalized_customer_city == city_data['normalized_name']:
                return city_data
        
        # If no exact match, try fuzzy matching
        city_names = [(city_data['normalized_name'], city_data) for city_data in state_cities]
        
        if city_names:
            # Use process.extractOne to find the best match
            result = process.extractOne(
                normalized_customer_city, 
                [name for name, _ in city_names],
                scorer=fuzz.token_sort_ratio
            )
            
            if result and result[1] >= threshold:
                matched_name = result[0]
                # Find the corresponding city data
                for name, city_data in city_names:
                    if name == matched_name:
                        return city_data
        
        return {}
    
    def create_database_schema(self):
        """Create the data warehouse database schema"""
        conn = self.connect_db()
        if not conn:
            return False
            
        cursor = conn.cursor()
        
        try:
            print("Creating data warehouse schema...")
            
            # Drop existing tables (in the correct order)
            drop_tables = [
                "DROP TABLE IF EXISTS FACT_Orders;",
                "DROP TABLE IF EXISTS DIM_Time;",
                "DROP TABLE IF EXISTS DIM_Customer;", 
                "DROP TABLE IF EXISTS DIM_Seller;",
                "DROP TABLE IF EXISTS DIM_Payment;",
                "DROP TABLE IF EXISTS DIM_Review;"
            ]
            
            for drop_sql in drop_tables:
                cursor.execute(drop_sql)
            
            # 1. TIME DIMENSION (hierarchical: Year -> Quarter -> Month -> Day)
            dim_time_sql = """
            CREATE TABLE DIM_Time (
                Time_Key INT IDENTITY(1,1) PRIMARY KEY,
                Date_Value DATE NOT NULL,
                Day_Name NVARCHAR(20),
                Day_Number INT,
                Week_Number INT,
                Month_Number INT,
                Month_Name NVARCHAR(20),
                Quarter_Number INT,
                Quarter_Name NVARCHAR(10),
                Year_Number INT,
                Is_Weekend BIT,
                Is_Holiday BIT,
                Date_String NVARCHAR(10)
            );
            """
            
            # 2. CUSTOMER DIMENSION (hierarchical: State -> City -> Customer + economic data)
            dim_customer_sql = """
            CREATE TABLE DIM_Customer (
                Customer_Key INT IDENTITY(1,1) PRIMARY KEY,
                Customer_ID NVARCHAR(50) NOT NULL,
                Customer_Unique_ID NVARCHAR(50),
                Customer_Zip_Code NVARCHAR(10),
                Customer_City NVARCHAR(100),
                Customer_State NVARCHAR(5),
                Customer_Region NVARCHAR(50),
                -- Economic data from cities dataset
                City_Population INT,
                City_GDP_Per_Capita DECIMAL(15,2),
                City_HDI DECIMAL(5,4),
                City_HDI_Income DECIMAL(5,4),
                City_HDI_Education DECIMAL(5,4),
                City_HDI_Longevity DECIMAL(5,4),
                City_Is_Capital BIT,
                City_Category NVARCHAR(50)
            );
            """
            
            # 3. SELLER DIMENSION (hierarchical: State -> City -> Seller + economic data)
            dim_seller_sql = """
            CREATE TABLE DIM_Seller (
                Seller_Key INT IDENTITY(1,1) PRIMARY KEY,
                Seller_ID NVARCHAR(50) NOT NULL,
                Seller_Zip_Code NVARCHAR(10),
                Seller_City NVARCHAR(100),
                Seller_State NVARCHAR(5),
                Seller_Region NVARCHAR(50),
                -- Economic data from cities dataset
                City_Population INT,
                City_GDP_Per_Capita DECIMAL(15,2),
                City_HDI DECIMAL(5,4),
                City_HDI_Income DECIMAL(5,4),
                City_HDI_Education DECIMAL(5,4),
                City_HDI_Longevity DECIMAL(5,4),
                City_Is_Capital BIT,
                City_Category NVARCHAR(50)
            );
            """
            
            # 4. PAYMENT DIMENSION
            dim_payment_sql = """
            CREATE TABLE DIM_Payment (
                Payment_Key INT IDENTITY(1,1) PRIMARY KEY,
                Payment_Type NVARCHAR(50),
                Payment_Category NVARCHAR(50),
                Installments_Range NVARCHAR(20),
                Is_Credit BIT,
                Is_Installment BIT
            );
            """
            
            # 5. REVIEW DIMENSION
            dim_review_sql = """
            CREATE TABLE DIM_Review (
                Review_Key INT IDENTITY(1,1) PRIMARY KEY,
                Review_Score INT,
                Review_Category NVARCHAR(30),
                Satisfaction_Level NVARCHAR(20),
                Has_Comment BIT,
                Comment_Length_Category NVARCHAR(30)
            );
            """
            
            # FACT TABLE
            fact_orders_sql = """
            CREATE TABLE FACT_Orders (
                Order_Key INT IDENTITY(1,1) PRIMARY KEY,
                Order_ID NVARCHAR(50) NOT NULL,
                Time_Key INT,
                Customer_Key INT,
                Seller_Key INT,
                Payment_Key INT,
                Review_Key INT,
                
                -- ADDITIVE MEASURES
                Order_Value DECIMAL(15,2),
                Freight_Value DECIMAL(15,2),
                Items_Count INT,
                
                -- NON-ADDITIVE MEASURES
                Delivery_Days INT,
                Review_Score INT,
                
                -- DATA FOR CALCULATED MEASURES
                Purchase_Date DATE,
                Delivery_Date DATE,
                Estimated_Delivery_Date DATE,
                
                FOREIGN KEY (Time_Key) REFERENCES DIM_Time(Time_Key),
                FOREIGN KEY (Customer_Key) REFERENCES DIM_Customer(Customer_Key),
                FOREIGN KEY (Seller_Key) REFERENCES DIM_Seller(Seller_Key),
                FOREIGN KEY (Payment_Key) REFERENCES DIM_Payment(Payment_Key),
                FOREIGN KEY (Review_Key) REFERENCES DIM_Review(Review_Key)
            );
            """
            
            # Execute table creation queries
            tables = [
                ("DIM_Time", dim_time_sql),
                ("DIM_Customer", dim_customer_sql),
                ("DIM_Seller", dim_seller_sql),
                ("DIM_Payment", dim_payment_sql),
                ("DIM_Review", dim_review_sql),
                ("FACT_Orders", fact_orders_sql)
            ]
            
            for table_name, sql in tables:
                print(f"  Creating table {table_name}...")
                cursor.execute(sql)
            
            conn.commit()
            print("Data warehouse schema created successfully")
            return True
            
        except Exception as e:
            print(f"Error creating schema: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    def load_data_files(self):
        """Load data from CSV files"""
        print("Loading data from files...")
        
        try:
            # Load Olist data
            self.orders_df = pd.read_csv(os.path.join(self.data_path, 'olist', 'olist_orders_dataset.csv'))
            self.order_items_df = pd.read_csv(os.path.join(self.data_path, 'olist', 'olist_order_items_dataset.csv'))
            self.customers_df = pd.read_csv(os.path.join(self.data_path, 'olist', 'olist_customers_dataset.csv'))
            self.sellers_df = pd.read_csv(os.path.join(self.data_path, 'olist', 'olist_sellers_dataset.csv'))
            self.payments_df = pd.read_csv(os.path.join(self.data_path, 'olist', 'olist_order_payments_dataset.csv'))
            self.reviews_df = pd.read_csv(os.path.join(self.data_path, 'olist', 'olist_order_reviews_dataset.csv'))
            
            # Load Brazilian cities data
            self.cities_df = pd.read_csv(os.path.join(self.data_path, 'cities', 'BRAZIL_CITIES_REV2022.CSV'))
            
            print(f"  Orders: {len(self.orders_df)} records")
            print(f"  Order items: {len(self.order_items_df)} records")
            print(f"  Customers: {len(self.customers_df)} records")
            print(f"  Sellers: {len(self.sellers_df)} records")
            print(f"  Payments: {len(self.payments_df)} records")
            print(f"  Reviews: {len(self.reviews_df)} records")
            print(f"  Cities: {len(self.cities_df)} records")
            
            return True
            
        except Exception as e:
            print(f"Error loading data: {e}")
            return False
    
    def create_time_dimension(self):
        """Create time dimension"""
        print("Creating time dimension...")
        
        conn = self.connect_db()
        if not conn:
            return False
            
        cursor = conn.cursor()
        
        try:
            # Generate dates from the beginning of 2016 to the end of 2019
            start_date = datetime(2016, 1, 1)
            end_date = datetime(2019, 12, 31)
            
            current_date = start_date
            
            while current_date <= end_date:
                day_name = current_date.strftime('%A')
                day_number = current_date.day
                week_number = current_date.isocalendar()[1]
                month_number = current_date.month
                month_name = current_date.strftime('%B')
                quarter_number = (current_date.month - 1) // 3 + 1
                quarter_name = f"Q{quarter_number}"
                year_number = current_date.year
                is_weekend = 1 if current_date.weekday() >= 5 else 0
                is_holiday = 0  # Simplification - can be extended with Brazilian holidays
                date_string = current_date.strftime('%Y-%m-%d')
                
                insert_sql = """
                INSERT INTO DIM_Time 
                (Date_Value, Day_Name, Day_Number, Week_Number, Month_Number, Month_Name,
                 Quarter_Number, Quarter_Name, Year_Number, Is_Weekend, Is_Holiday, Date_String)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                cursor.execute(insert_sql, (
                    current_date.date(), day_name, day_number, week_number, month_number, month_name,
                    quarter_number, quarter_name, year_number, is_weekend, is_holiday, date_string
                ))
                
                current_date += timedelta(days=1)
            
            conn.commit()
            print("  Time dimension created successfully")
            return True
            
        except Exception as e:
            print(f"  Error creating time dimension: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
            
    def create_customer_dimension(self):
        """Create customer dimension with economic data using fuzzy matching"""
        print("Creating customer dimension with fuzzy city matching...")
        
        conn = self.connect_db()
        if not conn:
            return False
            
        cursor = conn.cursor()
        
        try:
            # Brazil regions map
            region_map = {
                'AC': 'North', 'AL': 'Northeast', 'AP': 'North', 'AM': 'North', 'BA': 'Northeast',
                'CE': 'Northeast', 'DF': 'Central-West', 'ES': 'Southeast', 'GO': 'Central-West',
                'MA': 'Northeast', 'MT': 'Central-West', 'MS': 'Central-West', 'MG': 'Southeast',
                'PA': 'North', 'PB': 'Northeast', 'PR': 'South', 'PE': 'Northeast', 'PI': 'Northeast',
                'RJ': 'Southeast', 'RN': 'Northeast', 'RS': 'South', 'RO': 'North', 'RR': 'North',
                'SC': 'South', 'SP': 'Southeast', 'SE': 'Northeast', 'TO': 'North'
            }
            
            # Prepare cities data with normalized names for fuzzy matching
            print("  Preparing cities data for fuzzy matching...")
            cities_by_state = {}
            
            for _, row in self.cities_df.iterrows():
                state = str(row['STATE']).upper().strip()
                if state not in cities_by_state:
                    cities_by_state[state] = []
                
                city_data = {
                    'original_name': row['CITY'],
                    'normalized_name': self.normalize_city_name(row['CITY']),
                    'state': state,
                    'population': row.get('IBGE_POP', 0) if pd.notna(row.get('IBGE_POP', 0)) else 0,
                    'gdp_capita': row.get('GDP_CAPITA', 0) if pd.notna(row.get('GDP_CAPITA', 0)) else 0,
                    'hdi': row.get('IDHM', 0) if pd.notna(row.get('IDHM', 0)) else 0,
                    'hdi_income': row.get('IDHM_Renda', 0) if pd.notna(row.get('IDHM_Renda', 0)) else 0,
                    'hdi_education': row.get('IDHM_Educacao', 0) if pd.notna(row.get('IDHM_Educacao', 0)) else 0,
                    'hdi_longevity': row.get('IDHM_Longevidade', 0) if pd.notna(row.get('IDHM_Longevidade', 0)) else 0,
                    'is_capital': 1 if row.get('CAPITAL', 0) == 1 else 0,
                    'category': str(row.get('CATEGORIA_TUR', 'None')) if pd.notna(row.get('CATEGORIA_TUR')) else 'None'
                }
                cities_by_state[state].append(city_data)
            
            print(f"  Loaded {len(self.cities_df)} cities from {len(cities_by_state)} states")
            
            # Track matching statistics
            exact_matches = 0
            fuzzy_matches = 0
            no_matches = 0
            
            # Process customers
            total_customers = len(self.customers_df)
            processed = 0
            
            for _, customer in self.customers_df.iterrows():
                customer_city = str(customer['customer_city']).strip()
                customer_state = str(customer['customer_state']).upper().strip()
                
                # Find best matching city using fuzzy logic
                city_data = self.fuzzy_match_city(customer_city, customer_state, cities_by_state)
                
                if city_data:
                    # Check if it was an exact match
                    normalized_customer = self.normalize_city_name(customer_city)
                    if normalized_customer == city_data['normalized_name']:
                        exact_matches += 1
                    else:
                        fuzzy_matches += 1
                        # Log fuzzy matches for debugging
                        if processed < 10:  # Log first 10 fuzzy matches
                            print(f"    Fuzzy match: '{customer_city}' -> '{city_data['original_name']}'")
                else:
                    no_matches += 1
                    if processed < 5:  # Log first 5 non-matches
                        print(f"    No match found for: '{customer_city}', {customer_state}")
                
                # Assign region
                region = region_map.get(customer_state, 'Unknown')
                
                insert_sql = """
                INSERT INTO DIM_Customer 
                (Customer_ID, Customer_Unique_ID, Customer_Zip_Code, Customer_City, Customer_State, Customer_Region,
                 City_Population, City_GDP_Per_Capita, City_HDI, City_HDI_Income, City_HDI_Education, 
                 City_HDI_Longevity, City_Is_Capital, City_Category)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                cursor.execute(insert_sql, (
                    customer['customer_id'],
                    customer.get('customer_unique_id', ''),
                    customer.get('customer_zip_code_prefix', ''),
                    customer['customer_city'],
                    customer['customer_state'],
                    region,
                    city_data.get('population', 0),
                    city_data.get('gdp_capita', 0),
                    city_data.get('hdi', 0),
                    city_data.get('hdi_income', 0),
                    city_data.get('hdi_education', 0),
                    city_data.get('hdi_longevity', 0),
                    city_data.get('is_capital', 0),
                    city_data.get('category', 'None')
                ))
                
                processed += 1
                if processed % 10000 == 0:
                    print(f"    Processed {processed}/{total_customers} customers...")
            
            conn.commit()
            
            # Print matching statistics
            print(f"  Customer dimension created: {total_customers} records")
            print(f"  Matching statistics:")
            print(f"    Exact matches: {exact_matches} ({exact_matches/total_customers*100:.1f}%)")            
            print(f"    Fuzzy matches: {fuzzy_matches} ({fuzzy_matches/total_customers*100:.1f}%)")
            print(f"    No matches: {no_matches} ({no_matches/total_customers*100:.1f}%)")
            print(f"    Total matched: {exact_matches + fuzzy_matches} ({(exact_matches + fuzzy_matches)/total_customers*100:.1f}%)")
            
            return True
            
        except Exception as e:
            print(f"  Error creating customer dimension: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    def create_seller_dimension(self):
        """Create seller dimension with economic data using fuzzy matching"""
        print("Creating seller dimension with fuzzy city matching...")
        
        conn = self.connect_db()
        if not conn:
            return False
            
        cursor = conn.cursor()
        
        try:
            # Brazil regions map
            region_map = {
                'AC': 'North', 'AL': 'Northeast', 'AP': 'North', 'AM': 'North', 'BA': 'Northeast',
                'CE': 'Northeast', 'DF': 'Central-West', 'ES': 'Southeast', 'GO': 'Central-West',
                'MA': 'Northeast', 'MT': 'Central-West', 'MS': 'Central-West', 'MG': 'Southeast',
                'PA': 'North', 'PB': 'Northeast', 'PR': 'South', 'PE': 'Northeast', 'PI': 'Northeast',
                'RJ': 'Southeast', 'RN': 'Northeast', 'RS': 'South', 'RO': 'North', 'RR': 'North',
                'SC': 'South', 'SP': 'Southeast', 'SE': 'Northeast', 'TO': 'North'
            }
            
            # Prepare cities data with normalized names for fuzzy matching
            print("  Preparing cities data for fuzzy matching...")
            cities_by_state = {}
            
            for _, row in self.cities_df.iterrows():
                state = str(row['STATE']).upper().strip()
                if state not in cities_by_state:
                    cities_by_state[state] = []
                
                city_data = {
                    'original_name': row['CITY'],
                    'normalized_name': self.normalize_city_name(row['CITY']),
                    'state': state,
                    'population': row.get('IBGE_POP', 0) if pd.notna(row.get('IBGE_POP', 0)) else 0,
                    'gdp_capita': row.get('GDP_CAPITA', 0) if pd.notna(row.get('GDP_CAPITA', 0)) else 0,
                    'hdi': row.get('IDHM', 0) if pd.notna(row.get('IDHM', 0)) else 0,
                    'hdi_income': row.get('IDHM_Renda', 0) if pd.notna(row.get('IDHM_Renda', 0)) else 0,
                    'hdi_education': row.get('IDHM_Educacao', 0) if pd.notna(row.get('IDHM_Educacao', 0)) else 0,
                    'hdi_longevity': row.get('IDHM_Longevidade', 0) if pd.notna(row.get('IDHM_Longevidade', 0)) else 0,
                    'is_capital': 1 if row.get('CAPITAL', 0) == 1 else 0,
                    'category': str(row.get('CATEGORIA_TUR', 'None')) if pd.notna(row.get('CATEGORIA_TUR')) else 'None'
                }
                cities_by_state[state].append(city_data)
            
            print(f"  Loaded {len(self.cities_df)} cities from {len(cities_by_state)} states")
            
            # Track matching statistics
            exact_matches = 0
            fuzzy_matches = 0
            no_matches = 0
            
            # Process sellers
            total_sellers = len(self.sellers_df)
            processed = 0
            
            for _, seller in self.sellers_df.iterrows():
                seller_city = str(seller['seller_city']).strip()
                seller_state = str(seller['seller_state']).upper().strip()
                
                # Find best matching city using fuzzy logic
                city_data = self.fuzzy_match_city(seller_city, seller_state, cities_by_state)
                
                if city_data:
                    # Check if it was an exact match
                    normalized_seller = self.normalize_city_name(seller_city)
                    if normalized_seller == city_data['normalized_name']:
                        exact_matches += 1
                    else:
                        fuzzy_matches += 1
                        # Log fuzzy matches for debugging
                        if processed < 10:  # Log first 10 fuzzy matches
                            print(f"    Fuzzy match: '{seller_city}' -> '{city_data['original_name']}'")
                else:
                    no_matches += 1
                    if processed < 5:  # Log first 5 non-matches
                        print(f"    No match found for: '{seller_city}', {seller_state}")
                
                # Assign region
                region = region_map.get(seller_state, 'Unknown')
                
                insert_sql = """
                INSERT INTO DIM_Seller 
                (Seller_ID, Seller_Zip_Code, Seller_City, Seller_State, Seller_Region,
                 City_Population, City_GDP_Per_Capita, City_HDI, City_HDI_Income, City_HDI_Education, 
                 City_HDI_Longevity, City_Is_Capital, City_Category)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                cursor.execute(insert_sql, (
                    seller['seller_id'],
                    seller.get('seller_zip_code_prefix', ''),
                    seller['seller_city'],
                    seller['seller_state'],
                    region,
                    city_data.get('population', 0),
                    city_data.get('gdp_capita', 0),
                    city_data.get('hdi', 0),
                    city_data.get('hdi_income', 0),
                    city_data.get('hdi_education', 0),
                    city_data.get('hdi_longevity', 0),
                    city_data.get('is_capital', 0),
                    city_data.get('category', 'None')
                ))
                
                processed += 1
                if processed % 1000 == 0:
                    print(f"    Processed {processed}/{total_sellers} sellers...")
            conn.commit()
            
            # Print matching statistics
            print(f"  Seller dimension created: {total_sellers} records")
            print(f"  Matching statistics:")
            print(f"    Exact matches: {exact_matches} ({exact_matches/total_sellers*100:.1f}%)")
            print(f"    Fuzzy matches: {fuzzy_matches} ({fuzzy_matches/total_sellers*100:.1f}%)")
            print(f"    No matches: {no_matches} ({no_matches/total_sellers*100:.1f}%)")
            print(f"    Total matched: {exact_matches + fuzzy_matches} ({(exact_matches + fuzzy_matches)/total_sellers*100:.1f}%)")
            
            return True
                
        except Exception as e:
                print(f"  Error creating seller dimension: {e}")
                conn.rollback()
                return False
        finally:
            conn.close()
    
    def create_payment_dimension(self):
        """Create payment dimension"""
        print("Creating payment dimension...")
        
        conn = self.connect_db()
        if not conn:
            return False
            
        cursor = conn.cursor()
        
        try:
            # Unique payment types
            unique_payments = self.payments_df[['payment_type', 'payment_installments']].drop_duplicates()
            
            for _, payment in unique_payments.iterrows():
                payment_type = payment['payment_type']
                installments = payment['payment_installments']
                
                # Categorize payment type
                if payment_type == 'credit_card':
                    payment_category = 'Credit Card'
                    is_credit = 1
                elif payment_type == 'boleto':
                    payment_category = 'Boleto'
                    is_credit = 0
                elif payment_type == 'voucher':
                    payment_category = 'Voucher'
                    is_credit = 0
                elif payment_type == 'debit_card':
                    payment_category = 'Debit Card'
                    is_credit = 0
                else:
                    payment_category = 'Other'
                    is_credit = 0
                
                # Categorize installments
                if installments == 1:
                    installments_range = '1 installment'
                elif installments <= 3:
                    installments_range = '2-3 installments'
                elif installments <= 6:
                    installments_range = '4-6 installments'
                elif installments <= 12:
                    installments_range = '7-12 installments'
                else:
                    installments_range = '13+ installments'
                
                is_installment = 1 if installments > 1 else 0
                
                insert_sql = """
                INSERT INTO DIM_Payment 
                (Payment_Type, Payment_Category, Installments_Range, Is_Credit, Is_Installment)
                VALUES (?, ?, ?, ?, ?)
                """
                
                cursor.execute(insert_sql, (
                    payment_type,
                    payment_category,
                    installments_range,
                    is_credit,
                    is_installment
                ))
            
            conn.commit()
            print(f"  Payment dimension created: {len(unique_payments)} records")
            return True
            
        except Exception as e:
            print(f"  Error creating payment dimension: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    def create_review_dimension(self):
        """Create review dimension"""
        print("Creating review dimension...")
        
        conn = self.connect_db()
        if not conn:
            return False
            
        cursor = conn.cursor()
        
        try:
            # Unique scores
            unique_scores = [1, 2, 3, 4, 5]
            # Prepare comment length categories
            def comment_length_category(text):
                if pd.isna(text) or not text or str(text).strip() == '':
                    return 'No Comment'
                length = len(str(text))
                if length < 30:
                    return 'Short (<30)'
                elif length < 50:
                    return 'Medium (30-49)'
                elif length < 100:
                    return 'Long (50-99)'
                elif length < 200:
                    return 'Very Long (100-199)'
                else:
                    return 'Extremely Long (200+)'
            # Get unique (score, comment length category) pairs
            review_pairs = set()
            for _, row in self.reviews_df.iterrows():
                score = row.get('review_score', 0)
                comment = row.get('review_comment_message', '')
                cat = comment_length_category(comment)
                review_pairs.add((score, cat))
            # Insert each unique pair
            for score, cat in review_pairs:
                if score <= 2:
                    review_category = 'Negative'
                    satisfaction_level = 'Unsatisfied'
                elif score == 3:
                    review_category = 'Neutral'
                    satisfaction_level = 'Neutral'
                elif score >= 4:
                    review_category = 'Positive'
                    satisfaction_level = 'Satisfied'
                else:
                    review_category = 'No review'
                    satisfaction_level = 'Unknown'
                has_comment = 0 if cat == 'No Comment' else 1
                insert_sql = """
                INSERT INTO DIM_Review 
                (Review_Score, Review_Category, Satisfaction_Level, Has_Comment, Comment_Length_Category)
                VALUES (?, ?, ?, ?, ?)
                """
                cursor.execute(insert_sql, (
                    score,
                    review_category,
                    satisfaction_level,
                    has_comment,
                    cat
                ))
            # Add record for no review at all
            cursor.execute(
                insert_sql,
                (0, 'No review', 'Unknown', 0, 'No Comment')
            )
            conn.commit()
            print(f"  Review dimension created: {len(review_pairs)+1} records")
            return True
            
        except Exception as e:
            print(f"  Error creating review dimension: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    def create_fact_table(self):
        """Create fact table"""
        print("Creating fact table...")
        
        conn = self.connect_db()
        if not conn:
            return False
            
        cursor = conn.cursor()
        
        try:
            # Prepare data for fact table
            print("  Joining data from different sources...")
            
            # Join orders with items
            fact_data = self.orders_df.merge(
                self.order_items_df.groupby('order_id').agg({
                    'price': 'sum',
                    'freight_value': 'sum',
                    'order_item_id': 'count',
                    'seller_id': 'first'  # First seller in the order
                }).reset_index(),
                on='order_id', how='inner'
            )
            
            # Join with payments
            fact_data = fact_data.merge(
                self.payments_df.groupby('order_id').agg({
                    'payment_type': 'first',
                    'payment_installments': 'first',
                    'payment_value': 'sum'
                }).reset_index(),
                on='order_id', how='left'
            )
            
            # Join with reviews
            fact_data = fact_data.merge(
                self.reviews_df[['order_id', 'review_score']],
                on='order_id', how='left'
            )
            
            print(f"  Prepared {len(fact_data)} fact records")
            
            # Get dimension keys
            dim_keys = {}
            
            # Time keys
            cursor.execute("SELECT Time_Key, Date_Value FROM DIM_Time")
            dim_keys['time'] = {row[1]: row[0] for row in cursor.fetchall()}
            
            # Customer keys
            cursor.execute("SELECT Customer_Key, Customer_ID FROM DIM_Customer")
            dim_keys['customer'] = {row[1]: row[0] for row in cursor.fetchall()}
            
            # Seller keys
            cursor.execute("SELECT Seller_Key, Seller_ID FROM DIM_Seller")
            dim_keys['seller'] = {row[1]: row[0] for row in cursor.fetchall()}
            
            # Payment keys
            cursor.execute("SELECT Payment_Key, Payment_Type, Installments_Range FROM DIM_Payment")
            payment_keys = {}
            for row in cursor.fetchall():
                payment_keys[f"{row[1]}_{row[2]}"] = row[0]
            
            # Review keys
            cursor.execute("SELECT Review_Key, Review_Score FROM DIM_Review")
            dim_keys['review'] = {row[1]: row[0] for row in cursor.fetchall()}
            
            # Insert records into fact table
            inserted_count = 0
            
            for _, row in fact_data.iterrows():
                try:
                    # Convert dates
                    purchase_date = pd.to_datetime(row['order_purchase_timestamp']).date()
                    delivery_date = pd.to_datetime(row.get('order_delivered_customer_date', None), errors='coerce')
                    estimated_delivery = pd.to_datetime(row.get('order_estimated_delivery_date', None), errors='coerce')
                    
                    # Calculate delivery days
                    delivery_days = None
                    if delivery_date is not None and not pd.isna(delivery_date):
                        delivery_days = (delivery_date.date() - purchase_date).days
                        delivery_date = delivery_date.date()
                    else:
                        delivery_date = None
                    
                    if estimated_delivery is not None and not pd.isna(estimated_delivery):
                        estimated_delivery = estimated_delivery.date()
                    else:
                        estimated_delivery = None
                    
                    # Lookup dimension keys
                    time_key = dim_keys['time'].get(purchase_date)
                    customer_key = dim_keys['customer'].get(row['customer_id'])
                    seller_key = dim_keys['seller'].get(row.get('seller_id'))
                    
                    # Payment key
                    payment_type = row.get('payment_type', 'unknown')
                    installments = row.get('payment_installments', 1)
                    
                    if installments == 1:
                        installments_range = '1 installment'
                    elif installments <= 3:
                        installments_range = '2-3 installments'
                    elif installments <= 6:
                        installments_range = '4-6 installments'
                    elif installments <= 12:
                        installments_range = '7-12 installments'
                    else:
                        installments_range = '13+ installments'
                    
                    payment_key = payment_keys.get(f"{payment_type}_{installments_range}")
                    
                    # Review key
                    review_score = row.get('review_score', 0)
                    if pd.isna(review_score):
                        review_score = 0
                    review_key = dim_keys['review'].get(int(review_score))
                    
                    if all([time_key, customer_key, seller_key, payment_key, review_key]):
                        insert_sql = """
                        INSERT INTO FACT_Orders 
                        (Order_ID, Time_Key, Customer_Key, Seller_Key, Payment_Key, Review_Key,
                         Order_Value, Freight_Value, Items_Count, Delivery_Days, Review_Score,
                         Purchase_Date, Delivery_Date, Estimated_Delivery_Date)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """
                        
                        cursor.execute(insert_sql, (
                            row['order_id'],
                            time_key,
                            customer_key,
                            seller_key,
                            payment_key,
                            review_key,
                            float(row.get('price', 0)),
                            float(row.get('freight_value', 0)),
                            int(row.get('order_item_id', 1)),
                            delivery_days,
                            int(review_score) if review_score > 0 else None,
                            purchase_date,
                            delivery_date,
                            estimated_delivery
                        ))
                        
                        inserted_count += 1
                        
                        if inserted_count % 1000 == 0:
                            print(f"    Inserted {inserted_count} records...")
                            conn.commit()
                
                except Exception as e:
                    print(f"    Warning: Error in record {row['order_id']}: {e}")
                    continue
            
            conn.commit()
            print(f"  Fact table created: {inserted_count} records")
            return True
            
        except Exception as e:
            print(f"  Error creating fact table: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    def run_etl_process(self):
        """Main ETL process"""
        print("Starting Olist data warehouse ETL process...")
        print("=" * 60)
        
        steps = [
            ("Load data", self.load_data_files),
            ("Create schema", self.create_database_schema),
            ("Time dimension", self.create_time_dimension),
            ("Customer dimension", self.create_customer_dimension),
            ("Seller dimension", self.create_seller_dimension),
            ("Payment dimension", self.create_payment_dimension),
            ("Review dimension", self.create_review_dimension),
            ("Fact table", self.create_fact_table),
        ]
        
        for step_name, step_function in steps:
            print(f"\nStep: {step_name}")
            if not step_function():
                print(f"ETL process stopped at step: {step_name}")
                return False
        
        print("\n" + "=" * 60)
        print("ETL PROCESS COMPLETED SUCCESSFULLY!")
        
        return True

def main():
    """Main function to run the ETL process."""
    dw = OlistDataWarehouse()
    dw.run_etl_process()

if __name__ == "__main__":
    main()
