import pyodbc
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import logging
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from fuzzywuzzy import fuzz, process
from unidecode import unidecode


@dataclass
class ETLConfig:
    """Configuration class for ETL process"""
    # Database configuration
    server: str = '192.168.1.204'
    database: str = 'Olist'
    driver: str = 'SQL Server Native Client 11.0'
    username: str = 'sa'
    password: str = 'password'
    
    # Data paths
    data_path: str = r'c:\Users\Kuba\PycharmProjects\hurtownie\data'
    
    # Processing parameters
    batch_size: int = 1000
    fuzzy_threshold: int = 80
    
    # Date ranges
    start_date: str = '2016-01-01'
    end_date: str = '2019-12-31'


@dataclass 
class ETLMetrics:
    """Class to track ETL process metrics"""
    start_time: datetime = None
    end_time: datetime = None
    records_processed: Dict[str, int] = None
    records_loaded: Dict[str, int] = None
    errors_encountered: Dict[str, int] = None
    fuzzy_match_stats: Dict[str, Dict[str, int]] = None
    
    def __post_init__(self):
        if self.records_processed is None:
            self.records_processed = {}
        if self.records_loaded is None:
            self.records_loaded = {}
        if self.errors_encountered is None:
            self.errors_encountered = {}
        if self.fuzzy_match_stats is None:
            self.fuzzy_match_stats = {}


class ETLLogger:
    """Enhanced logging for ETL process"""
    
    def __init__(self, log_level=logging.INFO):
        self.logger = logging.getLogger('OlistETL')
        self.logger.setLevel(log_level)
        
        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        
        # Create file handler
        file_handler = logging.FileHandler('etl_process.log')
        file_handler.setLevel(logging.DEBUG)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)
        
        # Add handlers
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)
    
    def info(self, message: str):
        self.logger.info(message)
    
    def error(self, message: str):
        self.logger.error(message)
    
    def warning(self, message: str):
        self.logger.warning(message)
    
    def debug(self, message: str):
        self.logger.debug(message)


class DatabaseManager:
    """Database connection and management utilities"""
    
    def __init__(self, config: ETLConfig, logger: ETLLogger):
        self.config = config
        self.logger = logger
        self.connection_string = self._build_connection_string()
    
    def _build_connection_string(self) -> str:
        """Build database connection string"""
        return (
            f'DRIVER={{{self.config.driver}}};'
            f'SERVER={self.config.server};'
            f'DATABASE={self.config.database};'
            f'UID={self.config.username};'
            f'PWD={self.config.password};'
            f'Encrypt=no;'
        )
    
    def get_connection(self) -> Optional[pyodbc.Connection]:
        """Establish database connection with error handling"""
        try:
            conn = pyodbc.connect(self.connection_string)
            self.logger.info("Database connection established successfully")
            return conn
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {e}")
            return None
    
    def execute_sql(self, sql: str, params: Tuple = None) -> bool:
        """Execute SQL statement with error handling"""
        conn = self.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            conn.commit()
            return True
        except Exception as e:
            self.logger.error(f"SQL execution failed: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()


class DataQualityManager:
    """Data quality and validation utilities"""
    
    def __init__(self, logger: ETLLogger):
        self.logger = logger
    
    def validate_dataframe(self, df: pd.DataFrame, table_name: str, 
                          required_columns: List[str]) -> bool:
        """Validate DataFrame structure and basic quality"""
        try:
            # Check if DataFrame is empty
            if df.empty:
                self.logger.error(f"{table_name}: DataFrame is empty")
                return False
            
            # Check required columns
            missing_cols = set(required_columns) - set(df.columns)
            if missing_cols:
                self.logger.error(f"{table_name}: Missing columns: {missing_cols}")
                return False
            
            # Log basic statistics
            self.logger.info(f"{table_name}: {len(df)} records, {len(df.columns)} columns")
            self.logger.debug(f"{table_name}: Columns: {list(df.columns)}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"DataFrame validation failed for {table_name}: {e}")
            return False
    
    def normalize_city_name(self, city_name: str) -> str:
        """Normalize city name for fuzzy matching"""
        if pd.isna(city_name) or not city_name:
            return ""
        
        # Convert to string and normalize
        normalized = str(city_name).lower().strip()
        # Remove accents
        normalized = unidecode(normalized)
        # Clean up special characters
        replacements = {"'": "", '"': "", "-": " ", "_": " "}
        
        for old, new in replacements.items():
            normalized = normalized.replace(old, new)
        
        # Remove multiple spaces
        normalized = " ".join(normalized.split())
        return normalized
    
    def fuzzy_match_city(self, target_city: str, target_state: str, 
                        cities_by_state: Dict, threshold: int = 80) -> Dict[str, Any]:
        """Find best matching city using fuzzy matching"""
        if target_state not in cities_by_state:
            return {}
        
        normalized_target = self.normalize_city_name(target_city)
        if not normalized_target:
            return {}
        
        state_cities = cities_by_state[target_state]
        
        # Try exact match first
        for city_data in state_cities:
            if normalized_target == city_data['normalized_name']:
                return city_data
        
        # Try fuzzy match
        city_names = [city['normalized_name'] for city in state_cities]
        match = process.extractOne(normalized_target, city_names, scorer=fuzz.ratio)
        
        if match and match[1] >= threshold:
            matched_city = next(city for city in state_cities 
                              if city['normalized_name'] == match[0])
            return matched_city
        
        return {}


class T1_DataExtractor:
    """Task 1: Extract and validate source data files"""
    
    def __init__(self, config: ETLConfig, logger: ETLLogger, quality_manager: DataQualityManager):
        self.config = config
        self.logger = logger
        self.quality_manager = quality_manager
        self.data_frames = {}
    
    def execute(self) -> bool:
        """Execute data extraction process"""
        self.logger.info("=== T1: Starting Data Extraction ===")
        
        try:
            # Define file mappings
            file_mappings = {
                'orders': 'olist/olist_orders_dataset.csv',
                'order_items': 'olist/olist_order_items_dataset.csv', 
                'customers': 'olist/olist_customers_dataset.csv',
                'sellers': 'olist/olist_sellers_dataset.csv',
                'payments': 'olist/olist_order_payments_dataset.csv',
                'reviews': 'olist/olist_order_reviews_dataset.csv',
                'cities': 'cities/BRAZIL_CITIES_REV2022.CSV'
            }
            
            # Extract each file
            for table_name, file_path in file_mappings.items():
                if not self._extract_file(table_name, file_path):
                    return False
            
            # Validate extracted data
            if not self._validate_extracted_data():
                return False
            
            self.logger.info("T1: Data extraction completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"T1: Data extraction failed: {e}")
            return False
    
    def _extract_file(self, table_name: str, file_path: str) -> bool:
        """Extract single CSV file"""
        try:
            full_path = os.path.join(self.config.data_path, file_path)
            
            if not os.path.exists(full_path):
                self.logger.error(f"T1: File not found: {full_path}")
                return False
            
            # Load CSV with error handling
            df = pd.read_csv(full_path, encoding='utf-8')
            self.data_frames[table_name] = df
            
            self.logger.info(f"T1: Loaded {table_name}: {len(df)} records")
            return True
            
        except Exception as e:
            self.logger.error(f"T1: Failed to load {table_name}: {e}")
            return False
    
    def _validate_extracted_data(self) -> bool:
        """Validate all extracted data"""
        validation_rules = {
            'orders': ['order_id', 'customer_id', 'order_status'],
            'order_items': ['order_id', 'seller_id', 'product_id'],
            'customers': ['customer_id', 'customer_city', 'customer_state'],
            'sellers': ['seller_id', 'seller_city', 'seller_state'],
            'payments': ['order_id', 'payment_type'],
            'reviews': ['order_id', 'review_score'],
            'cities': ['CITY', 'STATE']
        }
        
        for table_name, required_cols in validation_rules.items():
            df = self.data_frames.get(table_name)
            if df is None:
                self.logger.error(f"T1: Missing data for {table_name}")
                return False
            
            if not self.quality_manager.validate_dataframe(df, table_name, required_cols):
                return False
        
        return True
    
    def get_dataframe(self, table_name: str) -> Optional[pd.DataFrame]:
        """Get extracted DataFrame"""
        return self.data_frames.get(table_name)


class T2_SchemaManager:
    """Task 2: Create and manage database schema"""
    
    def __init__(self, config: ETLConfig, logger: ETLLogger, db_manager: DatabaseManager):
        self.config = config
        self.logger = logger
        self.db_manager = db_manager
    
    def execute(self) -> bool:
        """Execute schema creation process"""
        self.logger.info("=== T2: Starting Schema Creation ===")
        
        conn = self.db_manager.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            
            # Drop existing tables in correct order
            if not self._drop_existing_tables(cursor):
                return False
            
            # Create new tables
            if not self._create_tables(cursor):
                return False
            
            conn.commit()
            self.logger.info("T2: Schema creation completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"T2: Schema creation failed: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    def _drop_existing_tables(self, cursor) -> bool:
        """Drop existing tables in correct order"""
        try:
            drop_order = [
                "FACT_Orders",
                "DIM_Time", 
                "DIM_Customer",
                "DIM_Seller",
                "DIM_Payment",
                "DIM_Review"
            ]
            
            for table in drop_order:
                cursor.execute(f"DROP TABLE IF EXISTS {table};")
                self.logger.info(f"T2: Dropped table {table}")
            
            return True
        except Exception as e:
            self.logger.error(f"T2: Failed to drop tables: {e}")
            return False
    
    def _create_tables(self, cursor) -> bool:
        """Create all data warehouse tables"""
        try:
            table_definitions = self._get_table_definitions()
            
            for table_name, sql in table_definitions:
                cursor.execute(sql)
                self.logger.info(f"T2: Created table {table_name}")
            
            return True
        except Exception as e:
            self.logger.error(f"T2: Failed to create tables: {e}")
            return False
    
    def _get_table_definitions(self) -> List[Tuple[str, str]]:
        """Get all table creation SQL statements"""
        return [
            ("DIM_Time", """
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
            """),
            ("DIM_Customer", """
                CREATE TABLE DIM_Customer (
                    Customer_Key INT IDENTITY(1,1) PRIMARY KEY,
                    Customer_ID NVARCHAR(50) NOT NULL,
                    Customer_Unique_ID NVARCHAR(50),
                    Customer_Zip_Code NVARCHAR(10),
                    Customer_City NVARCHAR(100),
                    Customer_State NVARCHAR(5),
                    Customer_Region NVARCHAR(50),
                    City_Population INT,
                    City_GDP_Per_Capita DECIMAL(15,2),
                    City_HDI DECIMAL(5,4),
                    City_HDI_Income DECIMAL(5,4),
                    City_HDI_Education DECIMAL(5,4),
                    City_HDI_Longevity DECIMAL(5,4),
                    City_Is_Capital BIT,
                    City_Category NVARCHAR(50)
                );
            """),
            ("DIM_Seller", """
                CREATE TABLE DIM_Seller (
                    Seller_Key INT IDENTITY(1,1) PRIMARY KEY,
                    Seller_ID NVARCHAR(50) NOT NULL,
                    Seller_Zip_Code NVARCHAR(10),
                    Seller_City NVARCHAR(100),
                    Seller_State NVARCHAR(5),
                    Seller_Region NVARCHAR(50),
                    City_Population INT,
                    City_GDP_Per_Capita DECIMAL(15,2),
                    City_HDI DECIMAL(5,4),
                    City_HDI_Income DECIMAL(5,4),
                    City_HDI_Education DECIMAL(5,4),
                    City_HDI_Longevity DECIMAL(5,4),
                    City_Is_Capital BIT,
                    City_Category NVARCHAR(50)
                );
            """),
            ("DIM_Payment", """
                CREATE TABLE DIM_Payment (
                    Payment_Key INT IDENTITY(1,1) PRIMARY KEY,
                    Payment_Type NVARCHAR(50),
                    Payment_Category NVARCHAR(50),
                    Installments_Range NVARCHAR(20),
                    Is_Credit BIT,
                    Is_Installment BIT
                );
            """),
            ("DIM_Review", """
                CREATE TABLE DIM_Review (
                    Review_Key INT IDENTITY(1,1) PRIMARY KEY,
                    Review_Score INT,
                    Review_Category NVARCHAR(30),
                    Satisfaction_Level NVARCHAR(20),
                    Has_Comment BIT,
                    Comment_Length_Category NVARCHAR(30)
                );
            """),
            ("FACT_Orders", """
                CREATE TABLE FACT_Orders (
                    Order_Key INT IDENTITY(1,1) PRIMARY KEY,
                    Order_ID NVARCHAR(50) NOT NULL,
                    Time_Key INT,
                    Customer_Key INT,
                    Seller_Key INT,
                    Payment_Key INT,
                    Review_Key INT,
                    Order_Value DECIMAL(15,2),
                    Freight_Value DECIMAL(15,2),
                    Items_Count INT,
                    Delivery_Days INT,
                    Review_Score INT,
                    Purchase_Date DATE,
                    Delivery_Date DATE,
                    Estimated_Delivery_Date DATE,
                    FOREIGN KEY (Time_Key) REFERENCES DIM_Time(Time_Key),
                    FOREIGN KEY (Customer_Key) REFERENCES DIM_Customer(Customer_Key),
                    FOREIGN KEY (Seller_Key) REFERENCES DIM_Seller(Seller_Key),
                    FOREIGN KEY (Payment_Key) REFERENCES DIM_Payment(Payment_Key),
                    FOREIGN KEY (Review_Key) REFERENCES DIM_Review(Review_Key)
                );
            """)
        ]