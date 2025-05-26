import pyodbc
import pandas as pd
from datetime import datetime
import logging
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from fuzzywuzzy import fuzz, process
from unidecode import unidecode
from config import ETLConfig

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