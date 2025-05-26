import pandas as pd
import os
from typing import Optional
from utils import *
from config import ETLConfig

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