from config import ETLConfig
from utils import *
from extract import T1_DataExtractor
from schema import T2_SchemaManager
from transform_load import T3_DimensionBuilder, T4_FactBuilder
import time

class T5_ETLOrchestrator:
    """Task 5: Main ETL process orchestrator with monitoring and validation"""
    
    def __init__(self, config: ETLConfig = None):
        self.config = ETLConfig()
        self.logger = ETLLogger()
        self.metrics = ETLMetrics()
        
        # Initialize components
        self.db_manager = DatabaseManager(self.config, self.logger)
        self.quality_manager = DataQualityManager(self.logger)
        
        # Initialize task components
        self.data_extractor = None
        self.schema_manager = None
        self.dimension_builder = None
        self.fact_builder = None
    
    def execute_full_etl(self) -> bool:
        """Execute complete ETL process with monitoring"""
        self.logger.info("="*80)
        self.logger.info("STARTING OLIST DATA WAREHOUSE ETL PROCESS v2.0")
        self.logger.info("="*80)
        
        self.metrics.start_time = datetime.now()
        
        try:
            # Define ETL pipeline
            pipeline_steps = [
                ("T1_Extract_Data", self._execute_data_extraction),
                ("T2_Create_Schema", self._execute_schema_creation),
                ("T3_Build_Dimensions", self._execute_dimension_building),
                ("T4_Build_Facts", self._execute_fact_building),
                ("T5_Validate_Results", self._execute_final_validation)
            ]
            
            # Execute pipeline steps
            for step_name, step_function in pipeline_steps:
                self.logger.info(f"\n{'='*60}")
                self.logger.info(f"EXECUTING STEP: {step_name}")
                self.logger.info(f"{'='*60}")
                
                step_start = time.time()
                success = step_function()
                step_duration = time.time() - step_start
                
                if success:
                    self.logger.info(f"{step_name} completed successfully in {step_duration:.2f}s")
                else:
                    self.logger.error(f"{step_name} FAILED after {step_duration:.2f}s")
                    self.logger.error("ETL PROCESS TERMINATED DUE TO FAILURE")
                    return False
            
            self.metrics.end_time = datetime.now()
            self._generate_final_report()
            
            self.logger.info("="*80)
            self.logger.info("ETL PROCESS COMPLETED SUCCESSFULLY!")
            self.logger.info("="*80)
            
            return True
            
        except Exception as e:
            self.logger.error(f"CRITICAL ETL FAILURE: {e}")
            return False
    
    def _execute_data_extraction(self) -> bool:
        """Execute T1: Data Extraction"""
        self.data_extractor = T1_DataExtractor(self.config, self.logger, self.quality_manager)
        success = self.data_extractor.execute()
        
        if success:
            # Store metrics
            for table_name in ['orders', 'order_items', 'customers', 'sellers', 'payments', 'reviews', 'cities']:
                df = self.data_extractor.get_dataframe(table_name)
                if df is not None:
                    self.metrics.records_processed[table_name] = len(df)
        
        return success
    
    def _execute_schema_creation(self) -> bool:
        """Execute T2: Schema Creation"""
        self.schema_manager = T2_SchemaManager(self.config, self.logger, self.db_manager)
        return self.schema_manager.execute()
    
    def _execute_dimension_building(self) -> bool:
        """Execute T3: Dimension Building"""
        self.dimension_builder = T3_DimensionBuilder(
            self.config, self.logger, self.db_manager, 
            self.data_extractor, self.quality_manager
        )
        success = self.dimension_builder.execute()
        
        if success:
            # Store dimension metrics
            self.metrics.fuzzy_match_stats = self.dimension_builder.metrics
        
        return success
    
    def _execute_fact_building(self) -> bool:
        """Execute T4: Fact Building"""
        self.fact_builder = T4_FactBuilder(
            self.config, self.logger, self.db_manager, self.data_extractor
        )
        return self.fact_builder.execute()
    
    def _execute_final_validation(self) -> bool:
        """Execute T5: Final Validation and Quality Checks"""
        self.logger.info("T5: Performing final data validation...")
        
        conn = self.db_manager.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            validation_passed = True
            
            # Validate table counts
            table_counts = self._get_table_counts(cursor)
            self.metrics.records_loaded = table_counts
            
            # Validate referential integrity
            integrity_checks = self._validate_referential_integrity(cursor)
            
            # Validate data quality
            quality_checks = self._validate_data_quality(cursor)
            
            if not all([integrity_checks, quality_checks]):
                validation_passed = False
            
            # Log validation results
            self._log_validation_results(table_counts, integrity_checks, quality_checks)
            
            return validation_passed
            
        except Exception as e:
            self.logger.error(f"T5: Final validation failed: {e}")
            return False
        finally:
            conn.close()
    
    def _get_table_counts(self, cursor) -> Dict[str, int]:
        """Get record counts for all tables"""
        tables = ['DIM_Time', 'DIM_Customer', 'DIM_Seller', 'DIM_Payment', 'DIM_Review', 'FACT_Orders']
        counts = {}
        
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                counts[table] = count
                self.logger.info(f"T5: {table}: {count:,} records")
            except Exception as e:
                self.logger.error(f"T5: Failed to count {table}: {e}")
                counts[table] = 0
        
        return counts
    
    def _validate_referential_integrity(self, cursor) -> bool:
        """Validate foreign key relationships"""
        self.logger.info("T5: Validating referential integrity...")
        
        integrity_checks = [
            ("FACT_Orders -> DIM_Time", """
                SELECT COUNT(*) FROM FACT_Orders f 
                LEFT JOIN DIM_Time d ON f.Time_Key = d.Time_Key 
                WHERE d.Time_Key IS NULL
            """),
            ("FACT_Orders -> DIM_Customer", """
                SELECT COUNT(*) FROM FACT_Orders f 
                LEFT JOIN DIM_Customer d ON f.Customer_Key = d.Customer_Key 
                WHERE d.Customer_Key IS NULL
            """),
            ("FACT_Orders -> DIM_Seller", """
                SELECT COUNT(*) FROM FACT_Orders f 
                LEFT JOIN DIM_Seller d ON f.Seller_Key = d.Seller_Key 
                WHERE d.Seller_Key IS NULL
            """),
            ("FACT_Orders -> DIM_Payment", """
                SELECT COUNT(*) FROM FACT_Orders f 
                LEFT JOIN DIM_Payment d ON f.Payment_Key = d.Payment_Key 
                WHERE d.Payment_Key IS NULL
            """),
            ("FACT_Orders -> DIM_Review", """
                SELECT COUNT(*) FROM FACT_Orders f 
                LEFT JOIN DIM_Review d ON f.Review_Key = d.Review_Key 
                WHERE d.Review_Key IS NULL
            """)
        ]
        
        all_checks_passed = True
        
        for check_name, sql in integrity_checks:
            try:
                cursor.execute(sql)
                orphaned_count = cursor.fetchone()[0]
                
                if orphaned_count == 0:
                    self.logger.info(f"T5: ✓ {check_name}: No orphaned records")
                else:
                    self.logger.warning(f"T5: ✗ {check_name}: {orphaned_count} orphaned records")
                    all_checks_passed = False
                    
            except Exception as e:
                self.logger.error(f"T5: Failed integrity check {check_name}: {e}")
                all_checks_passed = False
        
        return all_checks_passed
    
    def _validate_data_quality(self, cursor) -> bool:
        """Validate data quality metrics"""
        self.logger.info("T5: Validating data quality...")
        
        quality_checks = [
            ("Fact table measures", """
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN Order_Value < 0 THEN 1 END) as negative_values,
                    COUNT(CASE WHEN Order_Value IS NULL THEN 1 END) as null_values,
                    AVG(Order_Value) as avg_order_value,
                    MAX(Order_Value) as max_order_value
                FROM FACT_Orders
            """),
            ("Date ranges", """
                SELECT 
                    MIN(Purchase_Date) as min_date,
                    MAX(Purchase_Date) as max_date,
                    COUNT(CASE WHEN Purchase_Date < '2016-01-01' OR Purchase_Date > '2019-12-31' THEN 1 END) as out_of_range
                FROM FACT_Orders
            """),
            ("Customer distribution", """
                SELECT 
                    COUNT(DISTINCT Customer_Key) as unique_customers,
                    COUNT(*) as total_orders,
                    CAST(COUNT(*) as FLOAT) / COUNT(DISTINCT Customer_Key) as avg_orders_per_customer
                FROM FACT_Orders
            """)
        ]
        
        all_checks_passed = True
        
        for check_name, sql in quality_checks:
            try:
                cursor.execute(sql)
                result = cursor.fetchone()
                self.logger.info(f"T5: {check_name}: {dict(zip([desc[0] for desc in cursor.description], result))}")
                
            except Exception as e:
                self.logger.error(f"T5: Failed quality check {check_name}: {e}")
                all_checks_passed = False
        
        return all_checks_passed
    
    def _log_validation_results(self, table_counts: Dict, integrity_ok: bool, quality_ok: bool):
        """Log comprehensive validation results"""
        self.logger.info("T5: VALIDATION SUMMARY")
        self.logger.info("-" * 40)
        
        # Table counts
        total_records = sum(table_counts.values())
        self.logger.info(f"Total records loaded: {total_records:,}")
        
        # Status indicators
        integrity_status = "PASSED" if integrity_ok else "FAILED"
        quality_status = "PASSED" if quality_ok else "FAILED"
        
        self.logger.info(f"Referential Integrity: {integrity_status}")
        self.logger.info(f"Data Quality: {quality_status}")
    
    def _generate_final_report(self):
        """Generate comprehensive ETL execution report"""
        duration = self.metrics.end_time - self.metrics.start_time
        
        self.logger.info("\n" + "="*80)
        self.logger.info("ETL EXECUTION REPORT")
        self.logger.info("="*80)
        
        # Timing information
        self.logger.info(f"Start Time: {self.metrics.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"End Time: {self.metrics.end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"Total Duration: {duration}")
        
        # Processing statistics
        self.logger.info(f"\nSOURCE DATA PROCESSED:")
        for table, count in self.metrics.records_processed.items():
            self.logger.info(f"  {table:15}: {count:,} records")
        
        # Loading statistics
        if self.metrics.records_loaded:
            self.logger.info(f"\nWAREHOUSE DATA LOADED:")
            for table, count in self.metrics.records_loaded.items():
                self.logger.info(f"  {table:15}: {count:,} records")
        
        # Fuzzy matching statistics
        if self.metrics.fuzzy_match_stats:
            self.logger.info(f"\nFUZZY MATCHING STATISTICS:")
            for dim_type, stats in self.metrics.fuzzy_match_stats.items():
                if dim_type in ['customer', 'seller']:
                    match_rate = stats.get('match_rate', 0)
                    self.logger.info(f"  {dim_type.title()}: {match_rate:.1f}% match rate")
        
        # Performance metrics
        total_source_records = sum(self.metrics.records_processed.values())
        total_warehouse_records = sum(self.metrics.records_loaded.values()) if self.metrics.records_loaded else 0
        
        if duration.total_seconds() > 0:
            throughput = total_source_records / duration.total_seconds()
            self.logger.info(f"\nPERFORMANCE METRICS:")
            self.logger.info(f"  Processing Throughput: {throughput:.0f} records/second")
            self.logger.info(f"  Total Source Records: {total_source_records:,}")
            self.logger.info(f"  Total Warehouse Records: {total_warehouse_records:,}")


def main():
    """Main entry point for ETL execution"""
    config = ETLConfig()
    
    # Execute full ETL
    orchestrator = T5_ETLOrchestrator(config)
    success = orchestrator.execute_full_etl()
    
    if success:
        print("\nETL PROCESS COMPLETED SUCCESSFULLY!")
        print("Your data warehouse is ready for analysis.")
    else:
        print("\nETL PROCESS FAILED!")
        print("Please check the logs for error details.")
    
    return success


if __name__ == "__main__":
    main()
 