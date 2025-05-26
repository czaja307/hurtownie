from typing import List, Tuple
from utils import *
from config import ETLConfig

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