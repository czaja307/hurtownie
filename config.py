from dataclasses import dataclass

@dataclass
class ETLConfig:
    """Configuration class for ETL process"""
    # Database configuration
    server: str = '192.168.11.131'
    database: str = 'Olist'
    driver: str = 'SQL Server Native Client 11.0'
    username: str = 'sa'
    password: str = 'password'
    
    # Data paths
    data_path: str = r'c:\Users\Kuba\PycharmProjects\hurtownie\data'
    
    # Processing parameters
    batch_size: int = 10000
    fuzzy_threshold: int = 80
    
    # Date ranges
    start_date: str = '2016-01-01'
    end_date: str = '2019-12-31'