# Olist Data Warehouse ETL Process Documentation

## Overview
This ETL process transforms raw e-commerce data from the Olist Brazilian marketplace into a star schema data warehouse optimized for OLAP operations.

## Architecture

### Data Sources
- **Olist CSV Files**: Orders, customers, sellers, payments, reviews, order items
- **Brazilian Cities Data**: Economic indicators, demographics, geographic information

### Target Schema
**Star Schema** with 1 fact table and 5 dimension tables:

```
FACT_Orders (Central fact table)
├── DIM_Time (Date hierarchy)
├── DIM_Customer (Geographic + economic data)
├── DIM_Seller (Geographic + economic data)  
├── DIM_Payment (Payment methods & installments)
└── DIM_Review (Review scores & satisfaction)
```

## ETL Process Map

### Phase 1: Extract & Prepare (E)
```
┌─────────────────────┐
│   T1_Extract_Data   │ → Load CSV files from filesystem
│                     │   Validate file existence & structure
│                     │   Basic data quality checks
└─────────────────────┘
```

### Phase 2: Transform & Clean (T)
```
┌─────────────────────┐
│ T2_Create_Schema    │ → Drop/Create database tables
│                     │   Define foreign key relationships
│                     │   Set up indexes for performance
└─────────────────────┘
          ↓
┌─────────────────────┐
│ T3_Build_Dimensions │ → Process dimension tables
│                     │   Apply data cleansing rules
│                     │   Fuzzy matching for cities
│                     │   Generate surrogate keys
└─────────────────────┘
          ↓
┌─────────────────────┐
│ T4_Build_Facts      │ → Join source tables
│                     │   Lookup dimension keys
│                     │   Calculate measures
│                     │   Handle data quality issues
└─────────────────────┘
```

### Phase 3: Load & Validate (L)
```
┌─────────────────────┐
│ T5_Load_Warehouse   │ → Bulk insert into target tables
│                     │   Validate referential integrity
│                     │   Generate load statistics
│                     │   Create data quality reports
└─────────────────────┘
```

## Task Breakdown

### T1_Extract_Data
**Purpose**: Extract and validate source data files
- **Input**: CSV files from filesystem
- **Output**: Pandas DataFrames in memory
- **Data Quality**: File existence, column validation, basic statistics
- **Error Handling**: Missing files, corrupt data, encoding issues

### T2_Create_Schema  
**Purpose**: Prepare target database structure
- **Input**: Database connection parameters
- **Output**: Empty star schema tables
- **Operations**: DROP existing tables, CREATE new tables with proper data types
- **Error Handling**: Connection failures, permission issues

### T3_Build_Dimensions
**Purpose**: Populate dimension tables with cleansed data
#### T3.1_Time_Dimension
- **Logic**: Generate calendar from 2016-2019
- **Hierarchy**: Year → Quarter → Month → Day
- **Attributes**: Weekends, holidays, business days

#### T3.2_Customer_Dimension  
- **Logic**: Denormalize customers with city economic data
- **Automation**: Fuzzy matching for city names using Levenshtein distance
- **Enhancement**: Regional categorization, economic indicators

#### T3.3_Seller_Dimension
- **Logic**: Similar to customers, with seller-specific attributes
- **Automation**: Same fuzzy matching algorithm
- **Quality**: Track matching success rates

#### T3.4_Payment_Dimension
- **Logic**: Categorize payment types and installment ranges
- **Rules**: Credit vs non-credit, installment categorization

#### T3.5_Review_Dimension  
- **Logic**: Score categorization and comment analysis
- **Automation**: Comment length classification
- **Categories**: Satisfaction levels (Positive/Neutral/Negative)

### T4_Build_Facts
**Purpose**: Create denormalized fact table
- **Logic**: JOIN orders + items + payments + reviews
- **Key Lookup**: Replace business keys with surrogate keys
- **Measures**: Calculate additive and non-additive measures
- **Quality**: Handle missing relationships, orphaned records

### T5_Load_Warehouse
**Purpose**: Final data loading and validation  
- **Operations**: Bulk INSERT with batch processing
- **Performance**: Commit every 1000 records
- **Validation**: Foreign key constraints, data ranges
- **Reporting**: Load statistics, error summary

## Automation Features

### 1. Fuzzy City Matching
- **Algorithm**: Levenshtein distance with threshold (80%)
- **Normalization**: Remove accents, standardize spacing
- **Fallback**: Exact match → Fuzzy match → Default values

### 2. Error Handling
- **Database**: Automatic rollback on failures
- **Data Quality**: Skip bad records with logging
- **Progress Tracking**: Batch progress indicators

### 3. Data Validation
- **Type Conversion**: Safe parsing with defaults
- **Business Rules**: Valid date ranges, positive amounts
- **Referential Integrity**: Ensure FK relationships exist

### 4. Incremental Loading Support
- **Design**: Process includes date-based filtering capability
- **Implementation**: Can be extended for delta loads
- **Monitoring**: Track processing timestamps

## Quality Measures

### Data Quality Checks
1. **Completeness**: Record counts, null value analysis
2. **Validity**: Data type validation, range checks
3. **Consistency**: Cross-table relationship validation
4. **Accuracy**: Fuzzy matching success rates

### Performance Optimization
1. **Batch Processing**: 1000-record commits
2. **Error Recovery**: Continue processing after individual record failures
3. **Memory Management**: Stream processing for large datasets
4. **Database Optimization**: Proper indexing strategy

## Error Handling Strategy

### Recovery Mechanisms
- **Transaction Management**: Rollback on critical failures
- **Partial Success**: Continue processing after minor errors
- **Logging**: Detailed error tracking with context
- **Monitoring**: Progress indicators and statistics

### Data Quality Issues
- **Missing Values**: Default value assignment
- **Invalid Dates**: Error logging and null assignment
- **Orphaned Records**: Skip with warning messages
- **Duplicate Keys**: First occurrence wins
