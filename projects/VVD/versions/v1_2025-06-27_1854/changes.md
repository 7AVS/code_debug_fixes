# Changes - v1
Date: 2025-06-27_1854

## Files Modified:
- Created `vvd_campaign_pipeline.py` - Optimized PySpark pipeline
- Created `vvd_campaign_analysis.sql` - SQL version for database execution

## Key Improvements:

### 1. Performance Optimizations
- Added Spark adaptive query execution configurations
- Implemented proper data caching for the 27M record dataset
- Optimized shuffle partitions (400) for better parallelism
- Added schema definition for faster data loading

### 2. Contact Frequency Analysis
- Implemented rolling window calculations (30, 60, 90 days)
- Added proper window functions for accurate frequency counting
- Excluded current contact from frequency counts

### 3. Campaign Performance Metrics
- Added comprehensive performance calculations:
  - Response rates
  - Conversion rates
  - Response-to-conversion rates
  - Revenue metrics (total, average, per client)
  - Time-to-action metrics

### 4. Statistical Analysis
- Added standard deviation calculations for revenue
- Implemented confidence levels based on sample size
- Added minimum thresholds for statistical significance

### 5. Client Segmentation
- Created engagement scoring algorithm (weighted composite)
- Defined client segments based on behavior and value
- Added lifetime value calculations

### 6. Optimal Frequency Detection
- Analyzed performance by contact frequency per channel
- Calculated lift vs baseline (0 contacts)
- Ranked frequencies by revenue performance

### 7. Time Series Analysis
- Monthly trend analysis with MoM growth calculations
- Channel-specific performance tracking

### 8. Output Management
- Organized outputs into logical categories
- Added executive summary generation
- Implemented proper data partitioning for output files

## Technical Improvements:
- Error handling for null values
- Proper date range filtering
- Optimized joins and aggregations
- Added data quality checks

## Business Impact:
- Enables analysis of 27M deployments efficiently
- Identifies optimal contact frequency by channel
- Segments 4M clients by engagement level
- Provides actionable insights for campaign optimization
