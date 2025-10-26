# Roaring MSISDN Extension - Bitmap Aggregate Functionality

This extension provides efficient bitmap-based storage and analysis of MSISDN (Mobile Station International Subscriber Directory Number) data using Roaring bitmaps.

## Features

### Core Functions

1. **`msisdn_normalize(text) -> BIGINT`** - Normalizes phone numbers to integers
2. **`bitmap_agg(BIGINT) -> BLOB`** - Aggregate function to create bitmaps from MSISDNs
3. **`bitmap_cardinality(BLOB) -> BIGINT`** - Get the number of unique values in a bitmap
4. **`bitmap_has(BLOB, BIGINT) -> BOOLEAN`** - Check if a value exists in a bitmap

### Analysis Functions

5. **`bitmap_intersection(BLOB, BLOB) -> BLOB`** - Find common values between two bitmaps
6. **`bitmap_union(BLOB, BLOB) -> BLOB`** - Combine values from two bitmaps
7. **`bitmap_difference(BLOB, BLOB) -> BLOB`** - Find values in first bitmap but not in second

## Usage Examples

### Basic Aggregation

```sql
-- Get unique MSISDNs per region
SELECT 
    region,
    bitmap_cardinality(bitmap_agg(msisdn_normalize(msisdn))) as unique_count
FROM msisdn_data 
WHERE msisdn_normalize(msisdn) > 0
GROUP BY region;
```

### Set Operations

```sql
-- Find common MSISDNs between two groups
WITH group1 AS (
    SELECT bitmap_agg(msisdn_normalize(msisdn)) as bitmap
    FROM msisdn_data WHERE category = 'A'
),
group2 AS (
    SELECT bitmap_agg(msisdn_normalize(msisdn)) as bitmap
    FROM msisdn_data WHERE category = 'B'
)
SELECT bitmap_cardinality(bitmap_intersection(g1.bitmap, g2.bitmap)) as common_count
FROM group1 g1, group2 g2;
```

### Existence Checks

```sql
-- Check if specific MSISDNs exist
WITH all_msisdns AS (
    SELECT bitmap_agg(msisdn_normalize(msisdn)) as bitmap
    FROM msisdn_data
)
SELECT 
    bitmap_has(bitmap, msisdn_normalize('+1234567890')) as has_number
FROM all_msisdns;
```

## Performance Benefits

1. **Memory Efficiency**: Roaring bitmaps compress sparse data efficiently
2. **Fast Operations**: Set operations (union, intersection, difference) are very fast
3. **Scalability**: Handles millions of MSISDNs with minimal memory usage
4. **Deduplication**: Automatically handles duplicate values

## Use Cases

- **Telecom Analytics**: Analyze subscriber bases across regions/time periods
- **Fraud Detection**: Find overlapping MSISDNs between different datasets
- **Marketing**: Identify unique customers across campaigns
- **Network Analysis**: Track MSISDN usage patterns
- **Compliance**: Ensure data privacy by working with anonymized identifiers

## Technical Details

- Uses CRoaring library for 64-bit bitmap operations
- Portable serialization format for cross-platform compatibility
- Thread-safe aggregate operations
- Optimized for sparse integer datasets

## Installation

```sql
LOAD 'roaring_msisdn';
```

## Example Dataset

```sql
CREATE TABLE msisdn_data (
    user_id INTEGER,
    msisdn VARCHAR,
    region VARCHAR,
    timestamp TIMESTAMP
);

INSERT INTO msisdn_data VALUES 
    (1, '+1234567890', 'US', '2024-01-01 10:00:00'),
    (2, '+1 (234) 567-890', 'US', '2024-01-01 11:00:00'),
    (3, '+9876543210', 'EU', '2024-01-01 12:00:00');
```

See `example_usage.sql` for comprehensive examples.
