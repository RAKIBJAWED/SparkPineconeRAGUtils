# ALTER PARTITION Type Validation Migration

## Overview
This migration rule addresses the change in Spark 3.4 where ALTER PARTITION commands now validate partition specification types against the actual column types, following the behavior of `spark.sql.storeAssignmentPolicy`.

## Rule Details
- **Rule ID**: alter_partition_validation_migration_003
- **Language**: Python
- **Source Version**: Spark 3.3 and earlier
- **Target Version**: Spark 3.4+
- **Severity**: High
- **Category**: Partition Management

## Problem Description
In Spark 3.3 and earlier, ALTER PARTITION commands did not validate that partition specification values matched the data types of the partition columns. This could lead to:

- Type inconsistencies in partition metadata
- Data integrity issues
- Unexpected query behavior
- Runtime errors during data access

### Example of Problematic Code (Spark 3.3)
```sql
-- This would work in Spark 3.3 even though partition_col is INTEGER
ALTER TABLE my_table ADD PARTITION (partition_col='string_value')
```

## Solution
Spark 3.4+ introduces strict type validation for partition specifications:

- Partition values must match the column data types
- Type conversion follows `spark.sql.storeAssignmentPolicy` rules
- Invalid type conversions result in immediate errors
- Better data integrity and consistency

### Updated Code (Spark 3.4+)
```sql
-- Correct approach - use proper data types
ALTER TABLE my_table ADD PARTITION (partition_col=123)  -- Integer value for integer column

-- If you need legacy behavior (not recommended)
SET spark.sql.legacy.skipTypeValidationOnAlterPartition=true;
```

## Migration Steps

### 1. Audit Existing Partition Operations
Review your codebase for ALTER PARTITION commands and ensure:
- Partition values match column data types
- String values are only used for string partition columns
- Numeric values use proper numeric literals

### 2. Update Partition Specifications
```sql
-- Before (Spark 3.3)
ALTER TABLE sales ADD PARTITION (year='2023', month='12')  -- Strings for potentially numeric columns

-- After (Spark 3.4+)
ALTER TABLE sales ADD PARTITION (year=2023, month=12)     -- Proper numeric values
```

### 3. Handle Dynamic Partition Addition
```python
# Before - risky string formatting
partition_year = "2023"
spark.sql(f"ALTER TABLE sales ADD PARTITION (year='{partition_year}')")

# After - ensure proper types
partition_year = 2023  # Use integer
spark.sql(f"ALTER TABLE sales ADD PARTITION (year={partition_year})")
```

### 4. Configuration for Legacy Behavior (If Needed)
```python
# Only if absolutely necessary - not recommended for production
spark.conf.set("spark.sql.legacy.skipTypeValidationOnAlterPartition", "true")
```

## Testing

### Test the Migration
```bash
# Test old behavior (with legacy flag)
python src/migration_rules/alter_partition_validation_rule/before_script.py

# Test new behavior (strict validation)
python src/migration_rules/alter_partition_validation_rule/after_script.py
```

### Verify Your Tables
```sql
-- Check partition column types
DESCRIBE FORMATTED your_table_name;

-- Verify existing partitions
SHOW PARTITIONS your_table_name;

-- Test partition addition with correct types
ALTER TABLE your_table_name ADD PARTITION (your_partition_col=proper_value);
```

## Best Practices

### 1. Type Safety
- Always use correct data types in partition specifications
- Avoid string literals for numeric partition columns
- Use proper date/timestamp formats for temporal partitions

### 2. Validation
- Test partition operations in development environment
- Validate partition values before executing ALTER PARTITION
- Use schema validation tools where possible

### 3. Code Review
- Review all ALTER PARTITION statements during migration
- Check dynamic SQL generation for type consistency
- Ensure ETL pipelines use proper data types

### 4. Monitoring
- Monitor for partition-related errors after upgrade
- Set up alerts for failed ALTER PARTITION operations
- Log partition operations for debugging

## Common Scenarios

### Scenario 1: Numeric Partitions
```sql
-- Problem
ALTER TABLE metrics ADD PARTITION (year='2023')  -- String for numeric column

-- Solution
ALTER TABLE metrics ADD PARTITION (year=2023)   -- Numeric literal
```

### Scenario 2: Date Partitions
```sql
-- Problem
ALTER TABLE events ADD PARTITION (event_date='2023-12-01')  -- String for date column

-- Solution
ALTER TABLE events ADD PARTITION (event_date=DATE '2023-12-01')  -- Proper date literal
```

### Scenario 3: Mixed Type Partitions
```sql
-- Problem
ALTER TABLE logs ADD PARTITION (level='INFO', code='404')  -- Mixed types

-- Solution - ensure each matches its column type
ALTER TABLE logs ADD PARTITION (level='INFO', code=404)  -- String and numeric
```

## Troubleshooting

### Error: Type Conversion Failed
```
Error: Cannot safely cast 'string_value' to INTEGER
```
**Solution**: Use proper integer literals instead of strings.

### Error: Invalid Partition Specification
```
Error: Partition spec is invalid
```
**Solution**: Check that all partition columns are specified with correct types.

### Legacy Behavior Needed
If you absolutely need the old behavior:
```sql
SET spark.sql.legacy.skipTypeValidationOnAlterPartition=true;
```
**Warning**: This is not recommended for production use.

## Related Configuration

- `spark.sql.storeAssignmentPolicy`: Controls type coercion behavior
- `spark.sql.legacy.skipTypeValidationOnAlterPartition`: Disables validation (not recommended)

## References
- [Spark SQL Migration Guide](https://spark.apache.org/docs/latest/sql-migration-guide.html#upgrading-from-spark-sql-33-to-34)
- [Spark SQL Configuration](https://spark.apache.org/docs/latest/configuration.html#spark-sql)
- [ALTER TABLE Documentation](https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-alter-table.html)