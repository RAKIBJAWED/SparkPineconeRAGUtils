# Cast Alias Migration Rule

## Overview
This migration rule addresses the change in Spark 3.2 where auto-generated CAST expressions are stripped from column alias names, resulting in cleaner and more predictable column naming.

## Rule Details
- **Rule ID**: cast_alias_migration_002
- **Source Version**: Spark 3.1 and earlier
- **Target Version**: Spark 3.2+
- **Severity**: Medium
- **Category**: Column Naming

## Problem Description
In Spark 3.1 and earlier, when type coercion rules automatically added CAST expressions, these would appear in the generated column alias names. For example:
- `sql("SELECT floor(1)").columns` would return `["FLOOR(CAST(1 AS DOUBLE))"]`

This behavior caused:
- Inconsistent column naming
- Longer, more complex column names
- Potential issues with downstream applications expecting specific names
- Difficulties in schema evolution

## Solution (Spark 3.2+)
Starting from Spark 3.2, auto-generated CAST expressions are stripped from column alias names:
- `sql("SELECT floor(1)").columns` now returns `["FLOOR(1)"]`

## Benefits
- **Cleaner Names**: Column aliases are more readable and concise
- **Consistency**: Predictable naming patterns across different operations
- **Compatibility**: Better integration with external tools and applications
- **Maintainability**: Easier to work with column names in code

## Testing
Run the before and after scripts to see the difference:

```bash
# Test old behavior (conceptually - actual behavior depends on Spark version)
python before_script.py

# Test new behavior
python after_script.py
```

## Impact Assessment
- **Breaking Change**: Potentially yes, if applications rely on specific column names
- **Mitigation**: Update code that depends on column names with CAST expressions
- **Recommendation**: Use explicit column aliases when specific names are required

## Examples

### Before (Spark 3.1)
```python
df = spark.sql("SELECT floor(1), ceil(2.5)")
print(df.columns)  # ['FLOOR(CAST(1 AS DOUBLE))', 'CEIL(2.5)']
```

### After (Spark 3.2+)
```python
df = spark.sql("SELECT floor(1), ceil(2.5)")
print(df.columns)  # ['FLOOR(1)', 'CEIL(2.5)']
```

## Related Functions
This change affects functions that commonly trigger type coercion:
- Mathematical functions: `floor()`, `ceil()`, `round()`, `abs()`
- Comparison operations with mixed types
- Aggregate functions with type coercion
- String to numeric conversions

## Documentation
- [Spark SQL Migration Guide](https://spark.apache.org/docs/latest/sql-migration-guide.html#upgrading-from-spark-sql-31-to-32)