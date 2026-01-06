# Format String Argument Index Migration Rule

## Overview
This migration rule addresses the breaking change in Apache Spark 3.3+ where the `format_string()` and `printf()` functions no longer support `0$` indexing for the first argument.

## Rule Details
- **Rule ID**: `format_string_migration_001`
- **Source Version**: Spark 3.3.0 and earlier
- **Target Version**: Spark 3.4.0+
- **Severity**: High
- **Category**: String Formatting

## Problem Description
Since Spark 3.3, the argument indexing in `format_string()` and `printf()` functions has changed:
- **Before**: First argument could be referenced with `%0$`
- **After**: First argument must be referenced with `%1$`

## Files Structure
```
format_string_rule/
├── README.md              # This documentation
├── rule_config.json       # Rule configuration and metadata
├── before_script.py       # Script showing deprecated 0$ indexing
└── after_script.py        # Script showing correct 1$ indexing
```

## Running the Scripts

### Before Script (Deprecated Behavior)
```bash
cd src/migration_rules/format_string_rule
python before_script.py
```
**Expected Result**: May show errors in Spark 3.3+ due to deprecated 0$ indexing

### After Script (Correct Behavior)
```bash
cd src/migration_rules/format_string_rule
python after_script.py
```
**Expected Result**: Runs successfully with proper 1$-based indexing

## Migration Examples

### Before (Deprecated)
```python
# This will fail in Spark 3.3+
df.select(expr("format_string('Hello %0$s, you have %0$d messages', name, count)"))
df.select(expr("printf('User %0$s has %0$s status', name, status)"))
```

### After (Correct)
```python
# This works in Spark 3.3+
df.select(expr("format_string('Hello %1$s, you have %2$d messages', name, count)"))
df.select(expr("printf('User %1$s has %2$s status', name, status)"))
```

## Key Changes
1. **Argument Indexing**: Change from `%0$` to `%1$` for first argument
2. **Sequential Numbering**: All arguments now use 1-based indexing (`%1$`, `%2$`, `%3$`, etc.)
3. **Argument Reuse**: Can still reuse arguments with proper indexing (`%1$s ... %1$s`)

## Error Messages
Common errors when using deprecated 0$ indexing:
- `ArgumentIndexOutOfBoundsException`
- `IllegalArgumentException: argument index out of range`
- Parsing errors in SQL expressions

## Documentation
- [Spark SQL Migration Guide](https://spark.apache.org/docs/latest/sql-migration-guide.html#upgrading-from-spark-sql-33-to-34)
- [Format String Function Documentation](https://spark.apache.org/docs/latest/api/sql/index.html#format_string)

## Testing
Both scripts include comprehensive test cases covering:
- Basic format string operations
- Complex multi-argument formatting
- Argument reuse scenarios
- Nested format string operations
- Mixed data type formatting