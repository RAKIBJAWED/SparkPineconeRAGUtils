# Cast Auto-Generation Column Alias Migration (Scala)

## Overview
This migration rule addresses the change in Spark 3.2 where auto-generated CAST expressions are stripped from column alias names, resulting in cleaner and more consistent column naming.

## Rule Details
- **Rule ID**: cast_alias_migration_scala_005
- **Language**: Scala
- **Source Version**: Spark 3.1 and earlier
- **Target Version**: Spark 3.2+
- **Severity**: Medium
- **Category**: Column Naming

## Problem Description
In Spark 3.1 and earlier, when Spark automatically added CAST expressions for type coercion, these CAST expressions would appear in the generated column names, making them verbose and inconsistent.

### Example of Problematic Behavior (Spark 3.1)
```scala
// This might generate column name like "FLOOR(CAST(1 AS DOUBLE))"
spark.sql("SELECT floor(1)").columns
// Array("FLOOR(CAST(1 AS DOUBLE))")

// Inconsistent with explicit double
spark.sql("SELECT floor(1.0)").columns  
// Array("FLOOR(1.0)")
```

## Solution
Spark 3.2+ strips auto-generated CAST expressions from column names, providing cleaner and more consistent naming.

### Updated Behavior (Spark 3.2+)
```scala
// Clean column names in Spark 3.2+
spark.sql("SELECT floor(1)").columns
// Array("FLOOR(1)")

spark.sql("SELECT floor(1.0)").columns
// Array("FLOOR(1.0)")

// Both are now consistent and clean
```

## Migration Impact

### 1. Column Name Changes
Your existing code that relies on specific column names may need updates:

```scala
// Before (Spark 3.1) - might have worked with verbose names
val df = spark.sql("SELECT floor(1)")
val oldColumnName = "FLOOR(CAST(1 AS DOUBLE))"  // Verbose name
df.select(col(oldColumnName))

// After (Spark 3.2+) - use clean names
val newColumnName = "FLOOR(1)"  // Clean name
df.select(col(newColumnName))
```

### 2. Programmatic Column Handling
```scala
// More predictable column name handling
val df = spark.sql("SELECT floor(1), ceil(2), round(3)")
val columns = df.columns  // Now consistently clean

// Easier pattern matching and processing
columns.foreach { colName =>
  if (colName.startsWith("FLOOR(")) {
    // Process floor operations - cleaner logic
  }
}
```

### 3. External Tool Integration
```scala
// Better for exports and external tools
val df = spark.sql("""
  SELECT 
    floor(value) as floor_result,
    ceil(value) as ceil_result
  FROM my_table
""")

// Clean column names in exported files
df.write.option("header", "true").csv("output.csv")
// Headers: floor_result, ceil_result (clean and readable)
```

## Testing

### Test the Migration
```bash
# Test old behavior (may show verbose names in older Spark)
spark-shell -i src/migration_rules/cast_alias_rule_scala/before_script.scala

# Test new behavior (clean names)
spark-shell -i src/migration_rules/cast_alias_rule_scala/after_script.scala
```

### Validate Your Code
```scala
// Test column name generation
val testDF = spark.sql("SELECT floor(1), ceil(2.5)")
println("Column names: " + testDF.columns.mkString(", "))

// Verify clean names (should not contain "CAST")
testDF.columns.foreach { colName =>
  assert(!colName.contains("CAST"), s"Column name should not contain CAST: $colName")
}
```

## Best Practices

### 1. Explicit Column Aliases
```scala
// Always use explicit aliases for important columns
val df = spark.sql("""
  SELECT 
    floor(value) AS floor_value,
    ceil(value) AS ceil_value,
    round(value, 2) AS rounded_value
  FROM my_table
""")
```

### 2. Programmatic Column Handling
```scala
// Use DataFrame API for predictable column names
import org.apache.spark.sql.functions._

val df = sourceDF.select(
  col("id"),
  floor(col("value")).alias("floor_value"),
  ceil(col("value")).alias("ceil_value")
)
```

### 3. Column Name Validation
```scala
// Validate expected column names in tests
def validateColumnNames(df: DataFrame, expectedNames: Seq[String]): Unit = {
  val actualNames = df.columns.toSeq
  assert(actualNames == expectedNames, 
    s"Expected: $expectedNames, Actual: $actualNames")
}
```

## Common Scenarios

### Scenario 1: Mathematical Functions
```scala
// Functions that commonly trigger auto-casting
val mathDF = spark.sql("""
  SELECT 
    floor(1) as floor_int,      -- Clean: "floor_int"
    ceil(2) as ceil_int,        -- Clean: "ceil_int"  
    round(3) as round_int,      -- Clean: "round_int"
    sqrt(4) as sqrt_int         -- Clean: "sqrt_int"
""")
```

### Scenario 2: Aggregation Functions
```scala
// Aggregations with type coercion
val aggDF = spark.sql("""
  SELECT 
    sum(1) as sum_literal,      -- Clean column name
    avg(1) as avg_literal,      -- Clean column name
    max(1) as max_literal       -- Clean column name
  FROM my_table
""")
```

### Scenario 3: Complex Expressions
```scala
// Complex expressions with multiple operations
val complexDF = spark.sql("""
  SELECT 
    floor(value * 1.5) as scaled_floor,
    ceil(value / 2) as half_ceil
  FROM my_table
""")
```

## Troubleshooting

### Issue: Code Breaks After Upgrade
If your code breaks after upgrading to Spark 3.2+:

1. **Check column references**:
```scala
// Old code might have used verbose names
val oldName = "FLOOR(CAST(value AS DOUBLE))"
// Update to clean name
val newName = "FLOOR(value)"
```

2. **Update column selection logic**:
```scala
// Instead of hardcoded column names, use aliases
val df = spark.sql("SELECT floor(value) AS clean_floor FROM table")
df.select("clean_floor")  // Predictable name
```

3. **Verify external integrations**:
```scala
// Check if external tools expect specific column names
// Update configurations to use new clean names
```

### Issue: Inconsistent Column Names
If you see inconsistent behavior:

1. **Check Spark version**: Ensure you're running Spark 3.2+
2. **Use explicit aliases**: Don't rely on auto-generated names
3. **Test with simple cases**: Verify the behavior with basic functions

## Benefits of the Change

### 1. Improved Readability
- Column names are concise and meaningful
- Easier to understand query results
- Better for documentation and reporting

### 2. Better Tool Integration
- External tools get clean column names
- CSV/JSON exports have readable headers
- Database integrations work more smoothly

### 3. Consistent Behavior
- Similar operations produce similar column names
- Predictable naming patterns
- Easier programmatic handling

### 4. Reduced Verbosity
- Shorter column names save space
- Less cluttered query results
- Improved performance in some cases

## References
- [Spark SQL Migration Guide](https://spark.apache.org/docs/latest/sql-migration-guide.html#upgrading-from-spark-sql-31-to-32)
- [Spark SQL Functions](https://spark.apache.org/docs/latest/api/sql/)
- [DataFrame Column Operations](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html)