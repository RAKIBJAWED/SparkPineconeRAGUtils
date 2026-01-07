# Format String Argument Index Migration (Scala)

## Overview
This migration rule addresses the change in Spark 3.3 where `format_string()` and `printf()` functions no longer support 0$-based argument indexing and require 1$-based indexing instead.

## Rule Details
- **Rule ID**: format_string_migration_scala_004
- **Language**: Scala
- **Source Version**: Spark 3.2 and earlier
- **Target Version**: Spark 3.3+
- **Severity**: High
- **Category**: String Formatting

## Problem Description
In Spark 3.2 and earlier, the `format_string()` and `printf()` functions allowed using `%0$` to reference the first argument. This was non-standard compared to most printf implementations and could cause confusion.

### Example of Problematic Code (Spark 3.2)
```scala
// This would work in Spark 3.2 but fails in Spark 3.3+
df.select(expr("format_string('Name: %0$s, Age: %1$d', name, age)"))
df.select(expr("printf('Salary: %0$.2f', salary)"))
```

## Solution
Spark 3.3+ requires 1$-based indexing for argument references in format strings.

### Updated Code (Spark 3.3+)
```scala
// Correct approach - use 1$-based indexing
df.select(expr("format_string('Name: %1$s, Age: %2$d', name, age)"))
df.select(expr("printf('Salary: %1$.2f', salary)"))

// Alternative - use simple format without explicit indexing
df.select(expr("format_string('Name: %s, Age: %d', name, age)"))
```

## Migration Steps

### 1. Identify Format String Usage
Search your Scala codebase for:
```scala
// Direct SQL expressions
expr("format_string(")
expr("printf(")

// String literals with 0$ indexing
"%0$"
```

### 2. Update Argument Indexing
```scala
// Before (Spark 3.2)
expr("format_string('Employee: %0$s works in %1$s', name, department)")

// After (Spark 3.3+)
expr("format_string('Employee: %1$s works in %2$s', name, department)")
```

### 3. Handle Complex Format Strings
```scala
// Before - complex with repeated 0$ references
expr("format_string('%0$s (%1$d years) - %0$s is experienced', name, age)")

// After - update all references
expr("format_string('%1$s (%2$d years) - %1$s is experienced', name, age)")
```

### 4. Consider Alternatives
```scala
// Alternative 1: Simple format without indexing
expr("format_string('Name: %s, Age: %d', name, age)")

// Alternative 2: Use concat_ws for simple cases
import org.apache.spark.sql.functions._
df.select(concat_ws(" - ", lit("Name:"), col("name"), lit("Age:"), col("age")))

// Alternative 3: String interpolation in Scala code
df.select(lit(s"Static text with ${someValue}"))
```

## Testing

### Test the Migration
```bash
# Test old behavior (should show errors in Spark 3.3+)
spark-shell -i src/migration_rules/format_string_rule_scala/before_script.scala

# Test new behavior (correct approach)
spark-shell -i src/migration_rules/format_string_rule_scala/after_script.scala
```

### Validate Your Code
```scala
// Test your format strings
val testData = Seq(("Alice", 25)).toDF("name", "age")

// This should fail in Spark 3.3+
testData.select(expr("format_string('Name: %0$s', name)")).show()

// This should work
testData.select(expr("format_string('Name: %1$s', name)")).show()
```

## Best Practices

### 1. Consistent Indexing
```scala
// Good - clear 1$-based indexing
expr("format_string('User %1$s (ID: %2$d) has %3$d points', name, id, points)")

// Better - simple format when order matches
expr("format_string('User %s (ID: %d) has %d points', name, id, points)")
```

### 2. Complex Formatting
```scala
// For complex formatting needs
val formatStr = "Report: %1$s generated on %2$tF at %2$tT for user %3$s"
df.select(expr(s"format_string('$formatStr', report_name, current_timestamp(), user_name)"))
```

### 3. Performance Considerations
```scala
// For simple concatenation, concat_ws might be faster
concat_ws(" | ", col("field1"), col("field2"), col("field3"))

// vs format_string
expr("format_string('%1$s | %2$s | %3$s', field1, field2, field3)")
```

## Common Migration Patterns

### Pattern 1: Simple 0$ to 1$ Replacement
```scala
// Before
expr("format_string('Value: %0$s', column)")
// After  
expr("format_string('Value: %1$s', column)")
```

### Pattern 2: Multiple Arguments
```scala
// Before
expr("format_string('%0$s: %1$d (%2$.2f%%)', name, count, percentage)")
// After
expr("format_string('%1$s: %2$d (%3$.2f%%)', name, count, percentage)")
```

### Pattern 3: Repeated References
```scala
// Before
expr("format_string('%0$s works for %1$s. %0$s is happy.', employee, company)")
// After
expr("format_string('%1$s works for %2$s. %1$s is happy.', employee, company)")
```

## Error Messages
When using 0$ indexing in Spark 3.3+, you'll see:
```
[INVALID_PARAMETER_VALUE.ZERO_INDEX] The value of parameter(s) `strfmt` in `format_string` is invalid: expects %1$, %2$ and so on, but got %0$
```

## Related Functions
This migration also affects:
- `printf()` function
- Any custom UDFs that use similar format string patterns

## References
- [Spark SQL Migration Guide](https://spark.apache.org/docs/latest/sql-migration-guide.html#upgrading-from-spark-sql-33-to-34)
- [Spark SQL Functions](https://spark.apache.org/docs/latest/api/sql/index.html#format_string)
- [Java Printf Format Strings](https://docs.oracle.com/javase/8/docs/api/java/util/Formatter.html)