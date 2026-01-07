# ALTER PARTITION Type Validation Migration (Scala)

## Overview
This migration rule addresses the change in Spark 3.4 where ALTER PARTITION commands now validate partition specification types against the actual column types, following the behavior of `spark.sql.storeAssignmentPolicy`.

## Rule Details
- **Rule ID**: alter_partition_validation_scala_006
- **Language**: Scala
- **Source Version**: Spark 3.3 and earlier
- **Target Version**: Spark 3.4+
- **Severity**: High
- **Category**: Partition Management

## Problem Description
In Spark 3.3 and earlier, ALTER PARTITION commands did not validate that partition specification values matched the data types of the partition columns. This could lead to type inconsistencies and data integrity issues.

### Example of Problematic Code (Spark 3.3)
```scala
// This would work in Spark 3.3 even though partition_col is INTEGER
spark.sql("ALTER TABLE my_table ADD PARTITION (partition_col='string_value')")

// Programmatic approach that could cause issues
val partitionValue = "123"  // String value
spark.sql(s"ALTER TABLE my_table ADD PARTITION (partition_col='$partitionValue')")
```

## Solution
Spark 3.4+ introduces strict type validation for partition specifications, ensuring data integrity and consistency.

### Updated Code (Spark 3.4+)
```scala
// Correct approach - use proper data types
spark.sql("ALTER TABLE my_table ADD PARTITION (partition_col=123)")

// Type-safe programmatic approach
val partitionValue: Int = 123
spark.sql(s"ALTER TABLE my_table ADD PARTITION (partition_col=$partitionValue)")

// Using DataFrame API for type safety
newData.write
  .mode("append")
  .partitionBy("partition_col")
  .insertInto("my_table")
```

## Migration Steps

### 1. Audit Existing Partition Operations
Review your Scala codebase for ALTER PARTITION commands:

```scala
// Search for these patterns
spark.sql("ALTER TABLE .* ADD PARTITION")
spark.sql(s"ALTER TABLE $tableName ADD PARTITION")

// Check string interpolation with partition values
s"ALTER TABLE $table ADD PARTITION ($col='$value')"
```

### 2. Update Partition Specifications
```scala
// Before (Spark 3.3) - risky string values
val year = "2023"
val month = "12"
spark.sql(s"ALTER TABLE sales ADD PARTITION (year='$year', month='$month')")

// After (Spark 3.4+) - proper types
val year: Int = 2023
val month: Int = 12
spark.sql(s"ALTER TABLE sales ADD PARTITION (year=$year, month=$month)")
```

### 3. Handle Dynamic Partition Addition
```scala
// Before - unsafe string formatting
def addPartition(table: String, partitionCol: String, value: String): Unit = {
  spark.sql(s"ALTER TABLE $table ADD PARTITION ($partitionCol='$value')")
}

// After - type-safe approach
def addPartition(table: String, partitionCol: String, value: Int): Unit = {
  spark.sql(s"ALTER TABLE $table ADD PARTITION ($partitionCol=$value)")
}

// Or with validation
def addPartitionSafe(table: String, partitionCol: String, value: String): Unit = {
  val intValue = value.toInt  // Validates conversion
  spark.sql(s"ALTER TABLE $table ADD PARTITION ($partitionCol=$intValue)")
}
```

### 4. Use DataFrame API for Type Safety
```scala
// Type-safe partition addition using DataFrame API
def addPartitionWithData(df: DataFrame, tableName: String): Unit = {
  df.write
    .mode("append")
    .partitionBy("partition_col")
    .insertInto(tableName)
}

// Create partitions by writing empty DataFrames with correct schema
def createEmptyPartition(spark: SparkSession, tableName: String, partitionValue: Int): Unit = {
  import spark.implicits._
  
  val emptyDF = Seq.empty[(Int, String, Int)].toDF("id", "name", "partition_col")
    .filter($"partition_col" === partitionValue)
  
  emptyDF.write
    .mode("append")
    .partitionBy("partition_col")
    .insertInto(tableName)
}
```

## Testing

### Test the Migration
```bash
# Test old behavior (with legacy flag)
spark-shell -i src/migration_rules/alter_partition_validation_rule_scala/before_script.scala

# Test new behavior (strict validation)
spark-shell -i src/migration_rules/alter_partition_validation_rule_scala/after_script.scala
```

### Validate Your Code
```scala
// Test partition operations
val testTable = "test_partitions"

// This should fail in Spark 3.4+
try {
  spark.sql(s"ALTER TABLE $testTable ADD PARTITION (int_col='string_value')")
  println("ERROR: Type mismatch was allowed!")
} catch {
  case e: Exception => println(s"GOOD: Type validation working: ${e.getMessage}")
}

// This should work
spark.sql(s"ALTER TABLE $testTable ADD PARTITION (int_col=123)")
```

## Best Practices

### 1. Type Safety in Partition Operations
```scala
// Use strongly typed values
case class PartitionSpec(year: Int, month: Int, day: Int)

def addDatePartition(table: String, spec: PartitionSpec): Unit = {
  spark.sql(s"""
    ALTER TABLE $table ADD PARTITION (
      year=${spec.year}, 
      month=${spec.month}, 
      day=${spec.day}
    )
  """)
}
```

### 2. Validation Functions
```scala
// Validate partition values before use
def validateAndAddPartition(table: String, col: String, value: String): Unit = {
  val tableSchema = spark.table(table).schema
  val partitionField = tableSchema.find(_.name == col)
    .getOrElse(throw new IllegalArgumentException(s"Column $col not found"))
  
  partitionField.dataType match {
    case IntegerType => 
      val intValue = value.toInt
      spark.sql(s"ALTER TABLE $table ADD PARTITION ($col=$intValue)")
    case StringType => 
      spark.sql(s"ALTER TABLE $table ADD PARTITION ($col='$value')")
    case _ => 
      throw new IllegalArgumentException(s"Unsupported partition type: ${partitionField.dataType}")
  }
}
```

### 3. Configuration Management
```scala
// Handle legacy behavior when needed
class PartitionManager(spark: SparkSession) {
  def addPartitionWithLegacySupport(table: String, spec: String): Unit = {
    try {
      // Try with strict validation first
      spark.sql(s"ALTER TABLE $table ADD PARTITION $spec")
    } catch {
      case e: Exception if e.getMessage.contains("type conversion") =>
        // Temporarily enable legacy mode if absolutely necessary
        spark.conf.set("spark.sql.legacy.skipTypeValidationOnAlterPartition", "true")
        try {
          spark.sql(s"ALTER TABLE $table ADD PARTITION $spec")
          println("WARNING: Used legacy mode for partition addition")
        } finally {
          spark.conf.set("spark.sql.legacy.skipTypeValidationOnAlterPartition", "false")
        }
    }
  }
}
```

## Common Scenarios

### Scenario 1: Date-based Partitions
```scala
// Before - string-based dates
val dateStr = "2023-12-01"
spark.sql(s"ALTER TABLE events ADD PARTITION (event_date='$dateStr')")

// After - proper date handling
import java.sql.Date
val eventDate = Date.valueOf("2023-12-01")
spark.sql(s"ALTER TABLE events ADD PARTITION (event_date='$eventDate')")

// Or use date literals
spark.sql("ALTER TABLE events ADD PARTITION (event_date=DATE '2023-12-01')")
```

### Scenario 2: Numeric Partitions
```scala
// Before - string numbers
val yearStr = "2023"
spark.sql(s"ALTER TABLE metrics ADD PARTITION (year='$yearStr')")

// After - proper integers
val year: Int = 2023
spark.sql(s"ALTER TABLE metrics ADD PARTITION (year=$year)")
```

### Scenario 3: Mixed Type Partitions
```scala
// Before - inconsistent types
spark.sql("ALTER TABLE logs ADD PARTITION (level='INFO', code='404')")

// After - correct types for each column
spark.sql("ALTER TABLE logs ADD PARTITION (level='INFO', code=404)")
```

## Error Handling

### Common Errors and Solutions
```scala
// Handle type conversion errors gracefully
def safeAddPartition(table: String, col: String, value: String): Either[String, Unit] = {
  try {
    // Attempt to determine correct type and convert
    val tableSchema = spark.table(table).schema
    val field = tableSchema.find(_.name == col)
    
    field match {
      case Some(f) if f.dataType == IntegerType =>
        val intVal = value.toInt
        spark.sql(s"ALTER TABLE $table ADD PARTITION ($col=$intVal)")
        Right(())
      case Some(f) if f.dataType == StringType =>
        spark.sql(s"ALTER TABLE $table ADD PARTITION ($col='$value')")
        Right(())
      case _ =>
        Left(s"Unsupported or unknown column type for $col")
    }
  } catch {
    case e: NumberFormatException =>
      Left(s"Cannot convert '$value' to required numeric type")
    case e: Exception =>
      Left(s"Error adding partition: ${e.getMessage}")
  }
}
```

## Performance Considerations

### 1. Batch Partition Operations
```scala
// Instead of multiple ALTER TABLE commands
def addMultiplePartitions(table: String, partitions: Seq[Int]): Unit = {
  partitions.foreach { partition =>
    spark.sql(s"ALTER TABLE $table ADD PARTITION (partition_col=$partition)")
  }
}

// Consider using DataFrame API for bulk operations
def addPartitionsViaBulkInsert(table: String, partitionData: DataFrame): Unit = {
  partitionData.write
    .mode("append")
    .partitionBy("partition_col")
    .insertInto(table)
}
```

### 2. Validation Caching
```scala
// Cache table schema for repeated operations
class CachedPartitionManager(spark: SparkSession) {
  private val schemaCache = scala.collection.mutable.Map[String, StructType]()
  
  def getTableSchema(table: String): StructType = {
    schemaCache.getOrElseUpdate(table, spark.table(table).schema)
  }
}
```

## References
- [Spark SQL Migration Guide](https://spark.apache.org/docs/latest/sql-migration-guide.html#upgrading-from-spark-sql-33-to-34)
- [Spark SQL Configuration](https://spark.apache.org/docs/latest/configuration.html#spark-sql)
- [ALTER TABLE Documentation](https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-alter-table.html)
- [Scala DataFrame API](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html)