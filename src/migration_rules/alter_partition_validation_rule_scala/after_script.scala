// ALTER PARTITION Type Validation Migration - AFTER Script (Spark 3.4+)
// This script demonstrates the updated behavior with strict type validation.
//
// Language: Scala
// Usage: spark-shell -i after_script.scala
//
// This script can be run independently to test the updated behavior.

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.io.File
import java.nio.file.{Files, Paths}

object AlterPartitionValidationAfter {
  def main(args: Array[String]): Unit = {
    // Create SparkSession with Spark 3.4+ behavior (strict validation)
    val spark = SparkSession.builder()
      .appName("AlterPartitionValidationAfter")
      .master("local[*]")
      .config("spark.sql.legacy.skipTypeValidationOnAlterPartition", "false")
      .config("spark.sql.warehouse.dir", "spark-warehouse")
      .getOrCreate()

    import spark.implicits._

    println("=== ALTER PARTITION Type Validation - AFTER (Spark 3.4+ behavior) ===")
    println("Configuration: spark.sql.legacy.skipTypeValidationOnAlterPartition = false")

    // Create a temporary directory for our table
    val tempDir = Files.createTempDirectory("spark_partition_test").toString
    val tablePath = s"$tempDir/test_partition_table"

    try {
      // Create sample data with integer partition column
      println("\n📊 Creating sample partitioned table...")

      // Define schema with integer partition column
      val schema = StructType(Array(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("partition_col", IntegerType, true)  // Integer partition column
      ))

      // Create sample data
      val data = Seq(
        (1, "Alice", 100),
        (2, "Bob", 200),
        (3, "Charlie", 100),
        (4, "David", 200)
      )

      val df = data.toDF("id", "name", "partition_col")

      // Write as partitioned table
      df.write
        .mode("overwrite")
        .partitionBy("partition_col")
        .option("path", tablePath)
        .saveAsTable("test_partition_table")

      println("✅ Created partitioned table with integer partition column 'partition_col'")

      // Show current partitions
      println("\n📋 Current partitions:")
      spark.sql("SHOW PARTITIONS test_partition_table").show()

      // Demonstrate the NEW behavior - type validation prevents mismatches
      println("\n🔍 ATTEMPTING: ALTER TABLE ADD PARTITION with string value for int column...")
      println("   SQL: ALTER TABLE test_partition_table ADD PARTITION (partition_col='300')")

      try {
        // This will fail in Spark 3.4+ due to type validation
        spark.sql("ALTER TABLE test_partition_table ADD PARTITION (partition_col='300')")
        println("❌ UNEXPECTED: Partition was added (this shouldn't happen in Spark 3.4+)")

      } catch {
        case e: Exception =>
          println(s"✅ EXPECTED FAILURE: ${e.getMessage}")
          println("   🎉 Type validation prevented the type mismatch!")
          println("   💡 This protects data integrity")
      }

      // Demonstrate the CORRECT way - using proper types
      println("\n✅ CORRECT APPROACH: Adding partition with proper integer value...")
      println("   SQL: ALTER TABLE test_partition_table ADD PARTITION (partition_col=300)")

      try {
        spark.sql("ALTER TABLE test_partition_table ADD PARTITION (partition_col=300)")
        println("✅ SUCCESS: Partition added with correct integer value")

        // Show updated partitions
        println("\n📋 Partitions after adding correct partition:")
        spark.sql("SHOW PARTITIONS test_partition_table").show()

      } catch {
        case e: Exception =>
          println(s"❌ Unexpected error with correct type: ${e.getMessage}")
      }

      // Demonstrate another type validation scenario
      println("\n🔍 ATTEMPTING: Adding partition with completely invalid string...")
      println("   SQL: ALTER TABLE test_partition_table ADD PARTITION (partition_col='invalid_string')")

      try {
        spark.sql("ALTER TABLE test_partition_table ADD PARTITION (partition_col='invalid_string')")
        println("❌ UNEXPECTED: Invalid partition was added")

      } catch {
        case e: Exception =>
          println(s"✅ EXPECTED FAILURE: ${e.getMessage}")
          println("   🎉 Type validation prevented invalid data!")
      }

      // Show how to handle convertible strings programmatically
      println("\n💡 BEST PRACTICE: Programmatic partition addition with proper types...")

      try {
        val partitionValues = Seq(400, 500, 600)  // Integer values
        
        partitionValues.foreach { value =>
          try {
            val sql = s"ALTER TABLE test_partition_table ADD PARTITION (partition_col=$value)"
            println(s"   Attempting: $sql")
            spark.sql(sql)
            println(s"   ✅ Added partition with value $value")
          } catch {
            case e: Exception =>
              println(s"   ❌ Failed to add partition $value: ${e.getMessage}")
          }
        }

      } catch {
        case e: Exception =>
          println(s"❌ Error in programmatic addition: ${e.getMessage}")
      }

      // Demonstrate type-safe partition addition using DataFrame API
      println("\n🔧 ADVANCED: Type-safe partition addition using DataFrame API...")
      try {
        // Create new data for additional partitions
        val newData = Seq(
          (5, "Eve", 700),
          (6, "Frank", 800)
        ).toDF("id", "name", "partition_col")

        // Write new partitions using DataFrame API (type-safe)
        newData.write
          .mode("append")
          .partitionBy("partition_col")
          .insertInto("test_partition_table")

        println("✅ SUCCESS: Type-safe partition addition via DataFrame API")

      } catch {
        case e: Exception =>
          println(s"❌ Error with DataFrame API: ${e.getMessage}")
      }

      // Final partition list
      println("\n📊 Final partition list:")
      spark.sql("SHOW PARTITIONS test_partition_table").show()

      // Query all data to verify integrity
      println("\n🔍 Querying all data to verify integrity:")
      val result = spark.sql("SELECT * FROM test_partition_table ORDER BY id")
      result.show()

      // Demonstrate consistent query behavior
      println("\n✅ DEMONSTRATING: Consistent query behavior with proper types...")
      try {
        println("   Filtering by partition_col = 300:")
        val filterResult1 = spark.sql("SELECT * FROM test_partition_table WHERE partition_col = 300")
        filterResult1.show()

        println("   Filtering by partition_col IN (400, 500):")
        val filterResult2 = spark.sql("SELECT * FROM test_partition_table WHERE partition_col IN (400, 500)")
        filterResult2.show()

        println("✅ All queries work consistently with proper type handling!")

      } catch {
        case e: Exception =>
          println(s"❌ Query error: ${e.getMessage}")
      }

      println("\n🎯 MIGRATION RECOMMENDATIONS:")
      println("   ✅ Always use correct data types in partition specs")
      println("   ✅ Validate partition values before ALTER PARTITION commands")
      println("   ✅ Use integer literals for integer partition columns")
      println("   ✅ Use DataFrame API for type-safe partition operations")
      println("   ✅ Test partition operations in development environment")
      println("   ⚠️  If legacy behavior is absolutely needed, set:")
      println("      spark.sql.legacy.skipTypeValidationOnAlterPartition=true")

    } catch {
      case e: Exception =>
        println(s"❌ Error during demonstration: ${e.getMessage}")
        println("   Check your Spark configuration and table setup")
    } finally {
      // Cleanup
      try {
        spark.sql("DROP TABLE IF EXISTS test_partition_table")
        // Clean up temporary directory
        val tempDirFile = new File(tempDir)
        if (tempDirFile.exists()) {
          def deleteRecursively(file: File): Unit = {
            if (file.isDirectory) {
              file.listFiles().foreach(deleteRecursively)
            }
            file.delete()
          }
          deleteRecursively(tempDirFile)
        }
      } catch {
        case _: Exception => // Ignore cleanup errors
      }

      spark.stop()
      println("\nSpark session stopped.")

      println("\n🎉 SUMMARY - Spark 3.4+ Benefits:")
      println("   • Strict type validation prevents data integrity issues")
      println("   • Early error detection for type mismatches")
      println("   • Consistent behavior with spark.sql.storeAssignmentPolicy")
      println("   • Better data quality and reliability")
      println("   • Improved developer experience with clear error messages")
    }
  }
}

// If running in spark-shell, call the main method
AlterPartitionValidationAfter.main(Array())