// ALTER PARTITION Type Validation Migration - BEFORE Script (Spark 3.3 and earlier)
// This script demonstrates the old behavior where ALTER PARTITION allowed type mismatches.
//
// Language: Scala
// Usage: spark-shell -i before_script.scala
//
// This script can be run independently to test the old behavior.

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.io.File
import java.nio.file.{Files, Paths}

object AlterPartitionValidationBefore {
  def main(args: Array[String]): Unit = {
    // Create SparkSession with legacy behavior (simulating Spark 3.3)
    val spark = SparkSession.builder()
      .appName("AlterPartitionValidationBefore")
      .master("local[*]")
      .config("spark.sql.legacy.skipTypeValidationOnAlterPartition", "true")
      .config("spark.sql.warehouse.dir", "spark-warehouse")
      .getOrCreate()

    import spark.implicits._

    println("=== ALTER PARTITION Type Validation - BEFORE (Spark 3.3 behavior) ===")
    println("Configuration: spark.sql.legacy.skipTypeValidationOnAlterPartition = true")

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

      // Demonstrate the problematic behavior - adding partition with string value for int column
      println("\n⚠️  ATTEMPTING: ALTER TABLE ADD PARTITION with string value for int column...")
      println("   SQL: ALTER TABLE test_partition_table ADD PARTITION (partition_col='300')")

      try {
        // This would work in Spark 3.3 and earlier (with legacy flag)
        spark.sql("ALTER TABLE test_partition_table ADD PARTITION (partition_col='300')")
        println("✅ SUCCESS: Partition added with string value '300' for integer column")
        println("   ⚠️  WARNING: This creates type inconsistency!")

        // Show all partitions including the problematic one
        println("\n📋 Partitions after adding string partition:")
        spark.sql("SHOW PARTITIONS test_partition_table").show()

        // Try to query the data
        println("\n🔍 Querying data from all partitions:")
        val result = spark.sql("SELECT * FROM test_partition_table ORDER BY id")
        result.show()

        println("\n⚠️  LEGACY BEHAVIOR ISSUES:")
        println("   • Type inconsistency: partition_col='300' (string) vs partition_col=100 (int)")
        println("   • Potential query failures or unexpected results")
        println("   • Data integrity concerns")
        println("   • Inconsistent partition metadata")

      } catch {
        case e: Exception =>
          println(s"❌ Error adding partition: ${e.getMessage}")
          println("   This might occur if legacy flag is not set properly")
      }

      // Demonstrate another type mismatch scenario
      println("\n⚠️  ATTEMPTING: Another type mismatch scenario...")
      println("   SQL: ALTER TABLE test_partition_table ADD PARTITION (partition_col='invalid_string')")

      try {
        spark.sql("ALTER TABLE test_partition_table ADD PARTITION (partition_col='invalid_string')")
        println("✅ SUCCESS: Added partition with completely invalid string value")
        println("   ⚠️  CRITICAL: This creates serious data integrity issues!")

      } catch {
        case e: Exception =>
          println(s"❌ Error: ${e.getMessage}")
          println("   Some type mismatches might still fail even in legacy mode")
      }

      // Demonstrate programmatic partition addition with type issues
      println("\n⚠️  DEMONSTRATING: Programmatic partition addition with type issues...")
      try {
        val partitionValues = Seq("400", "invalid", "500.5")  // String values for int column
        
        partitionValues.foreach { value =>
          try {
            val sql = s"ALTER TABLE test_partition_table ADD PARTITION (partition_col='$value')"
            println(s"   Attempting: $sql")
            spark.sql(sql)
            println(s"   ✅ Added partition with value '$value' (type mismatch allowed)")
          } catch {
            case e: Exception =>
              println(s"   ❌ Failed to add partition '$value': ${e.getMessage}")
          }
        }

      } catch {
        case e: Exception =>
          println(s"❌ Error in programmatic addition: ${e.getMessage}")
      }

      // Show final partition list
      println("\n📊 Final partition list:")
      spark.sql("SHOW PARTITIONS test_partition_table").show()

      // Demonstrate potential query issues
      println("\n⚠️  DEMONSTRATING: Potential query issues with mixed types...")
      try {
        // Try filtering by partition column
        println("   Attempting to filter by partition_col = 300...")
        val filterResult = spark.sql("SELECT * FROM test_partition_table WHERE partition_col = 300")
        filterResult.show()

        println("   Attempting to filter by partition_col = '300'...")
        val filterResult2 = spark.sql("SELECT * FROM test_partition_table WHERE partition_col = '300'")
        filterResult2.show()

        println("⚠️  Notice potential inconsistencies in query results!")

      } catch {
        case e: Exception =>
          println(s"❌ Query error due to type inconsistencies: ${e.getMessage}")
      }

    } catch {
      case e: Exception =>
        println(s"❌ Error during demonstration: ${e.getMessage}")
        println("   This demonstrates why type validation is important")
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

      println("\n🎯 SUMMARY - Spark 3.3 and Earlier Behavior:")
      println("   • ALTER PARTITION allowed type mismatches")
      println("   • No validation of partition spec against column types")
      println("   • Could create data integrity issues")
      println("   • Required manual type checking by developers")
      println("   • Potential for inconsistent query behavior")
    }
  }
}

// If running in spark-shell, call the main method
AlterPartitionValidationBefore.main(Array())