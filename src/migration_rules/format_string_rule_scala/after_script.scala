// Format String Argument Index Migration - AFTER Script (Spark 3.3+)
// This script demonstrates the updated behavior where format_string() and printf() require 1$-based indexing.
//
// Language: Scala
// Usage: spark-shell -i after_script.scala
//
// This script can be run independently to test the updated behavior.

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object FormatStringMigrationAfter {
  def main(args: Array[String]): Unit = {
    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("FormatStringMigrationAfter")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "spark-warehouse")
      .getOrCreate()

    import spark.implicits._

    println("=== Format String Argument Index Migration - AFTER (Spark 3.3+ behavior) ===")

    try {
      // Create sample data
      println("\n📊 Creating sample data...")
      val data = Seq(
        ("Alice", 25, 50000.0),
        ("Bob", 30, 60000.0),
        ("Charlie", 35, 70000.0)
      ).toDF("name", "age", "salary")

      data.show()

      // Demonstrate that 0$ indexing no longer works
      println("\n🔍 ATTEMPTING: Using 0$ indexing (should fail in Spark 3.3+)...")
      println("   SQL: SELECT format_string('Employee: %0$s, Age: %1$d', name, age)")

      try {
        val result1 = data.select(
          col("name"),
          col("age"),
          expr("format_string('Employee: %0$s, Age: %1$d', name, age)").alias("formatted_0_index")
        )

        result1.show(truncate = false)
        println("❌ UNEXPECTED: 0$ indexing worked (this shouldn't happen in Spark 3.3+)")

      } catch {
        case e: Exception =>
          println(s"✅ EXPECTED FAILURE: ${e.getMessage}")
          println("   🎉 Spark 3.3+ correctly rejects 0$ indexing!")
      }

      // Demonstrate the CORRECT approach with 1$-based indexing
      println("\n✅ CORRECT APPROACH: Using 1$-based indexing...")
      println("   SQL: SELECT format_string('Employee: %1$s, Age: %2$d', name, age)")

      try {
        val result2 = data.select(
          col("name"),
          col("age"),
          expr("format_string('Employee: %1$s, Age: %2$d', name, age)").alias("formatted_1_index")
        )

        println("✅ SUCCESS: 1$-based indexing works correctly")
        result2.show(truncate = false)

      } catch {
        case e: Exception =>
          println(s"❌ Unexpected error with correct indexing: ${e.getMessage}")
      }

      // Demonstrate correct printf usage
      println("\n✅ CORRECT: Using 1$-based indexing in printf...")
      println("   SQL: SELECT printf('Salary: %1$.2f for %2$s', salary, name)")

      try {
        val result3 = data.select(
          col("name"),
          col("salary"),
          expr("printf('Salary: %1$.2f for %2$s', salary, name)").alias("printf_1_index")
        )

        println("✅ SUCCESS: printf with 1$-based indexing")
        result3.show(truncate = false)

      } catch {
        case e: Exception =>
          println(s"❌ Error: ${e.getMessage}")
      }

      // Show complex example with proper indexing
      println("\n✅ BEST PRACTICE: Complex format string with 1$-based indexing...")
      println("   SQL: SELECT format_string('%1$s is %2$d years old and earns %3$.2f. %1$s works hard!', name, age, salary)")

      try {
        val result4 = data.select(
          col("*"),
          expr("format_string('%1$s is %2$d years old and earns %3$.2f. %1$s works hard!', name, age, salary)")
            .alias("complex_1_index")
        )

        println("✅ SUCCESS: Complex 1$-based indexing")
        result4.show(truncate = false)

      } catch {
        case e: Exception =>
          println(s"❌ Error: ${e.getMessage}")
      }

      // Demonstrate DataFrame API with correct indexing
      println("\n📊 DataFrame API with correct 1$-based indexing...")
      try {
        val dfResult = data.withColumn(
          "description",
          expr("format_string('Name: %1$s, Details: Age %2$d, Salary $%3$.2f', name, age, salary)")
        )

        println("✅ DataFrame API with 1$-based indexing:")
        dfResult.select("name", "description").show(truncate = false)

      } catch {
        case e: Exception =>
          println(s"❌ DataFrame API error: ${e.getMessage}")
      }

      // Show alternative approaches without explicit indexing
      println("\n💡 ALTERNATIVE: Using format strings without explicit indexing...")
      try {
        val result5 = data.select(
          col("*"),
          expr("format_string('Employee: %s, Age: %d, Salary: %.2f', name, age, salary)")
            .alias("simple_format")
        )

        println("✅ SUCCESS: Simple format without explicit indexing")
        result5.show(truncate = false)

      } catch {
        case e: Exception =>
          println(s"❌ Error: ${e.getMessage}")
      }

      // Demonstrate concat_ws as an alternative
      println("\n💡 ALTERNATIVE: Using concat_ws for simple string concatenation...")
      try {
        val result6 = data.select(
          col("*"),
          concat_ws(" - ", 
            concat(lit("Name: "), col("name")),
            concat(lit("Age: "), col("age")),
            concat(lit("Salary: $"), format_number(col("salary"), 2))
          ).alias("concat_alternative")
        )

        println("✅ SUCCESS: concat_ws alternative")
        result6.show(truncate = false)

      } catch {
        case e: Exception =>
          println(s"❌ Error: ${e.getMessage}")
      }

      println("\n🎯 MIGRATION RECOMMENDATIONS:")
      println("   ✅ Replace all %0$ references with %1$")
      println("   ✅ Increment all other index numbers by 1")
      println("   ✅ Use simple format strings without indexing when possible")
      println("   ✅ Consider concat_ws() for simple concatenations")
      println("   ✅ Test all format_string() and printf() calls after upgrade")

    } catch {
      case e: Exception =>
        println(s"❌ Error during demonstration: ${e.getMessage}")
        println("   Check your Spark version and configuration")
    } finally {
      spark.stop()
      println("\nSpark session stopped.")

      println("\n🎉 SUMMARY - Spark 3.3+ Benefits:")
      println("   • Consistent with standard printf implementations")
      println("   • Eliminates confusion about argument indexing")
      println("   • Prevents off-by-one errors")
      println("   • Improves code readability and maintainability")
    }
  }
}

// If running in spark-shell, call the main method
FormatStringMigrationAfter.main(Array())