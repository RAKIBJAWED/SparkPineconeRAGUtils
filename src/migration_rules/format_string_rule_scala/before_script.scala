// Format String Argument Index Migration - BEFORE Script (Spark 3.2 and earlier)
// This script demonstrates the old behavior where format_string() and printf() supported 0$ indexing.
//
// Language: Scala
// Usage: spark-shell -i before_script.scala
//
// This script can be run independently to test the old behavior.

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object FormatStringMigrationBefore {
  def main(args: Array[String]): Unit = {
    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("FormatStringMigrationBefore")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "spark-warehouse")
      .getOrCreate()

    import spark.implicits._

    println("=== Format String Argument Index Migration - BEFORE (Spark 3.2 behavior) ===")

    try {
      // Create sample data
      println("\n📊 Creating sample data...")
      val data = Seq(
        ("Alice", 25, 50000.0),
        ("Bob", 30, 60000.0),
        ("Charlie", 35, 70000.0)
      ).toDF("name", "age", "salary")

      data.show()

      // Demonstrate the problematic 0$ indexing in format_string
      println("\n⚠️  ATTEMPTING: Using 0$ indexing in format_string (Spark 3.2 behavior)...")
      println("   SQL: SELECT format_string('Employee: %0$s, Age: %1$d', name, age)")

      try {
        // This would work in Spark 3.2 and earlier
        val result1 = data.select(
          col("name"),
          col("age"),
          expr("format_string('Employee: %0$s, Age: %1$d', name, age)").alias("formatted_0_index")
        )

        println("✅ SUCCESS: 0$ indexing worked in Spark 3.2")
        result1.show(truncate = false)

      } catch {
        case e: Exception =>
          println(s"❌ Error with 0$ indexing: ${e.getMessage}")
          println("   This error indicates you're running on Spark 3.3+ where 0$ indexing is not supported")
      }

      // Demonstrate the problematic 0$ indexing in printf
      println("\n⚠️  ATTEMPTING: Using 0$ indexing in printf...")
      println("   SQL: SELECT printf('Salary: %0$.2f for %1$s', salary, name)")

      try {
        val result2 = data.select(
          col("name"),
          col("salary"),
          expr("printf('Salary: %0$.2f for %1$s', salary, name)").alias("printf_0_index")
        )

        println("✅ SUCCESS: 0$ indexing in printf worked in Spark 3.2")
        result2.show(truncate = false)

      } catch {
        case e: Exception =>
          println(s"❌ Error with 0$ indexing in printf: ${e.getMessage}")
          println("   This demonstrates the breaking change in Spark 3.3")
      }

      // Show more complex example with multiple 0$ references
      println("\n⚠️  ATTEMPTING: Complex format string with multiple 0$ references...")
      println("   SQL: SELECT format_string('%0$s is %1$d years old and earns %2$.2f. %0$s works hard!', name, age, salary)")

      try {
        val result3 = data.select(
          col("*"),
          expr("format_string('%0$s is %1$d years old and earns %2$.2f. %0$s works hard!', name, age, salary)")
            .alias("complex_0_index")
        )

        println("✅ SUCCESS: Complex 0$ indexing worked")
        result3.show(truncate = false)

        println("\n⚠️  LEGACY BEHAVIOR ISSUES:")
        println("   • 0$ indexing was non-standard and confusing")
        println("   • Inconsistent with most printf implementations")
        println("   • Could lead to off-by-one errors in argument positioning")
        println("   • Made format strings harder to read and maintain")

      } catch {
        case e: Exception =>
          println(s"❌ Error: ${e.getMessage}")
          println("   Complex 0$ indexing patterns are no longer supported")
      }

      // Demonstrate DataFrame API usage
      println("\n📊 Using DataFrame API with format_string...")
      try {
        val dfResult = data.withColumn(
          "description",
          expr("format_string('Name: %0$s, Details: Age %1$d, Salary $%2$.2f', name, age, salary)")
        )

        println("✅ DataFrame API with 0$ indexing:")
        dfResult.select("name", "description").show(truncate = false)

      } catch {
        case e: Exception =>
          println(s"❌ DataFrame API error: ${e.getMessage}")
      }

    } catch {
      case e: Exception =>
        println(s"❌ Error during demonstration: ${e.getMessage}")
        println("   This demonstrates compatibility issues with format string indexing")
    } finally {
      spark.stop()
      println("\nSpark session stopped.")

      println("\n🎯 SUMMARY - Spark 3.2 and Earlier Behavior:")
      println("   • format_string() and printf() supported 0$ indexing")
      println("   • First argument could be referenced as %0$")
      println("   • This was non-standard compared to other printf implementations")
      println("   • Could cause confusion and maintenance issues")
    }
  }
}

// If running in spark-shell, call the main method
FormatStringMigrationBefore.main(Array())