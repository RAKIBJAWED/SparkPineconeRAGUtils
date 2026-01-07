// Cast Auto-Generation Column Alias Migration - BEFORE Script (Spark 3.1 and earlier)
// This script demonstrates the old behavior where auto-generated CAST expressions appeared in column names.
//
// Language: Scala
// Usage: spark-shell -i before_script.scala
//
// This script can be run independently to test the old behavior.

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object CastAliasMigrationBefore {
  def main(args: Array[String]): Unit = {
    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("CastAliasMigrationBefore")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "spark-warehouse")
      .getOrCreate()

    import spark.implicits._

    println("=== Cast Auto-Generation Column Alias Migration - BEFORE (Spark 3.1 behavior) ===")

    try {
      // Create sample data
      println("\n📊 Creating sample data...")
      val data = Seq(
        (1, 2.5, "2021-01-01"),
        (2, 3.7, "2021-02-15"),
        (3, 1.2, "2021-03-30")
      ).toDF("id", "value", "date_str")

      data.show()
      println("Schema:")
      data.printSchema()

      // Demonstrate FLOOR function with implicit casting
      println("\n⚠️  DEMONSTRATING: FLOOR function with implicit type coercion...")
      println("   SQL: SELECT floor(1) AS floor_result")

      try {
        val floorResult = spark.sql("SELECT floor(1) AS floor_result")
        println("✅ Query executed successfully")
        floorResult.show()

        // Check column names - this is where the issue appears
        val columnNames = floorResult.columns
        println(s"📋 Column names: ${columnNames.mkString(", ")}")

        // In Spark 3.1, this might show something like "FLOOR(CAST(1 AS DOUBLE))"
        println("⚠️  ISSUE: Column name may include auto-generated CAST expression")

      } catch {
        case e: Exception =>
          println(s"❌ Error: ${e.getMessage}")
      }

      // Demonstrate with DataFrame API
      println("\n⚠️  DEMONSTRATING: DataFrame API with implicit casting...")
      try {
        val dfResult = data.select(
          col("id"),
          floor(lit(1)).alias("floor_literal"),
          floor(col("value")).alias("floor_value"),
          ceil(col("value")).alias("ceil_value")
        )

        println("✅ DataFrame operations executed")
        dfResult.show()

        println("📋 Column names from DataFrame API:")
        dfResult.columns.foreach(colName => println(s"   • $colName"))

      } catch {
        case e: Exception =>
          println(s"❌ Error: ${e.getMessage}")
      }

      // Demonstrate more complex expressions with type coercion
      println("\n⚠️  DEMONSTRATING: Complex expressions with type coercion...")
      try {
        val complexResult = spark.sql("""
          SELECT 
            floor(1) as floor_int,
            floor(1.5) as floor_double,
            ceil(2) as ceil_int,
            round(3) as round_int,
            abs(-4) as abs_int,
            sqrt(9) as sqrt_int
        """)

        println("✅ Complex expressions executed")
        complexResult.show()

        println("📋 Column names with potential CAST expressions:")
        complexResult.columns.foreach { colName =>
          println(s"   • $colName")
          if (colName.contains("CAST")) {
            println(s"     ⚠️  Contains CAST expression!")
          }
        }

      } catch {
        case e: Exception =>
          println(s"❌ Error: ${e.getMessage}")
      }

      // Demonstrate the impact on downstream operations
      println("\n⚠️  DEMONSTRATING: Impact on downstream operations...")
      try {
        // Create a view with potentially problematic column names
        val viewResult = spark.sql("SELECT floor(1) as computed_floor")
        viewResult.createOrReplaceTempView("temp_view")

        // Try to reference the column in another query
        println("   Attempting to reference column in subsequent query...")
        val referenceResult = spark.sql("SELECT computed_floor * 2 FROM temp_view")
        referenceResult.show()

        println("✅ Reference worked, but column name consistency may be an issue")

      } catch {
        case e: Exception =>
          println(s"❌ Error referencing column: ${e.getMessage}")
          println("   This demonstrates potential downstream issues")
      }

      // Show column name extraction for programmatic use
      println("\n⚠️  DEMONSTRATING: Programmatic column name usage...")
      try {
        val testDF = spark.sql("SELECT floor(1), ceil(2.5), round(3.7)")
        val columns = testDF.columns

        println("📋 Extracted column names for programmatic use:")
        columns.zipWithIndex.foreach { case (colName, index) =>
          println(s"   Column $index: '$colName'")
          
          // This is where problems occur - column names might be inconsistent
          if (colName.length > 20) {
            println(s"     ⚠️  Very long column name - likely contains CAST expression")
          }
        }

        // Demonstrate potential issues with column selection
        println("\n   Attempting to select columns programmatically...")
        val selectedDF = testDF.select(columns.head)
        selectedDF.show()

      } catch {
        case e: Exception =>
          println(s"❌ Error with programmatic column usage: ${e.getMessage}")
      }

      println("\n⚠️  LEGACY BEHAVIOR ISSUES:")
      println("   • Column names include verbose CAST expressions")
      println("   • Inconsistent naming between similar operations")
      println("   • Harder to reference columns programmatically")
      println("   • Potential issues with external tools expecting clean names")
      println("   • Reduced readability in query results")

    } catch {
      case e: Exception =>
        println(s"❌ Error during demonstration: ${e.getMessage}")
        println("   This demonstrates potential issues with auto-generated column names")
    } finally {
      spark.stop()
      println("\nSpark session stopped.")

      println("\n🎯 SUMMARY - Spark 3.1 and Earlier Behavior:")
      println("   • Auto-generated CAST expressions appeared in column aliases")
      println("   • Column names could be verbose and inconsistent")
      println("   • Made programmatic column handling more difficult")
      println("   • Reduced query result readability")
    }
  }
}

// If running in spark-shell, call the main method
CastAliasMigrationBefore.main(Array())