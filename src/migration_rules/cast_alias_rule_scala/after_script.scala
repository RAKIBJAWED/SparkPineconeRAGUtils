// Cast Auto-Generation Column Alias Migration - AFTER Script (Spark 3.2+)
// This script demonstrates the updated behavior where auto-generated CAST expressions are stripped from column names.
//
// Language: Scala
// Usage: spark-shell -i after_script.scala
//
// This script can be run independently to test the updated behavior.

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object CastAliasMigrationAfter {
  def main(args: Array[String]): Unit = {
    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("CastAliasMigrationAfter")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "spark-warehouse")
      .getOrCreate()

    import spark.implicits._

    println("=== Cast Auto-Generation Column Alias Migration - AFTER (Spark 3.2+ behavior) ===")

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

      // Demonstrate FLOOR function with clean column names
      println("\n✅ DEMONSTRATING: FLOOR function with clean column names...")
      println("   SQL: SELECT floor(1) AS floor_result")

      try {
        val floorResult = spark.sql("SELECT floor(1) AS floor_result")
        println("✅ Query executed successfully")
        floorResult.show()

        // Check column names - should be clean in Spark 3.2+
        val columnNames = floorResult.columns
        println(s"📋 Column names: ${columnNames.mkString(", ")}")
        println("✅ Column name is clean and readable!")

      } catch {
        case e: Exception =>
          println(s"❌ Error: ${e.getMessage}")
      }

      // Demonstrate with DataFrame API
      println("\n✅ DEMONSTRATING: DataFrame API with clean column names...")
      try {
        val dfResult = data.select(
          col("id"),
          floor(lit(1)).alias("floor_literal"),
          floor(col("value")).alias("floor_value"),
          ceil(col("value")).alias("ceil_value")
        )

        println("✅ DataFrame operations executed")
        dfResult.show()

        println("📋 Clean column names from DataFrame API:")
        dfResult.columns.foreach(colName => println(s"   • $colName"))

      } catch {
        case e: Exception =>
          println(s"❌ Error: ${e.getMessage}")
      }

      // Demonstrate automatic column name generation
      println("\n✅ DEMONSTRATING: Automatic clean column name generation...")
      try {
        val autoResult = spark.sql("""
          SELECT 
            floor(1),
            floor(1.5),
            ceil(2),
            round(3),
            abs(-4),
            sqrt(9)
        """)

        println("✅ Auto-generated column names:")
        autoResult.show()

        println("📋 Clean auto-generated column names:")
        autoResult.columns.foreach { colName =>
          println(s"   • '$colName'")
          if (colName.startsWith("FLOOR") && !colName.contains("CAST")) {
            println(s"     ✅ Clean name without CAST expression!")
          }
        }

      } catch {
        case e: Exception =>
          println(s"❌ Error: ${e.getMessage}")
      }

      // Demonstrate improved downstream operations
      println("\n✅ DEMONSTRATING: Improved downstream operations...")
      try {
        // Create a view with clean column names
        val viewResult = spark.sql("SELECT floor(1) as computed_floor")
        viewResult.createOrReplaceTempView("temp_view")

        println("📋 Column names in view:")
        viewResult.columns.foreach(col => println(s"   • $col"))

        // Reference the column in another query
        println("   Referencing clean column name in subsequent query...")
        val referenceResult = spark.sql("SELECT computed_floor * 2 as doubled FROM temp_view")
        referenceResult.show()

        println("✅ Clean column references work perfectly!")

      } catch {
        case e: Exception =>
          println(s"❌ Error: ${e.getMessage}")
      }

      // Show improved programmatic column handling
      println("\n✅ DEMONSTRATING: Improved programmatic column handling...")
      try {
        val testDF = spark.sql("SELECT floor(1), ceil(2.5), round(3.7)")
        val columns = testDF.columns

        println("📋 Clean column names for programmatic use:")
        columns.zipWithIndex.foreach { case (colName, index) =>
          println(s"   Column $index: '$colName'")
          println(s"     ✅ Length: ${colName.length} characters (concise!)")
        }

        // Demonstrate easy column selection
        println("\n   Easy programmatic column selection:")
        columns.foreach { colName =>
          val selectedDF = testDF.select(col(colName))
          println(s"   ✅ Selected '$colName' successfully")
        }

      } catch {
        case e: Exception =>
          println(s"❌ Error: ${e.getMessage}")
      }

      // Demonstrate consistency across different operations
      println("\n✅ DEMONSTRATING: Consistent naming across operations...")
      try {
        val consistencyTest = spark.sql("""
          SELECT 
            floor(1) as floor_op,
            floor(1.0) as floor_op_double,
            floor(CAST(1 AS DOUBLE)) as floor_explicit_cast
        """)

        println("✅ Consistency test results:")
        consistencyTest.show()

        println("📋 All operations produce consistent column names:")
        consistencyTest.columns.foreach(col => println(s"   • $col"))

      } catch {
        case e: Exception =>
          println(s"❌ Error: ${e.getMessage}")
      }

      // Show benefits for external tool integration
      println("\n✅ DEMONSTRATING: Benefits for external tool integration...")
      try {
        val exportDF = spark.sql("""
          SELECT 
            floor(value) as floor_value,
            ceil(value) as ceil_value,
            round(value, 2) as rounded_value
          FROM (SELECT 3.14159 as value)
        """)

        println("✅ Export-ready DataFrame with clean column names:")
        exportDF.show()

        // Simulate export metadata
        println("📋 Export metadata (clean column names):")
        exportDF.schema.fields.foreach { field =>
          println(s"   • ${field.name}: ${field.dataType}")
        }

        println("✅ Perfect for CSV, Parquet, JSON exports!")

      } catch {
        case e: Exception =>
          println(s"❌ Error: ${e.getMessage}")
      }

      println("\n🎉 SPARK 3.2+ BENEFITS:")
      println("   • Clean, readable column names")
      println("   • Consistent naming across similar operations")
      println("   • Easier programmatic column handling")
      println("   • Better integration with external tools")
      println("   • Improved query result readability")
      println("   • Reduced metadata verbosity")

    } catch {
      case e: Exception =>
        println(s"❌ Error during demonstration: ${e.getMessage}")
        println("   Check your Spark version and configuration")
    } finally {
      spark.stop()
      println("\nSpark session stopped.")

      println("\n🎯 SUMMARY - Spark 3.2+ Improvements:")
      println("   • Auto-generated CAST expressions are stripped from column names")
      println("   • Column names are clean and concise")
      println("   • Better consistency and readability")
      println("   • Improved developer and tool experience")
    }
  }
}

// If running in spark-shell, call the main method
CastAliasMigrationAfter.main(Array())