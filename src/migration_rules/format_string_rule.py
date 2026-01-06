#!/usr/bin/env python3
"""
Format String Migration Rule

Since Spark 3.3, the strfmt in format_string(strfmt, obj, ...) and printf(strfmt, obj, ...)
will no longer support to use 0$ to specify the first argument, the first argument should
always reference by 1$ when use argument index to indicating the position of the argument
in the argument list.

This module demonstrates the before and after patterns for this migration.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string, printf, col, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FormatStringMigration:
    """Class demonstrating format_string and printf migration from Spark 3.2 to 3.3+."""
    
    def __init__(self, app_name: str = "FormatStringMigration"):
        """
        Initialize Spark session.
        
        Args:
            app_name: Name of the Spark application
        """
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .master("local[*]") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        logger.info(f"Spark session created: {app_name}")
        logger.info(f"Spark version: {self.spark.version}")
    
    def create_sample_data(self):
        """Create sample DataFrame for testing format_string operations."""
        logger.info("Creating sample data for format_string testing...")
        
        # Sample data with various data types
        data = [
            ("Alice", 25, 75000.50, "Engineer"),
            ("Bob", 30, 85000.75, "Manager"),
            ("Charlie", 35, 95000.25, "Director"),
            ("Diana", 28, 68000.00, "Analyst"),
            ("Eve", 32, 78000.80, "Senior Engineer")
        ]
        
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("salary", DoubleType(), True),
            StructField("position", StringType(), True)
        ])
        
        df = self.spark.createDataFrame(data, schema)
        logger.info(f"Created DataFrame with {df.count()} rows")
        return df
    
    def demonstrate_before_spark33(self, df):
        """
        Demonstrate format_string usage BEFORE Spark 3.3 (using 0$ indexing).
        
        Note: This will work in Spark 3.2 and earlier, but will fail in Spark 3.3+
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with formatted strings (or error in Spark 3.3+)
        """
        logger.info("Demonstrating BEFORE Spark 3.3 format_string usage (0$ indexing)...")
        
        try:
            # BEFORE: Using 0$ to reference the first argument (DEPRECATED in Spark 3.3+)
            result_df = df.select(
                col("name"),
                col("age"),
                col("salary"),
                col("position"),
                
                # Example 1: Simple format with 0$ indexing (OLD WAY - DEPRECATED)
                format_string("Employee: %0$s", col("name")).alias("formatted_name_old"),
                
                # Example 2: Multiple arguments with 0$ indexing (OLD WAY - DEPRECATED)
                format_string("Name: %0$s, Age: %1$d", col("name"), col("age")).alias("name_age_old"),
                
                # Example 3: Complex format with 0$ indexing (OLD WAY - DEPRECATED)
                format_string("Employee %0$s (%1$s) earns $%2$.2f", 
                            col("name"), col("position"), col("salary")).alias("full_info_old"),
                
                # Example 4: Reordered arguments with 0$ indexing (OLD WAY - DEPRECATED)
                format_string("Salary: $%1$.2f for %0$s", 
                            col("name"), col("salary")).alias("reordered_old")
            )
            
            logger.info("BEFORE Spark 3.3 format_string operations completed successfully")
            return result_df
            
        except Exception as e:
            logger.error(f"Error with BEFORE Spark 3.3 format_string (expected in Spark 3.3+): {e}")
            # Return a DataFrame with error information
            error_df = df.select(
                col("name"),
                lit(f"ERROR: {str(e)}").alias("error_message")
            )
            return error_df
    
    def demonstrate_after_spark33(self, df):
        """
        Demonstrate format_string usage AFTER Spark 3.3 (using 1$ indexing).
        
        This is the correct way to use format_string in Spark 3.3+
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with formatted strings using correct indexing
        """
        logger.info("Demonstrating AFTER Spark 3.3 format_string usage (1$ indexing)...")
        
        try:
            # AFTER: Using 1$ to reference the first argument (CORRECT in Spark 3.3+)
            result_df = df.select(
                col("name"),
                col("age"),
                col("salary"),
                col("position"),
                
                # Example 1: Simple format with 1$ indexing (NEW WAY - CORRECT)
                format_string("Employee: %1$s", col("name")).alias("formatted_name_new"),
                
                # Example 2: Multiple arguments with 1$ indexing (NEW WAY - CORRECT)
                format_string("Name: %1$s, Age: %2$d", col("name"), col("age")).alias("name_age_new"),
                
                # Example 3: Complex format with 1$ indexing (NEW WAY - CORRECT)
                format_string("Employee %1$s (%2$s) earns $%3$.2f", 
                            col("name"), col("position"), col("salary")).alias("full_info_new"),
                
                # Example 4: Reordered arguments with 1$ indexing (NEW WAY - CORRECT)
                format_string("Salary: $%2$.2f for %1$s", 
                            col("name"), col("salary")).alias("reordered_new"),
                
                # Example 5: Without explicit indexing (also works)
                format_string("Simple: %s is %d years old", 
                            col("name"), col("age")).alias("simple_format"),
                
                # Example 6: Mixed format types
                format_string("Report: %1$s (ID: %2$03d) - Status: %3$s", 
                            col("name"), col("age"), col("position")).alias("report_format")
            )
            
            logger.info("AFTER Spark 3.3 format_string operations completed successfully")
            return result_df
            
        except Exception as e:
            logger.error(f"Error with AFTER Spark 3.3 format_string: {e}")
            raise
    
    def demonstrate_printf_migration(self, df):
        """
        Demonstrate printf function migration from 0$ to 1$ indexing.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with printf results
        """
        logger.info("Demonstrating printf migration...")
        
        try:
            # Register DataFrame as temporary view for SQL operations
            df.createOrReplaceTempView("employees")
            
            # BEFORE Spark 3.3 (0$ indexing) - This would fail in Spark 3.3+
            logger.info("Attempting BEFORE Spark 3.3 printf usage (0$ indexing)...")
            try:
                old_printf_query = """
                SELECT name, age, salary,
                       printf('Employee: %0$s', name) as printf_old
                FROM employees
                LIMIT 3
                """
                old_result = self.spark.sql(old_printf_query)
                logger.info("OLD printf query executed successfully (Spark < 3.3)")
            except Exception as e:
                logger.warning(f"OLD printf query failed (expected in Spark 3.3+): {e}")
                old_result = None
            
            # AFTER Spark 3.3 (1$ indexing) - This is the correct way
            logger.info("Demonstrating AFTER Spark 3.3 printf usage (1$ indexing)...")
            new_printf_query = """
            SELECT name, age, salary, position,
                   printf('Employee: %1$s', name) as printf_simple,
                   printf('Name: %1$s, Age: %2$d', name, age) as printf_multi,
                   printf('Employee %1$s (%2$s) earns $%3$.2f', name, position, salary) as printf_complex,
                   printf('Report: %1$s (ID: %2$03d)', name, age) as printf_formatted
            FROM employees
            """
            
            new_result = self.spark.sql(new_printf_query)
            logger.info("NEW printf query executed successfully")
            
            return {
                'old_result': old_result,
                'new_result': new_result
            }
            
        except Exception as e:
            logger.error(f"Error in printf migration demonstration: {e}")
            raise
    
    def run_migration_demo(self):
        """
        Run complete migration demonstration showing before and after patterns.
        
        Returns:
            Dictionary with all results
        """
        logger.info("Running complete format_string migration demonstration...")
        
        # Create sample data
        df = self.create_sample_data()
        
        print("\n" + "="*80)
        print("SPARK 3.3 FORMAT_STRING MIGRATION DEMONSTRATION")
        print("="*80)
        
        print("\n📋 Sample Data:")
        df.show()
        
        # Demonstrate BEFORE Spark 3.3 (0$ indexing)
        print("\n❌ BEFORE Spark 3.3 (0$ indexing - DEPRECATED):")
        print("   format_string('Employee: %0$s', col('name'))")
        print("   format_string('Name: %0$s, Age: %1$d', col('name'), col('age'))")
        
        before_result = self.demonstrate_before_spark33(df)
        try:
            before_result.show(truncate=False)
        except Exception as e:
            print(f"   ⚠️  Error (expected in Spark 3.3+): {e}")
        
        # Demonstrate AFTER Spark 3.3 (1$ indexing)
        print("\n✅ AFTER Spark 3.3 (1$ indexing - CORRECT):")
        print("   format_string('Employee: %1$s', col('name'))")
        print("   format_string('Name: %1$s, Age: %2$d', col('name'), col('age'))")
        
        after_result = self.demonstrate_after_spark33(df)
        after_result.show(truncate=False)
        
        # Demonstrate printf migration
        print("\n🔄 PRINTF Migration:")
        printf_results = self.demonstrate_printf_migration(df)
        
        if printf_results['old_result']:
            print("\n❌ OLD printf (0$ indexing):")
            printf_results['old_result'].show(truncate=False)
        
        print("\n✅ NEW printf (1$ indexing):")
        printf_results['new_result'].show(truncate=False)
        
        # Migration summary
        print("\n" + "="*80)
        print("MIGRATION SUMMARY")
        print("="*80)
        print("🔄 CHANGE REQUIRED:")
        print("   • Replace %0$ with %1$ for first argument")
        print("   • Replace %1$ with %2$ for second argument")
        print("   • Replace %n$ with %(n+1)$ for nth argument")
        print("\n📝 EXAMPLES:")
        print("   BEFORE: format_string('%0$s is %1$d', name, age)")
        print("   AFTER:  format_string('%1$s is %2$d', name, age)")
        print("\n   BEFORE: printf('Hello %0$s', name)")
        print("   AFTER:  printf('Hello %1$s', name)")
        print("\n✨ ALTERNATIVE: Use format without explicit indexing:")
        print("   format_string('%s is %d', name, age)  # No indexing needed")
        
        return {
            'sample_data': df,
            'before_result': before_result,
            'after_result': after_result,
            'printf_results': printf_results
        }
    
    def stop(self):
        """Stop the Spark session."""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")


def main():
    """Main function to run the format_string migration demonstration."""
    migration = FormatStringMigration("FormatStringMigrationDemo")
    
    try:
        results = migration.run_migration_demo()
        print(f"\n🎉 Migration demonstration completed successfully!")
        print(f"   Sample data rows: {results['sample_data'].count()}")
        print(f"   After result columns: {len(results['after_result'].columns)}")
        
    except Exception as e:
        logger.error(f"Error in migration demonstration: {e}")
        raise
    finally:
        migration.stop()


if __name__ == "__main__":
    main()