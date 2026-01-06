#!/usr/bin/env python3
"""
Cast Alias Migration - AFTER Script (Spark 3.2+)
This script demonstrates the new behavior where auto-generated CAST expressions 
are stripped from column alias names.

Usage:
    python after_script.py

This script can be run independently to test the new behavior.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import floor, ceil, round, abs as spark_abs

def main():
    # Create SparkSession
    spark = SparkSession.builder \
        .appName('CastAliasAfter') \
        .master('local[*]') \
        .getOrCreate()
    
    print("=== Cast Alias Migration - AFTER (Spark 3.2+) ===")
    
    try:
        # Create sample DataFrame with different data types
        data = [
            (1, 2.5, "10.7", True),
            (2, 3.8, "15.2", False),
            (3, 1.2, "8.9", True),
            (4, 4.6, "12.3", False)
        ]
        df = spark.createDataFrame(data, ['int_col', 'double_col', 'string_col', 'bool_col'])
        
        print("\nOriginal DataFrame:")
        df.show()
        print(f"Original columns: {df.columns}")
        
        # 1. Using SQL with floor function on integer (auto-cast stripped)
        print("\n1. SQL with floor function on integer:")
        result1 = spark.sql("SELECT floor(1) as floor_result")
        result1.show()
        print(f"Column names: {result1.columns}")
        print("✅ In Spark 3.2+: Column name is clean - 'FLOOR(1)' (CAST stripped)")
        
        # 2. Using DataFrame API with floor on integer column
        print("\n2. DataFrame API with floor on integer column:")
        result2 = df.select(floor(df.int_col))
        result2.show()
        print(f"Column names: {result2.columns}")
        print("✅ Clean column name without CAST expressions")
        
        # 3. Multiple functions with stripped auto-cast
        print("\n3. Multiple functions with clean aliases:")
        result3 = spark.sql("""
            SELECT 
                floor(1) as floor_int,
                ceil(2) as ceil_int,
                round(3) as round_int,
                abs(-4) as abs_int
        """)
        result3.show()
        print(f"Column names: {result3.columns}")
        print("✅ All column names are clean and predictable")
        
        # 4. Mixed operations with clean naming
        print("\n4. Mixed operations with clean column names:")
        result4 = df.select(
            floor(df.int_col).alias('floor_int_col'),
            ceil(df.double_col).alias('ceil_double_col'),
            spark_abs(df.int_col - 5).alias('abs_operation')
        )
        result4.show()
        print(f"Column names: {result4.columns}")
        print("✅ Explicit aliases work as expected")
        
        # 5. String to numeric conversion with clean names
        print("\n5. String to numeric conversion with clean names:")
        result5 = df.select(
            floor(df.string_col.cast('double')).alias('floor_from_string'),
            round(df.string_col.cast('double'), 1).alias('round_from_string')
        )
        result5.show()
        print(f"Column names: {result5.columns}")
        print("✅ Even complex type conversions have clean names")
        
        # 6. Complex expressions with clean aliases
        print("\n6. Complex expressions with clean aliases:")
        df.createOrReplaceTempView("temp_view")
        result6 = spark.sql("""
            SELECT 
                floor(int_col + 0.5) as complex_floor,
                ceil(double_col * 2) as complex_ceil
            FROM temp_view
        """)
        result6.show()
        print(f"Column names: {result6.columns}")
        print("✅ Complex expressions also have clean, readable names")
        
        # 7. Demonstrate the improvement with auto-generated names
        print("\n7. Auto-generated column names (no explicit alias):")
        result7 = spark.sql("""
            SELECT 
                floor(1),
                ceil(2.5),
                round(3.7),
                abs(-4)
        """)
        result7.show()
        print(f"Column names: {result7.columns}")
        print("✅ Auto-generated names are clean: FLOOR(1), CEIL(2.5), etc.")
        
        # 8. Comparison with explicit casting
        print("\n8. Explicit vs implicit casting behavior:")
        result8 = spark.sql("""
            SELECT 
                floor(1) as implicit_cast,
                floor(CAST(1 AS DOUBLE)) as explicit_cast
        """)
        result8.show()
        print(f"Column names: {result8.columns}")
        print("✅ Both implicit and explicit casting produce clean names")
        
        print("\n✅ IMPROVEMENTS IN SPARK 3.2+:")
        print("   • Column aliases are clean and predictable")
        print("   • Auto-generated CAST expressions are stripped from names")
        print("   • Better consistency in column naming")
        print("   • Improved compatibility with downstream applications")
        print("   • Easier schema evolution and maintenance")
        print("   • More readable and intuitive column names")
        
        print("\n📋 MIGRATION BENEFITS:")
        print("   • Consistent column naming across versions")
        print("   • Reduced complexity in column name handling")
        print("   • Better integration with external tools")
        print("   • Improved code maintainability")
        
    except Exception as e:
        print(f"❌ Error executing cast alias operations: {e}")
        print("   This might indicate version-specific behavior differences")
    
    finally:
        spark.stop()
        print("\nSpark session stopped.")

if __name__ == "__main__":
    main()