#!/usr/bin/env python3
"""
Cast Alias Migration - BEFORE Script (Spark 3.1 and earlier)
This script demonstrates the old behavior where auto-generated CAST expressions 
appear in column alias names.

Usage:
    python before_script.py

This script can be run independently to test the old behavior.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import floor, ceil, round, abs as spark_abs

def main():
    # Create SparkSession
    spark = SparkSession.builder \
        .appName('CastAliasBefore') \
        .master('local[*]') \
        .getOrCreate()
    
    print("=== Cast Alias Migration - BEFORE (Spark 3.1 and earlier) ===")
    
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
        
        # 1. Using SQL with floor function on integer (triggers auto-cast)
        print("\n1. SQL with floor function on integer:")
        result1 = spark.sql("SELECT floor(1) as floor_result")
        result1.show()
        print(f"Column names: {result1.columns}")
        print("⚠️  In Spark 3.1: Column name includes CAST - 'FLOOR(CAST(1 AS DOUBLE))'")
        
        # 2. Using DataFrame API with floor on integer column
        print("\n2. DataFrame API with floor on integer column:")
        result2 = df.select(floor(df.int_col))
        result2.show()
        print(f"Column names: {result2.columns}")
        
        # 3. Multiple functions that trigger auto-cast
        print("\n3. Multiple functions with auto-cast:")
        result3 = spark.sql("""
            SELECT 
                floor(1) as floor_int,
                ceil(2) as ceil_int,
                round(3) as round_int,
                abs(-4) as abs_int
        """)
        result3.show()
        print(f"Column names: {result3.columns}")
        
        # 4. Mixed operations with type coercion
        print("\n4. Mixed operations with type coercion:")
        result4 = df.select(
            floor(df.int_col).alias('floor_int_col'),
            ceil(df.double_col).alias('ceil_double_col'),
            spark_abs(df.int_col - 5).alias('abs_operation')
        )
        result4.show()
        print(f"Column names: {result4.columns}")
        
        # 5. String to numeric conversion with math functions
        print("\n5. String to numeric conversion with math functions:")
        result5 = df.select(
            floor(df.string_col.cast('double')).alias('floor_from_string'),
            round(df.string_col.cast('double'), 1).alias('round_from_string')
        )
        result5.show()
        print(f"Column names: {result5.columns}")
        
        # 6. Complex expressions with auto-cast
        print("\n6. Complex expressions with auto-cast:")
        # Register temp view for SQL first
        df.createOrReplaceTempView("temp_view")
        result6 = spark.sql("""
            SELECT 
                floor(int_col + 0.5) as complex_floor,
                ceil(double_col * 2) as complex_ceil
            FROM temp_view
        """)
        result6.show()
        print(f"Column names: {result6.columns}")
        
        print("\n⚠️  BEHAVIOR IN SPARK 3.1 AND EARLIER:")
        print("   • Column aliases include auto-generated CAST expressions")
        print("   • Names like 'FLOOR(CAST(1 AS DOUBLE))' instead of 'FLOOR(1)'")
        print("   • This can cause issues with:")
        print("     - Column name consistency")
        print("     - Downstream applications expecting specific names")
        print("     - Code that relies on predictable column naming")
        print("     - Schema evolution and compatibility")
        
    except Exception as e:
        print(f"❌ Error executing cast alias operations: {e}")
        print("   This might indicate version-specific behavior differences")
    
    finally:
        spark.stop()
        print("\nSpark session stopped.")

if __name__ == "__main__":
    main()