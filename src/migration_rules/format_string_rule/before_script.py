#!/usr/bin/env python3
"""
Format String Migration - BEFORE Script (Spark 3.2 and earlier)
This script demonstrates the deprecated 0$ indexing in format_string and printf functions.

Usage:
    python before_script.py

This script can be run independently to test the deprecated behavior.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

def main():
    # Create SparkSession
    spark = SparkSession.builder \
        .appName('FormatStringBefore') \
        .master('local[*]') \
        .getOrCreate()
    
    print("=== Format String Migration - BEFORE (Deprecated 0$ indexing) ===")
    
    try:
        # Create sample DataFrame
        data = [
            ('John', 5, 'Premium'),
            ('Alice', 100, 'Gold'),
            ('Bob', 25, 'Silver'),
            ('Carol', 75, 'Premium')
        ]
        df = spark.createDataFrame(data, ['name', 'message_count', 'tier'])
        
        print("\nOriginal DataFrame:")
        df.show()
        
        # Using format_string with 0$ indexing (deprecated in Spark 3.3+)
        print("\n1. Using format_string with 0$ indexing:")
        result1 = df.select(
            'name',
            'message_count',
            expr("format_string('Hello %0$s, you have %0$d messages', name, message_count)").alias('greeting')
        )
        result1.show(truncate=False)
        
        # Using printf with 0$ indexing (deprecated in Spark 3.3+)
        print("\n2. Using printf with 0$ indexing:")
        result2 = df.select(
            'name',
            'tier',
            expr("printf('User %0$s has %0$s tier status', name, tier)").alias('user_info')
        )
        result2.show(truncate=False)
        
        # Complex format string with multiple 0$ references
        print("\n3. Complex format string with multiple 0$ references:")
        result3 = df.select(
            'name',
            'message_count',
            'tier',
            expr("format_string('Welcome %0$s! You are a %0$s member with %0$d unread messages', name, tier, message_count)").alias('welcome_msg')
        )
        result3.show(truncate=False)
        
        # Nested format string operations
        print("\n4. Nested format string operations with 0$ indexing:")
        result4 = df.select(
            'name',
            'message_count',
            expr("format_string('Status: %0$s', format_string('User %0$s has %0$d items', name, message_count))").alias('nested_format')
        )
        result4.show(truncate=False)
        
        print("\n⚠️  WARNING: This code uses deprecated 0$ indexing which is not supported in Spark 3.3+")
        print("   It may cause parsing errors or unexpected behavior in newer versions.")
        print("   Expected errors: ArgumentIndexOutOfBoundsException or similar parsing errors")
        
    except Exception as e:
        print(f"❌ Error executing format string operations: {e}")
        print("   This error is expected in Spark 3.3+ due to deprecated 0$ indexing")
        print("   The error demonstrates why migration to 1$-based indexing is necessary")
    
    finally:
        spark.stop()
        print("\nSpark session stopped.")

if __name__ == "__main__":
    main()