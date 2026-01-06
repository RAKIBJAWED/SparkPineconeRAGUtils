#!/usr/bin/env python3
"""
Format String Migration - AFTER Script (Spark 3.3+)
This script demonstrates the correct 1$-based indexing in format_string and printf functions.

Usage:
    python after_script.py

This script can be run independently to test the corrected behavior.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

def main():
    # Create SparkSession
    spark = SparkSession.builder \
        .appName('FormatStringAfter') \
        .master('local[*]') \
        .getOrCreate()
    
    print("=== Format String Migration - AFTER (Correct 1$-based indexing) ===")
    
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
        
        # Using format_string with 1$-based indexing (correct for Spark 3.3+)
        print("\n1. Using format_string with 1$-based indexing:")
        result1 = df.select(
            'name',
            'message_count',
            expr("format_string('Hello %1$s, you have %2$d messages', name, message_count)").alias('greeting')
        )
        result1.show(truncate=False)
        
        # Using printf with 1$-based indexing (correct for Spark 3.3+)
        print("\n2. Using printf with 1$-based indexing:")
        result2 = df.select(
            'name',
            'tier',
            expr("printf('User %1$s has %2$s tier status', name, tier)").alias('user_info')
        )
        result2.show(truncate=False)
        
        # Complex format string with proper argument indexing
        print("\n3. Complex format string with proper 1$-based indexing:")
        result3 = df.select(
            'name',
            'message_count',
            'tier',
            expr("format_string('Welcome %1$s! You are a %3$s member with %2$d unread messages', name, message_count, tier)").alias('welcome_msg')
        )
        result3.show(truncate=False)
        
        # Demonstrating argument reuse with proper indexing
        print("\n4. Argument reuse with proper indexing:")
        result4 = df.select(
            'name',
            expr("format_string('Hello %1$s, welcome back %1$s!', name)").alias('reuse_example')
        )
        result4.show(truncate=False)
        
        # Mixed format types with proper indexing
        print("\n5. Mixed format types with proper indexing:")
        result5 = df.select(
            'name',
            'message_count',
            'tier',
            expr("printf('User: %1$s | Messages: %2$d | Tier: %3$s | Score: %2$d%%', name, message_count, tier)").alias('detailed_info')
        )
        result5.show(truncate=False)
        
        # Nested format string operations with correct indexing
        print("\n6. Nested format string operations with 1$-based indexing:")
        result6 = df.select(
            'name',
            'message_count',
            expr("format_string('Status: %1$s', format_string('User %1$s has %2$d items', name, message_count))").alias('nested_format')
        )
        result6.show(truncate=False)
        
        print("\n✅ SUCCESS: All format string operations completed successfully!")
        print("   This code uses correct 1$-based indexing compatible with Spark 3.3+")
        print("   All argument references start from 1$ instead of 0$")
        
    except Exception as e:
        print(f"❌ Error executing format string operations: {e}")
        print("   If you see this error, there might be an issue with the Spark setup or syntax")
    
    finally:
        spark.stop()
        print("\nSpark session stopped.")

if __name__ == "__main__":
    main()