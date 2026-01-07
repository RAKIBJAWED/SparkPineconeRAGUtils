#!/usr/bin/env python3
"""
ALTER PARTITION Type Validation Migration - BEFORE Script (Spark 3.3 and earlier)
This script demonstrates the old behavior where ALTER PARTITION allowed type mismatches.

Language: Python
Usage: python before_script.py

This script can be run independently to test the old behavior.
In Spark 3.3 and earlier, ALTER PARTITION did not validate partition spec types.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import tempfile
import shutil
import os


def main():
    # Create SparkSession with legacy behavior (simulating Spark 3.3)
    spark = SparkSession.builder \
        .appName('AlterPartitionValidationBefore') \
        .master('local[*]') \
        .config('spark.sql.legacy.skipTypeValidationOnAlterPartition', 'true') \
        .config('spark.sql.warehouse.dir', 'spark-warehouse') \
        .getOrCreate()
    
    print("=== ALTER PARTITION Type Validation - BEFORE (Spark 3.3 behavior) ===")
    print("Configuration: spark.sql.legacy.skipTypeValidationOnAlterPartition = true")
    
    # Create a temporary directory for our table
    temp_dir = tempfile.mkdtemp()
    table_path = os.path.join(temp_dir, "test_partition_table")
    
    try:
        # Create sample data with integer partition column
        print("\n📊 Creating sample partitioned table...")
        
        # Define schema with integer partition column
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("partition_col", IntegerType(), True)  # Integer partition column
        ])
        
        # Create sample data
        data = [
            (1, "Alice", 100),
            (2, "Bob", 200),
            (3, "Charlie", 100),
            (4, "David", 200)
        ]
        
        df = spark.createDataFrame(data, schema)
        
        # Write as partitioned table
        df.write \
          .mode("overwrite") \
          .partitionBy("partition_col") \
          .option("path", table_path) \
          .saveAsTable("test_partition_table")
        
        print("✅ Created partitioned table with integer partition column 'partition_col'")
        
        # Show current partitions
        print("\n📋 Current partitions:")
        spark.sql("SHOW PARTITIONS test_partition_table").show()
        
        # Demonstrate the problematic behavior - adding partition with string value for int column
        print("\n⚠️  ATTEMPTING: ALTER TABLE ADD PARTITION with string value for int column...")
        print("   SQL: ALTER TABLE test_partition_table ADD PARTITION (partition_col='300')")
        
        try:
            # This would work in Spark 3.3 and earlier (with legacy flag)
            spark.sql("ALTER TABLE test_partition_table ADD PARTITION (partition_col='300')")
            print("✅ SUCCESS: Partition added with string value '300' for integer column")
            print("   ⚠️  WARNING: This creates type inconsistency!")
            
            # Show all partitions including the problematic one
            print("\n📋 Partitions after adding string partition:")
            spark.sql("SHOW PARTITIONS test_partition_table").show()
            
            # Try to query the data
            print("\n🔍 Querying data from all partitions:")
            result = spark.sql("SELECT * FROM test_partition_table ORDER BY id")
            result.show()
            
            print("\n⚠️  LEGACY BEHAVIOR ISSUES:")
            print("   • Type inconsistency: partition_col='300' (string) vs partition_col=100 (int)")
            print("   • Potential query failures or unexpected results")
            print("   • Data integrity concerns")
            print("   • Inconsistent partition metadata")
            
        except Exception as e:
            print(f"❌ Error adding partition: {e}")
            print("   This might occur if legacy flag is not set properly")
        
        # Demonstrate another type mismatch scenario
        print(f"\n⚠️  ATTEMPTING: Another type mismatch scenario...")
        print("   SQL: ALTER TABLE test_partition_table ADD PARTITION (partition_col='invalid_string')")
        
        try:
            spark.sql("ALTER TABLE test_partition_table ADD PARTITION (partition_col='invalid_string')")
            print("✅ SUCCESS: Added partition with completely invalid string value")
            print("   ⚠️  CRITICAL: This creates serious data integrity issues!")
            
        except Exception as e:
            print(f"❌ Error: {e}")
            print("   Some type mismatches might still fail even in legacy mode")
        
        print(f"\n📊 Final partition list:")
        spark.sql("SHOW PARTITIONS test_partition_table").show()
        
    except Exception as e:
        print(f"❌ Error during demonstration: {e}")
        print("   This demonstrates why type validation is important")
    
    finally:
        # Cleanup
        try:
            spark.sql("DROP TABLE IF EXISTS test_partition_table")
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
        except:
            pass
        
        spark.stop()
        print("\nSpark session stopped.")
        
        print(f"\n🎯 SUMMARY - Spark 3.3 and Earlier Behavior:")
        print(f"   • ALTER PARTITION allowed type mismatches")
        print(f"   • No validation of partition spec against column types")
        print(f"   • Could create data integrity issues")
        print(f"   • Required manual type checking by developers")


if __name__ == "__main__":
    main()