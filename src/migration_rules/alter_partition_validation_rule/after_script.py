#!/usr/bin/env python3
"""
ALTER PARTITION Type Validation Migration - AFTER Script (Spark 3.4+)
This script demonstrates the updated behavior with strict type validation.

Language: Python
Usage: python after_script.py

This script can be run independently to test the updated behavior.
In Spark 3.4+, ALTER PARTITION validates partition spec types against column types.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import tempfile
import shutil
import os


def main():
    # Create SparkSession with Spark 3.4+ behavior (strict validation)
    spark = SparkSession.builder \
        .appName('AlterPartitionValidationAfter') \
        .master('local[*]') \
        .config('spark.sql.legacy.skipTypeValidationOnAlterPartition', 'false') \
        .config('spark.sql.warehouse.dir', 'spark-warehouse') \
        .getOrCreate()
    
    print("=== ALTER PARTITION Type Validation - AFTER (Spark 3.4+ behavior) ===")
    print("Configuration: spark.sql.legacy.skipTypeValidationOnAlterPartition = false")
    
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
        
        # Demonstrate the NEW behavior - type validation prevents mismatches
        print("\n🔍 ATTEMPTING: ALTER TABLE ADD PARTITION with string value for int column...")
        print("   SQL: ALTER TABLE test_partition_table ADD PARTITION (partition_col='300')")
        
        try:
            # This will fail in Spark 3.4+ due to type validation
            spark.sql("ALTER TABLE test_partition_table ADD PARTITION (partition_col='300')")
            print("❌ UNEXPECTED: Partition was added (this shouldn't happen in Spark 3.4+)")
            
        except Exception as e:
            print(f"✅ EXPECTED FAILURE: {e}")
            print("   🎉 Type validation prevented the type mismatch!")
            print("   💡 This protects data integrity")
        
        # Demonstrate the CORRECT way - using proper types
        print(f"\n✅ CORRECT APPROACH: Adding partition with proper integer value...")
        print("   SQL: ALTER TABLE test_partition_table ADD PARTITION (partition_col=300)")
        
        try:
            spark.sql("ALTER TABLE test_partition_table ADD PARTITION (partition_col=300)")
            print("✅ SUCCESS: Partition added with correct integer value")
            
            # Show updated partitions
            print("\n📋 Partitions after adding correct partition:")
            spark.sql("SHOW PARTITIONS test_partition_table").show()
            
        except Exception as e:
            print(f"❌ Unexpected error with correct type: {e}")
        
        # Demonstrate another type validation scenario
        print(f"\n🔍 ATTEMPTING: Adding partition with completely invalid string...")
        print("   SQL: ALTER TABLE test_partition_table ADD PARTITION (partition_col='invalid_string')")
        
        try:
            spark.sql("ALTER TABLE test_partition_table ADD PARTITION (partition_col='invalid_string')")
            print("❌ UNEXPECTED: Invalid partition was added")
            
        except Exception as e:
            print(f"✅ EXPECTED FAILURE: {e}")
            print("   🎉 Type validation prevented invalid data!")
        
        # Show how to handle convertible strings
        print(f"\n💡 BEST PRACTICE: Using convertible string values...")
        print("   SQL: ALTER TABLE test_partition_table ADD PARTITION (partition_col=400)")
        
        try:
            # Use integer literal instead of string
            spark.sql("ALTER TABLE test_partition_table ADD PARTITION (partition_col=400)")
            print("✅ SUCCESS: Partition added with integer literal")
            
        except Exception as e:
            print(f"❌ Error: {e}")
        
        # Demonstrate programmatic approach for dynamic partition addition
        print(f"\n🔧 PROGRAMMATIC APPROACH: Adding partitions with proper type casting...")
        
        try:
            # When adding partitions programmatically, ensure proper types
            partition_value = 500  # Integer value
            spark.sql(f"ALTER TABLE test_partition_table ADD PARTITION (partition_col={partition_value})")
            print(f"✅ SUCCESS: Added partition with value {partition_value}")
            
        except Exception as e:
            print(f"❌ Error: {e}")
        
        # Final partition list
        print(f"\n📊 Final partition list:")
        spark.sql("SHOW PARTITIONS test_partition_table").show()
        
        # Query all data to verify integrity
        print(f"\n🔍 Querying all data to verify integrity:")
        result = spark.sql("SELECT * FROM test_partition_table ORDER BY id")
        result.show()
        
        print(f"\n🎯 MIGRATION RECOMMENDATIONS:")
        print(f"   ✅ Always use correct data types in partition specs")
        print(f"   ✅ Validate partition values before ALTER PARTITION commands")
        print(f"   ✅ Use integer literals for integer partition columns")
        print(f"   ✅ Test partition operations in development environment")
        print(f"   ⚠️  If legacy behavior is absolutely needed, set:")
        print(f"      spark.sql.legacy.skipTypeValidationOnAlterPartition=true")
        
    except Exception as e:
        print(f"❌ Error during demonstration: {e}")
        print("   Check your Spark configuration and table setup")
    
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
        
        print(f"\n🎉 SUMMARY - Spark 3.4+ Benefits:")
        print(f"   • Strict type validation prevents data integrity issues")
        print(f"   • Early error detection for type mismatches")
        print(f"   • Consistent behavior with spark.sql.storeAssignmentPolicy")
        print(f"   • Better data quality and reliability")


if __name__ == "__main__":
    main()