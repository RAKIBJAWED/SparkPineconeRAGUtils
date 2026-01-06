#!/usr/bin/env python3
"""
Simple Apache Spark Example in Python
This example demonstrates basic Spark operations including:
- Creating a SparkSession
- Working with RDDs and DataFrames
- Basic transformations and actions
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max as spark_max

def main():
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("SimpleSparkExample") \
        .master("local[*]") \
        .getOrCreate()
    
    print("Spark session created successfully!")
    print(f"Spark version: {spark.version}")
    
    # Example 1: Working with RDDs
    print("\n=== RDD Example ===")
    numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    rdd = spark.sparkContext.parallelize(numbers)
    
    # Transformations and actions
    squared_rdd = rdd.map(lambda x: x ** 2)
    even_squares = squared_rdd.filter(lambda x: x % 2 == 0)
    result = even_squares.collect()
    
    print(f"Original numbers: {numbers}")
    print(f"Even squares: {result}")
    print(f"Sum of even squares: {even_squares.sum()}")
    
    # Example 2: Working with DataFrames
    print("\n=== DataFrame Example ===")
    
    # Create sample data
    data = [
        ("Alice", 25, "Engineer", 75000),
        ("Bob", 30, "Manager", 85000),
        ("Charlie", 35, "Engineer", 70000),
        ("Diana", 28, "Analyst", 65000),
        ("Eve", 32, "Manager", 90000)
    ]
    
    columns = ["name", "age", "job", "salary"]
    df = spark.createDataFrame(data, columns)
    
    print("Sample DataFrame:")
    df.show()
    
    # DataFrame operations
    print("\nDataFrame operations:")
    
    # Filter employees older than 28
    older_employees = df.filter(col("age") > 28)
    print("Employees older than 28:")
    older_employees.show()
    
    # Group by job and calculate average salary
    avg_salary_by_job = df.groupBy("job").agg(
        count("*").alias("count"),
        avg("salary").alias("avg_salary"),
        spark_max("salary").alias("max_salary")
    )
    print("Average salary by job:")
    avg_salary_by_job.show()
    
    # Select specific columns and add a new column
    df_with_bonus = df.select("name", "salary") \
        .withColumn("bonus", col("salary") * 0.1)
    print("Employees with 10% bonus:")
    df_with_bonus.show()
    
    # Example 3: Reading and writing data (commented out as files don't exist)
    print("\n=== File I/O Example (commented) ===")
    print("# To read from CSV:")
    print("# df_csv = spark.read.csv('data.csv', header=True, inferSchema=True)")
    print("# To write to Parquet:")
    print("# df.write.mode('overwrite').parquet('output.parquet')")
    
    # Stop the Spark session
    spark.stop()
    print("\nSpark session stopped.")

if __name__ == "__main__":
    main()