#!/usr/bin/env python3
"""
Spark Migration RAG Database Class
This class manages Spark migration data in Pinecone for RAG-based LLM applications.
Stores migration patterns, scripts, and documentation for Spark version upgrades.
"""

import time
import hashlib
from typing import List, Dict, Any, Optional
from pinecone import Pinecone


class SparkMigrationRAG:
    """Class to manage Spark migration data in Pinecone for RAG applications."""
    
    def __init__(self, api_key: str, index_name: str = "developer-quickstart-py"):
        """
        Initialize Pinecone client with existing index.
        
        Args:
            api_key: Pinecone API key
            index_name: Name of existing Pinecone index
        """
        self.api_key = api_key
        self.index_name = index_name
        self.pc = Pinecone(api_key=api_key)
        
        # Connect to existing index
        try:
            self.index = self.pc.Index(index_name)
            print(f"Connected to existing index: {index_name}")
        except Exception as e:
            print(f"Error connecting to index {index_name}: {e}")
            self.index = None
    
    def generate_embedding(self, text: str, dimension: int = 1024) -> List[float]:
        """
        Generate a simple hash-based embedding for demonstration.
        In production, use a proper embedding model like OpenAI, Sentence Transformers, etc.
        
        Args:
            text: Text to embed
            dimension: Embedding dimension
            
        Returns:
            List of float values representing the embedding
        """
        # Create a hash of the text
        hash_obj = hashlib.md5(text.encode())
        hash_hex = hash_obj.hexdigest()
        
        # Convert hash to numbers and normalize
        embedding = []
        for i in range(0, len(hash_hex), 2):
            # Take pairs of hex digits and convert to float
            hex_pair = hash_hex[i:i+2]
            value = int(hex_pair, 16) / 255.0  # Normalize to 0-1
            embedding.append(value)
        
        # Extend or truncate to desired dimension
        while len(embedding) < dimension:
            embedding.extend(embedding[:min(len(embedding), dimension - len(embedding))])
        
        return embedding[:dimension]
    
    def create_sample_migration_data(self) -> List[Dict[str, Any]]:
        """
        Create 15 sample Spark migration records for RAG database.
        
        Returns:
            List of migration record dictionaries
        """
        sample_data = [
            {
                "before_script": "spark.sql.adaptive.enabled=false\nspark.sql.adaptive.coalescePartitions.enabled=false",
                "after_script": "spark.sql.adaptive.enabled=true\nspark.sql.adaptive.coalescePartitions.enabled=true\nspark.sql.adaptive.advisoryPartitionSizeInBytes=64MB",
                "error_with_before_script": "Poor performance with small files, excessive number of tasks",
                "source_spark_version": "2.4.0",
                "target_spark_version": "3.5.0",
                "spark_rule": "Enable Adaptive Query Execution (AQE) for better performance",
                "spark_doc_link": "https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution"
            },
            {
                "before_script": "df.rdd.map(lambda x: x.value).collect()",
                "after_script": "df.select('value').collect()",
                "error_with_before_script": "Unnecessary RDD conversion causing performance overhead",
                "source_spark_version": "2.4.0",
                "target_spark_version": "3.5.0",
                "spark_rule": "Use DataFrame API instead of RDD operations when possible",
                "spark_doc_link": "https://spark.apache.org/docs/latest/sql-programming-guide.html#datasets-and-dataframes"
            },
            {
                "before_script": "spark.sql.execution.arrow.pyspark.enabled=false",
                "after_script": "spark.sql.execution.arrow.pyspark.enabled=true\nspark.sql.execution.arrow.maxRecordsPerBatch=10000",
                "error_with_before_script": "Slow data transfer between JVM and Python processes",
                "source_spark_version": "2.4.0",
                "target_spark_version": "3.5.0",
                "spark_rule": "Enable Arrow-based columnar data transfers for PySpark",
                "spark_doc_link": "https://spark.apache.org/docs/latest/sql-pyspark-pandas-with-arrow.html"
            },
            {
                "before_script": "df.write.mode('overwrite').parquet('/path/to/output')",
                "after_script": "df.write.mode('overwrite').option('compression', 'snappy').parquet('/path/to/output')",
                "error_with_before_script": "Large file sizes without compression",
                "source_spark_version": "2.4.0",
                "target_spark_version": "3.5.0",
                "spark_rule": "Always specify compression for Parquet files",
                "spark_doc_link": "https://spark.apache.org/docs/latest/sql-data-sources-parquet.html"
            },
            {
                "before_script": "spark.sql.shuffle.partitions=200",
                "after_script": "spark.sql.shuffle.partitions=auto\nspark.sql.adaptive.enabled=true",
                "error_with_before_script": "Fixed partition count not optimal for all workloads",
                "source_spark_version": "2.4.0",
                "target_spark_version": "3.5.0",
                "spark_rule": "Use adaptive partitioning instead of fixed shuffle partitions",
                "spark_doc_link": "https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution"
            },
            {
                "before_script": "from pyspark.sql.functions import udf\n@udf\ndef custom_func(x):\n    return x.upper()",
                "after_script": "from pyspark.sql.functions import upper\ndf.select(upper(col('column_name')))",
                "error_with_before_script": "UDF serialization overhead and poor performance",
                "source_spark_version": "2.4.0",
                "target_spark_version": "3.5.0",
                "spark_rule": "Use built-in functions instead of UDFs when possible",
                "spark_doc_link": "https://spark.apache.org/docs/latest/sql-ref-functions.html"
            },
            {
                "before_script": "df.cache()\ndf.count()\nresult = df.filter(col('status') == 'active')",
                "after_script": "df.persist(StorageLevel.MEMORY_AND_DISK_SER)\ndf.count()\nresult = df.filter(col('status') == 'active')",
                "error_with_before_script": "Memory pressure with default MEMORY_ONLY storage level",
                "source_spark_version": "2.4.0",
                "target_spark_version": "3.5.0",
                "spark_rule": "Use appropriate storage levels for caching based on memory availability",
                "spark_doc_link": "https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence"
            },
            {
                "before_script": "spark.sql.broadcastTimeout=300s",
                "after_script": "spark.sql.broadcastTimeout=600s\nspark.sql.adaptive.autoBroadcastJoinThreshold=10MB",
                "error_with_before_script": "Broadcast timeouts in large datasets",
                "source_spark_version": "2.4.0",
                "target_spark_version": "3.5.0",
                "spark_rule": "Increase broadcast timeout and use adaptive broadcast join threshold",
                "spark_doc_link": "https://spark.apache.org/docs/latest/sql-performance-tuning.html#broadcast-hint"
            },
            {
                "before_script": "df.write.format('delta').mode('append').save('/path/to/delta')",
                "after_script": "df.write.format('delta').mode('append').option('mergeSchema', 'true').save('/path/to/delta')",
                "error_with_before_script": "Schema evolution issues when appending data with new columns",
                "source_spark_version": "3.0.0",
                "target_spark_version": "3.5.0",
                "spark_rule": "Enable schema merging for Delta tables when schema evolution is expected",
                "spark_doc_link": "https://docs.delta.io/latest/delta-batch.html#schema-enforcement"
            },
            {
                "before_script": "spark.sql.execution.dynamicAllocation.enabled=false",
                "after_script": "spark.sql.execution.dynamicAllocation.enabled=true\nspark.dynamicAllocation.minExecutors=1\nspark.dynamicAllocation.maxExecutors=20",
                "error_with_before_script": "Resource waste with fixed executor allocation",
                "source_spark_version": "2.4.0",
                "target_spark_version": "3.5.0",
                "spark_rule": "Enable dynamic allocation for better resource utilization",
                "spark_doc_link": "https://spark.apache.org/docs/latest/job-scheduling.html#dynamic-resource-allocation"
            },
            {
                "before_script": "df.join(other_df, 'key', 'inner')",
                "after_script": "df.join(broadcast(other_df), 'key', 'inner')",
                "error_with_before_script": "Expensive shuffle joins for small tables",
                "source_spark_version": "2.4.0",
                "target_spark_version": "3.5.0",
                "spark_rule": "Use broadcast joins for small tables to avoid shuffling",
                "spark_doc_link": "https://spark.apache.org/docs/latest/sql-performance-tuning.html#broadcast-hint"
            },
            {
                "before_script": "spark.sql.execution.arrow.maxRecordsPerBatch=1000",
                "after_script": "spark.sql.execution.arrow.maxRecordsPerBatch=10000\nspark.sql.execution.arrow.fallback.enabled=true",
                "error_with_before_script": "Poor Arrow performance with small batch sizes",
                "source_spark_version": "3.0.0",
                "target_spark_version": "3.5.0",
                "spark_rule": "Optimize Arrow batch size and enable fallback for compatibility",
                "spark_doc_link": "https://spark.apache.org/docs/latest/sql-pyspark-pandas-with-arrow.html#arrow-optimization"
            },
            {
                "before_script": "df.write.partitionBy('year', 'month', 'day').parquet('/path')",
                "after_script": "df.write.partitionBy('year', 'month').parquet('/path')",
                "error_with_before_script": "Too many small partitions causing metadata overhead",
                "source_spark_version": "2.4.0",
                "target_spark_version": "3.5.0",
                "spark_rule": "Avoid over-partitioning to prevent small file problems",
                "spark_doc_link": "https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#partition-discovery"
            },
            {
                "before_script": "spark.sql.adaptive.skewJoin.enabled=false",
                "after_script": "spark.sql.adaptive.skewJoin.enabled=true\nspark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=256MB",
                "error_with_before_script": "Poor performance due to data skew in joins",
                "source_spark_version": "3.0.0",
                "target_spark_version": "3.5.0",
                "spark_rule": "Enable skew join optimization to handle data skew",
                "spark_doc_link": "https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution"
            },
            {
                "before_script": "df.coalesce(1).write.csv('/path/to/output')",
                "after_script": "df.repartition(4).write.csv('/path/to/output')",
                "error_with_before_script": "Single partition causing bottleneck in write operations",
                "source_spark_version": "2.4.0",
                "target_spark_version": "3.5.0",
                "spark_rule": "Use appropriate number of partitions for output instead of coalesce(1)",
                "spark_doc_link": "https://spark.apache.org/docs/latest/rdd-programming-guide.html#repartition-vs-coalesce"
            }
        ]
        
        # Convert to Pinecone format with embeddings
        vectors = []
        for i, record in enumerate(sample_data):
            # Create a combined text for embedding
            combined_text = f"""
            Migration Rule: {record['spark_rule']}
            Source Version: {record['source_spark_version']}
            Target Version: {record['target_spark_version']}
            Before Script: {record['before_script']}
            After Script: {record['after_script']}
            Error: {record['error_with_before_script']}
            """.strip()
            
            # Generate embedding (in production, use a proper embedding model)
            embedding = self.generate_embedding(combined_text)
            
            vector = {
                "id": f"spark_migration_{i+1}",
                "values": embedding,
                "metadata": {
                    "before_script": record["before_script"],
                    "after_script": record["after_script"],
                    "error_with_before_script": record["error_with_before_script"],
                    "source_spark_version": record["source_spark_version"],
                    "target_spark_version": record["target_spark_version"],
                    "spark_rule": record["spark_rule"],
                    "spark_doc_link": record["spark_doc_link"],
                    "migration_type": "performance_optimization",
                    "created_at": time.time()
                }
            }
            vectors.append(vector)
        
        return vectors
    
    def insert_migration_data(self) -> bool:
        """
        Insert sample migration data into the Pinecone index.
        
        Returns:
            bool: True if successful
        """
        try:
            if not self.index:
                print("Index not available. Please check connection.")
                return False
            
            print("Generating 15 sample Spark migration records...")
            vectors = self.create_sample_migration_data()
            
            print(f"Inserting {len(vectors)} migration records into index '{self.index_name}'...")
            response = self.index.upsert(vectors=vectors)
            print(f"Successfully inserted {response['upserted_count']} records!")
            
            return True
            
        except Exception as e:
            print(f"Error inserting migration data: {e}")
            return False
    
    def search_migration_patterns(self, query: str, top_k: int = 5) -> Optional[List[Dict[str, Any]]]:
        """
        Search for relevant migration patterns based on query.
        
        Args:
            query: Search query describing the migration issue
            top_k: Number of results to return
            
        Returns:
            List of relevant migration patterns
        """
        try:
            if not self.index:
                print("Index not available. Please check connection.")
                return None
            
            # Generate embedding for query
            query_embedding = self.generate_embedding(query)
            
            print(f"Searching for migration patterns related to: '{query}'")
            response = self.index.query(
                vector=query_embedding,
                top_k=top_k,
                include_metadata=True
            )
            
            results = []
            print(f"\nFound {len(response['matches'])} relevant migration patterns:")
            for i, match in enumerate(response['matches']):
                metadata = match['metadata']
                result = {
                    "id": match['id'],
                    "score": match['score'],
                    "spark_rule": metadata.get('spark_rule', ''),
                    "source_version": metadata.get('source_spark_version', ''),
                    "target_version": metadata.get('target_spark_version', ''),
                    "before_script": metadata.get('before_script', ''),
                    "after_script": metadata.get('after_script', ''),
                    "error_description": metadata.get('error_with_before_script', ''),
                    "documentation": metadata.get('spark_doc_link', '')
                }
                results.append(result)
                
                print(f"\n{i+1}. {metadata.get('spark_rule', 'Unknown Rule')} (Score: {match['score']:.4f})")
                print(f"   Versions: {metadata.get('source_spark_version', '')} → {metadata.get('target_spark_version', '')}")
                print(f"   Error: {metadata.get('error_with_before_script', '')[:100]}...")
            
            return results
            
        except Exception as e:
            print(f"Error searching migration patterns: {e}")
            return None
    
    def get_migration_by_version(self, source_version: str, target_version: str) -> Optional[List[Dict[str, Any]]]:
        """
        Get migration patterns for specific version upgrade.
        
        Args:
            source_version: Source Spark version
            target_version: Target Spark version
            
        Returns:
            List of migration patterns for the version upgrade
        """
        try:
            if not self.index:
                print("Index not available. Please check connection.")
                return None
            
            # Query with version-specific text
            query = f"migrate from Spark {source_version} to {target_version}"
            return self.search_migration_patterns(query, top_k=10)
            
        except Exception as e:
            print(f"Error getting migration by version: {e}")
            return None
    
    def get_index_stats(self) -> Optional[Dict[str, Any]]:
        """
        Get statistics about the migration data in the index.
        
        Returns:
            Index statistics
        """
        try:
            if not self.index:
                print("Index not available. Please check connection.")
                return None
            
            stats = self.index.describe_index_stats()
            print("Migration RAG Database Statistics:")
            print(f"  Total migration patterns: {stats.get('total_vector_count', 0)}")
            print(f"  Vector dimension: {stats.get('dimension', 0)}")
            if 'namespaces' in stats:
                print(f"  Namespaces: {list(stats['namespaces'].keys())}")
            
            return stats
            
        except Exception as e:
            print(f"Error getting index stats: {e}")
            return None


def main():
    """Main function to demonstrate Spark Migration RAG functionality."""
    # Initialize with your API key and existing index
    api_key = "pcsk_4kn9f1_7pTT2RoGerBQkmCKTMgUsR6BzKXvYwrYmLCdM3dEZCu58LRtmMz9yoJ4NgWpDjv"
    
    print("=== Spark Migration RAG Database ===\n")
    
    # Create RAG instance
    rag = SparkMigrationRAG(api_key, "developer-quickstart-py")
    
    try:
        # 1. Insert Migration Data
        # print("1. Inserting Spark Migration Data...")
        # if not rag.insert_migration_data():
        #    return
        
        # Wait for indexing
        # time.sleep(3)
        
        # 2. Get Index Stats
        print("\n2. Getting Database Statistics...")
        rag.get_index_stats()
        
        # 3. Search for Performance Issues
        print("\n3. Searching for Performance Optimization Patterns...")
        rag.search_migration_patterns("performance issues with small files", top_k=3)
        
        # 4. Search for Specific Version Migration
        print("\n4. Searching for Spark 2.4 to 3.5 Migration...")
        rag.get_migration_by_version("2.4.0", "3.5.0")
        
        # 5. Search for Join Optimization
        print("\n5. Searching for Join Optimization Patterns...")
        rag.search_migration_patterns("join optimization broadcast", top_k=2)
        
    except KeyboardInterrupt:
        print("\nOperation interrupted by user.")
    except Exception as e:
        print(f"Unexpected error: {e}")
    
    print("\n=== RAG Database Setup Complete ===")


if __name__ == "__main__":
    main()