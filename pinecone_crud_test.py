#!/usr/bin/env python3
"""
Simple Pinecone Vector Database CRUD Operations Test Class
This class demonstrates basic CRUD operations with Pinecone vector database.
"""

import os
import time
import random
from typing import List, Dict, Any, Optional
from pinecone import Pinecone, ServerlessSpec


class PineconeCRUDTest:
    """Simple class to test Pinecone vector database CRUD operations."""
    
    def __init__(self, api_key: str, environment: str = "us-east-1"):
        """
        Initialize Pinecone client.
        
        Args:
            api_key: Pinecone API key
            environment: Pinecone environment (default: us-east-1)
        """
        self.api_key = api_key
        self.environment = environment
        self.pc = Pinecone(api_key=api_key)
        self.index = None
        self.index_name = "developer-quickstart-py"
        
    def create_index(self, dimension: int = 128, metric: str = "cosine") -> bool:
        """
        Create a new Pinecone index.
        
        Args:
            dimension: Vector dimension
            metric: Distance metric (cosine, euclidean, dotproduct)
            
        Returns:
            bool: True if successful
        """
        try:
            # Check if index already exists
            if self.index_name in self.pc.list_indexes().names():
                print(f"Index '{self.index_name}' already exists. Deleting it first...")
                self.pc.delete_index(self.index_name)
                time.sleep(5)  # Wait for deletion to complete
            
            # Create new index
            print(f"Creating index '{self.index_name}' with dimension {dimension}...")
            self.pc.create_index(
                name=self.index_name,
                dimension=dimension,
                metric=metric,
                spec=ServerlessSpec(
                    cloud='aws',
                    region=self.environment
                )
            )
            
            # Wait for index to be ready
            print("Waiting for index to be ready...")
            while not self.pc.describe_index(self.index_name).status['ready']:
                time.sleep(1)
            
            self.index = self.pc.Index(self.index_name)
            print(f"Index '{self.index_name}' created successfully!")
            return True
            
        except Exception as e:
            print(f"Error creating index: {e}")
            return False
    
    def generate_sample_vectors(self, count: int = 5, dimension: int = 128) -> List[Dict[str, Any]]:
        """
        Generate sample vectors for testing.
        
        Args:
            count: Number of vectors to generate
            dimension: Vector dimension
            
        Returns:
            List of vector dictionaries
        """
        vectors = []
        for i in range(count):
            vector = {
                "id": f"vec_{i}",
                "values": [random.random() for _ in range(dimension)],
                "metadata": {
                    "category": f"category_{i % 3}",
                    "description": f"Sample vector {i}",
                    "timestamp": time.time()
                }
            }
            vectors.append(vector)
        return vectors
    
    def upsert_vectors(self, vectors: List[Dict[str, Any]]) -> bool:
        """
        Insert or update vectors in the index.
        
        Args:
            vectors: List of vector dictionaries
            
        Returns:
            bool: True if successful
        """
        try:
            if not self.index:
                print("Index not initialized. Please create an index first.")
                return False
            
            print(f"Upserting {len(vectors)} vectors...")
            response = self.index.upsert(vectors=vectors)
            print(f"Upserted {response['upserted_count']} vectors successfully!")
            return True
            
        except Exception as e:
            print(f"Error upserting vectors: {e}")
            return False
    
    def query_vectors(self, query_vector: List[float], top_k: int = 3, 
                     include_metadata: bool = True) -> Optional[Dict[str, Any]]:
        """
        Query similar vectors from the index.
        
        Args:
            query_vector: Query vector
            top_k: Number of similar vectors to return
            include_metadata: Whether to include metadata in results
            
        Returns:
            Query results or None if error
        """
        try:
            if not self.index:
                print("Index not initialized. Please create an index first.")
                return None
            
            print(f"Querying for top {top_k} similar vectors...")
            response = self.index.query(
                vector=query_vector,
                top_k=top_k,
                include_metadata=include_metadata,
                include_values=True
            )
            
            print(f"Found {len(response['matches'])} matches:")
            for i, match in enumerate(response['matches']):
                print(f"  {i+1}. ID: {match['id']}, Score: {match['score']:.4f}")
                if include_metadata and 'metadata' in match:
                    print(f"     Metadata: {match['metadata']}")
            
            return response
            
        except Exception as e:
            print(f"Error querying vectors: {e}")
            return None
    
    def fetch_vectors(self, ids: List[str]) -> Optional[Dict[str, Any]]:
        """
        Fetch specific vectors by their IDs.
        
        Args:
            ids: List of vector IDs to fetch
            
        Returns:
            Fetch results or None if error
        """
        try:
            if not self.index:
                print("Index not initialized. Please create an index first.")
                return None
            
            print(f"Fetching vectors with IDs: {ids}")
            response = self.index.fetch(ids=ids)
            
            print(f"Fetched {len(response['vectors'])} vectors:")
            for vector_id, vector_data in response['vectors'].items():
                print(f"  ID: {vector_id}")
                if 'metadata' in vector_data:
                    print(f"  Metadata: {vector_data['metadata']}")
            
            return response
            
        except Exception as e:
            print(f"Error fetching vectors: {e}")
            return None
    
    def update_vector(self, vector_id: str, values: Optional[List[float]] = None, 
                     metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        Update a specific vector's values or metadata.
        
        Args:
            vector_id: ID of the vector to update
            values: New vector values (optional)
            metadata: New metadata (optional)
            
        Returns:
            bool: True if successful
        """
        try:
            if not self.index:
                print("Index not initialized. Please create an index first.")
                return False
            
            print(f"Updating vector '{vector_id}'...")
            update_data = {"id": vector_id}
            
            if values:
                update_data["values"] = values
            if metadata:
                update_data["set_metadata"] = metadata
            
            self.index.update(**update_data)
            print(f"Vector '{vector_id}' updated successfully!")
            return True
            
        except Exception as e:
            print(f"Error updating vector: {e}")
            return False
    
    def delete_vectors(self, ids: List[str]) -> bool:
        """
        Delete specific vectors by their IDs.
        
        Args:
            ids: List of vector IDs to delete
            
        Returns:
            bool: True if successful
        """
        try:
            if not self.index:
                print("Index not initialized. Please create an index first.")
                return False
            
            print(f"Deleting vectors with IDs: {ids}")
            self.index.delete(ids=ids)
            print(f"Deleted {len(ids)} vectors successfully!")
            return True
            
        except Exception as e:
            print(f"Error deleting vectors: {e}")
            return False
    
    def get_index_stats(self) -> Optional[Dict[str, Any]]:
        """
        Get index statistics.
        
        Returns:
            Index stats or None if error
        """
        try:
            if not self.index:
                print("Index not initialized. Please create an index first.")
                return None
            
            stats = self.index.describe_index_stats()
            print("Index Statistics:")
            print(f"  Total vectors: {stats.get('total_vector_count', 0)}")
            print(f"  Dimension: {stats.get('dimension', 0)}")
            if 'namespaces' in stats:
                print(f"  Namespaces: {list(stats['namespaces'].keys())}")
            
            return stats
            
        except Exception as e:
            print(f"Error getting index stats: {e}")
            return None
    
    def cleanup(self) -> bool:
        """
        Delete the test index to clean up resources.
        
        Returns:
            bool: True if successful
        """
        try:
            if self.index_name in self.pc.list_indexes().names():
                print(f"Deleting index '{self.index_name}'...")
                self.pc.delete_index(self.index_name)
                print("Index deleted successfully!")
                return True
            else:
                print("Index doesn't exist, nothing to clean up.")
                return True
                
        except Exception as e:
            print(f"Error cleaning up: {e}")
            return False


def main():
    """Main function to demonstrate CRUD operations."""
    # Initialize with your API key
    api_key = "pcsk_4kn9f1_7pTT2RoGerBQkmCKTMgUsR6BzKXvYwrYmLCdM3dEZCu58LRtmMz9yoJ4NgWpDjv"
    
    print("=== Pinecone CRUD Operations Test ===\n")
    
    # Create test instance
    crud_test = PineconeCRUDTest(api_key)
    
    try:
        # 1. Create Index
        print("1. Creating Index...")
        if not crud_test.create_index(dimension=128):
            return
        
        # 2. Generate and Insert Vectors
        print("\n2. Generating and Inserting Vectors...")
        sample_vectors = crud_test.generate_sample_vectors(count=5, dimension=128)
        if not crud_test.upsert_vectors(sample_vectors):
            return
        
        # Wait a moment for vectors to be indexed
        time.sleep(2)
        
        # 3. Get Index Stats
        print("\n3. Getting Index Statistics...")
        crud_test.get_index_stats()
        
        # 4. Query Similar Vectors
        print("\n4. Querying Similar Vectors...")
        query_vector = [random.random() for _ in range(128)]
        crud_test.query_vectors(query_vector, top_k=3)
        
        # 5. Fetch Specific Vectors
        print("\n5. Fetching Specific Vectors...")
        crud_test.fetch_vectors(["vec_0", "vec_1"])
        
        # 6. Update Vector
        print("\n6. Updating Vector...")
        new_metadata = {
            "category": "updated_category",
            "description": "Updated vector 0",
            "updated_at": time.time()
        }
        crud_test.update_vector("vec_0", metadata=new_metadata)
        
        # 7. Fetch Updated Vector
        print("\n7. Fetching Updated Vector...")
        crud_test.fetch_vectors(["vec_0"])
        
        # 8. Delete Some Vectors
        print("\n8. Deleting Vectors...")
        crud_test.delete_vectors(["vec_3", "vec_4"])
        
        # 9. Final Stats
        print("\n9. Final Index Statistics...")
        crud_test.get_index_stats()
        
    except KeyboardInterrupt:
        print("\nOperation interrupted by user.")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        # 10. Cleanup
        print("\n10. Cleaning Up...")
        crud_test.cleanup()
    
    print("\n=== Test Complete ===")


if __name__ == "__main__":
    main()