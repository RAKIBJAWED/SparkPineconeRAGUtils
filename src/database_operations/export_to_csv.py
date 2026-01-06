#!/usr/bin/env python3
"""
Export Migration Rules to CSV
This script downloads all migration rules from Pinecone database and exports them to CSV format.
"""

import csv
import json
import time
from datetime import datetime
from typing import Dict, Any, List, Optional
from pinecone import Pinecone
import os


class MigrationRuleExporter:
    """Class to export migration rules from Pinecone to CSV format."""
    
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
            print(f"🔗 Connected to existing index: {index_name}")
        except Exception as e:
            print(f"❌ Error connecting to index {index_name}: {e}")
            self.index = None
    
    def get_all_vector_ids(self) -> List[str]:
        """
        Get all vector IDs from the index.
        Note: This is a simplified approach. For large datasets, you'd need pagination.
        
        Returns:
            List of vector IDs
        """
        try:
            # Get index stats to understand the data
            stats = self.index.describe_index_stats()
            total_vectors = stats.get('total_vector_count', 0)
            
            print(f"📊 Found {total_vectors} vectors in the index")
            
            if total_vectors == 0:
                print("⚠️  No vectors found in the index")
                return []
            
            # For small datasets, we can use a dummy query to get all vectors
            # This approach works for small datasets (< 10,000 vectors)
            dummy_vector = [0.0] * 1024  # Create a dummy vector with same dimension
            
            # Query with a high top_k to get all vectors
            response = self.index.query(
                vector=dummy_vector,
                top_k=min(total_vectors, 10000),  # Pinecone limit is 10,000
                include_metadata=True
            )
            
            vector_ids = [match['id'] for match in response['matches']]
            print(f"✅ Retrieved {len(vector_ids)} vector IDs")
            
            return vector_ids
            
        except Exception as e:
            print(f"❌ Error getting vector IDs: {e}")
            return []
    
    def fetch_all_records(self) -> List[Dict[str, Any]]:
        """
        Fetch all migration rule records from Pinecone.
        
        Returns:
            List of migration rule records with metadata
        """
        try:
            if not self.index:
                print("❌ Index not available. Please check connection.")
                return []
            
            print("\n📥 Fetching all migration rule records...")
            
            # Get all vector IDs
            vector_ids = self.get_all_vector_ids()
            
            if not vector_ids:
                return []
            
            # Fetch vectors by ID (batch fetch)
            print(f"🔍 Fetching detailed records for {len(vector_ids)} vectors...")
            
            # Use query approach to get all records with metadata
            dummy_vector = [0.0] * 1024
            response = self.index.query(
                vector=dummy_vector,
                top_k=len(vector_ids),
                include_metadata=True
            )
            
            records = []
            for match in response['matches']:
                record = {
                    'id': match['id'],
                    'score': match.get('score', 0.0),
                    **match.get('metadata', {})
                }
                records.append(record)
            
            print(f"✅ Successfully fetched {len(records)} complete records")
            return records
            
        except Exception as e:
            print(f"❌ Error fetching records: {e}")
            return []
    
    def prepare_csv_data(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Prepare records for CSV export by flattening and formatting data.
        
        Args:
            records: List of raw records from Pinecone
            
        Returns:
            List of records formatted for CSV export
        """
        csv_records = []
        
        for record in records:
            # Create a flattened record for CSV
            csv_record = {
                'id': record.get('id', ''),
                'rule_name': record.get('rule_name', ''),
                'source_spark_version': record.get('source_spark_version', ''),
                'target_spark_version': record.get('target_spark_version', ''),
                'migration_type': record.get('migration_type', ''),
                'severity': record.get('severity', ''),
                'category': record.get('category', ''),
                'spark_rule': record.get('spark_rule', ''),
                'error_with_before_script': record.get('error_with_before_script', ''),
                'spark_doc_link': record.get('spark_doc_link', ''),
                'tags': ', '.join(record.get('tags', [])) if isinstance(record.get('tags'), list) else str(record.get('tags', '')),
                'created_at': record.get('created_at', ''),
                'before_script_length': len(str(record.get('before_script', ''))),
                'after_script_length': len(str(record.get('after_script', ''))),
                'before_script': record.get('before_script', ''),
                'after_script': record.get('after_script', ''),
                'relevance_score': record.get('score', 0.0)
            }
            
            csv_records.append(csv_record)
        
        return csv_records
    
    def export_to_csv(self, output_file: str = None) -> bool:
        """
        Export all migration rules to CSV file.
        
        Args:
            output_file: Output CSV file path. If None, generates timestamp-based name.
            
        Returns:
            bool: True if successful
        """
        try:
            # Generate output filename if not provided
            if output_file is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = f"migration_rules_export_{timestamp}.csv"
            
            print(f"\n📤 Exporting migration rules to CSV: {output_file}")
            
            # Fetch all records
            records = self.fetch_all_records()
            
            if not records:
                print("❌ No records to export")
                return False
            
            # Prepare data for CSV
            csv_data = self.prepare_csv_data(records)
            
            # Define CSV columns
            csv_columns = [
                'id',
                'rule_name',
                'source_spark_version',
                'target_spark_version',
                'migration_type',
                'severity',
                'category',
                'spark_rule',
                'error_with_before_script',
                'spark_doc_link',
                'tags',
                'created_at',
                'before_script_length',
                'after_script_length',
                'relevance_score',
                'before_script',
                'after_script'
            ]
            
            # Write to CSV
            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
                
                # Write header
                writer.writeheader()
                
                # Write data rows
                for record in csv_data:
                    writer.writerow(record)
            
            print(f"✅ Successfully exported {len(csv_data)} records to {output_file}")
            
            # Show file info
            file_size = os.path.getsize(output_file)
            print(f"📁 File size: {file_size:,} bytes ({file_size/1024:.1f} KB)")
            
            # Show summary
            print(f"\n📋 Export Summary:")
            print(f"   • Total records: {len(csv_data)}")
            print(f"   • CSV columns: {len(csv_columns)}")
            print(f"   • Output file: {output_file}")
            
            # Show sample of exported data
            if csv_data:
                print(f"\n📄 Sample Record:")
                sample = csv_data[0]
                for key, value in sample.items():
                    if key in ['before_script', 'after_script']:
                        print(f"   • {key}: {len(str(value))} characters")
                    elif len(str(value)) > 100:
                        print(f"   • {key}: {str(value)[:100]}...")
                    else:
                        print(f"   • {key}: {value}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error exporting to CSV: {e}")
            return False
    
    def export_summary_csv(self, output_file: str = None) -> bool:
        """
        Export a summary CSV without the full script content (for easier viewing).
        
        Args:
            output_file: Output CSV file path. If None, generates timestamp-based name.
            
        Returns:
            bool: True if successful
        """
        try:
            # Generate output filename if not provided
            if output_file is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = f"migration_rules_summary_{timestamp}.csv"
            
            print(f"\n📤 Exporting migration rules summary to CSV: {output_file}")
            
            # Fetch all records
            records = self.fetch_all_records()
            
            if not records:
                print("❌ No records to export")
                return False
            
            # Define summary CSV columns (without full scripts)
            csv_columns = [
                'id',
                'rule_name',
                'source_spark_version',
                'target_spark_version',
                'migration_type',
                'severity',
                'category',
                'spark_rule',
                'error_with_before_script',
                'spark_doc_link',
                'tags',
                'created_at',
                'before_script_length',
                'after_script_length',
                'relevance_score'
            ]
            
            # Prepare summary data
            csv_data = []
            for record in records:
                csv_record = {
                    'id': record.get('id', ''),
                    'rule_name': record.get('rule_name', ''),
                    'source_spark_version': record.get('source_spark_version', ''),
                    'target_spark_version': record.get('target_spark_version', ''),
                    'migration_type': record.get('migration_type', ''),
                    'severity': record.get('severity', ''),
                    'category': record.get('category', ''),
                    'spark_rule': record.get('spark_rule', ''),
                    'error_with_before_script': record.get('error_with_before_script', ''),
                    'spark_doc_link': record.get('spark_doc_link', ''),
                    'tags': ', '.join(record.get('tags', [])) if isinstance(record.get('tags'), list) else str(record.get('tags', '')),
                    'created_at': record.get('created_at', ''),
                    'before_script_length': len(str(record.get('before_script', ''))),
                    'after_script_length': len(str(record.get('after_script', ''))),
                    'relevance_score': record.get('score', 0.0)
                }
                csv_data.append(csv_record)
            
            # Write to CSV
            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
                
                # Write header
                writer.writeheader()
                
                # Write data rows
                for record in csv_data:
                    writer.writerow(record)
            
            print(f"✅ Successfully exported {len(csv_data)} summary records to {output_file}")
            
            # Show file info
            file_size = os.path.getsize(output_file)
            print(f"📁 File size: {file_size:,} bytes ({file_size/1024:.1f} KB)")
            
            return True
            
        except Exception as e:
            print(f"❌ Error exporting summary CSV: {e}")
            return False


def main():
    """Main function to demonstrate CSV export functionality."""
    # Initialize with your API key
    api_key = "pcsk_4kn9f1_7pTT2RoGerBQkmCKTMgUsR6BzKXvYwrYmLCdM3dEZCu58LRtmMz9yoJ4NgWpDjv"
    
    print("📥 Migration Rules CSV Exporter")
    print("="*50)
    
    # Create exporter instance
    exporter = MigrationRuleExporter(api_key, "developer-quickstart-py")
    
    try:
        # 1. Export full CSV with scripts
        print("\n1️⃣  Exporting full CSV (with scripts)...")
        success1 = exporter.export_to_csv("migration_rules_full.csv")
        
        if success1:
            print("✅ Full CSV export completed!")
        else:
            print("❌ Full CSV export failed!")
        
        # 2. Export summary CSV without scripts
        print("\n2️⃣  Exporting summary CSV (without scripts)...")
        success2 = exporter.export_summary_csv("migration_rules_summary.csv")
        
        if success2:
            print("✅ Summary CSV export completed!")
        else:
            print("❌ Summary CSV export failed!")
        
        # 3. Show available files
        print("\n3️⃣  Generated Files:")
        for filename in ["migration_rules_full.csv", "migration_rules_summary.csv"]:
            if os.path.exists(filename):
                size = os.path.getsize(filename)
                print(f"   📄 {filename} ({size:,} bytes)")
            else:
                print(f"   ❌ {filename} (not found)")
        
    except KeyboardInterrupt:
        print("\n⏹️  Export interrupted by user.")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    
    print("\n🏁 CSV Export Process Complete!")


if __name__ == "__main__":
    main()