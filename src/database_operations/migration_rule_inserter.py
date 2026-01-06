#!/usr/bin/env python3
"""
Migration Rule Database Inserter
This class handles inserting migration rules from structured folders into Pinecone database.
It dynamically loads rule configurations and associated script files.
"""

import json
import time
import hashlib
import os
from typing import Dict, Any, List, Optional
from pinecone import Pinecone


class MigrationRuleInserter:
    """Class to insert migration rules from structured folders into Pinecone."""
    
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
    
    def load_script_file(self, file_path: str) -> Optional[str]:
        """
        Load Python script content from file.
        
        Args:
            file_path: Path to the Python script file
            
        Returns:
            String containing the script content or None if error
        """
        try:
            if not os.path.exists(file_path):
                print(f"⚠️  Script file not found: {file_path}")
                return None
                
            with open(file_path, 'r', encoding='utf-8') as file:
                script_content = file.read()
                print(f"✅ Loaded script from {file_path} ({len(script_content)} chars)")
                return script_content
        except Exception as e:
            print(f"❌ Error loading script file {file_path}: {e}")
            return None
    
    def load_rule_from_folder(self, rule_folder_path: str) -> Optional[Dict[str, Any]]:
        """
        Load migration rule from a structured folder containing rule_config.json and script files.
        
        Args:
            rule_folder_path: Path to the rule folder
            
        Returns:
            Dictionary containing the rule data with loaded scripts or None if error
        """
        try:
            config_file = os.path.join(rule_folder_path, 'rule_config.json')
            
            if not os.path.exists(config_file):
                print(f"❌ Rule config file not found: {config_file}")
                return None
            
            # Load rule configuration
            with open(config_file, 'r', encoding='utf-8') as file:
                rule_data = json.load(file)
                print(f"📋 Loaded rule config: {rule_data.get('rule_name', 'Unknown Rule')}")
            
            # Load before script from standard location
            before_script_path = os.path.join(rule_folder_path, 'before_script.py')
            if os.path.exists(before_script_path):
                before_script = self.load_script_file(before_script_path)
                if before_script:
                    rule_data['before_script'] = before_script
                    print(f"✅ Loaded before script ({len(before_script)} chars)")
                else:
                    print(f"⚠️  Failed to load before script from {before_script_path}")
            else:
                print(f"⚠️  Before script not found at {before_script_path}")
            
            # Load after script from standard location
            after_script_path = os.path.join(rule_folder_path, 'after_script.py')
            if os.path.exists(after_script_path):
                after_script = self.load_script_file(after_script_path)
                if after_script:
                    rule_data['after_script'] = after_script
                    print(f"✅ Loaded after script ({len(after_script)} chars)")
                else:
                    print(f"⚠️  Failed to load after script from {after_script_path}")
            else:
                print(f"⚠️  After script not found at {after_script_path}")
            
            return rule_data
            
        except FileNotFoundError:
            print(f"❌ Rule folder not found: {rule_folder_path}")
            return None
        except json.JSONDecodeError as e:
            print(f"❌ Error parsing rule config JSON: {e}")
            return None
        except Exception as e:
            print(f"❌ Unexpected error loading rule from folder: {e}")
            return None
    
    def generate_embedding(self, text: str, dimension: int = 1024) -> List[float]:
        """
        Generate a hash-based embedding for the rule text.
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
    
    def create_vector_from_rule(self, rule_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert rule data to Pinecone vector format.
        
        Args:
            rule_data: Dictionary containing the migration rule data
            
        Returns:
            Vector dictionary ready for Pinecone insertion
        """
        # Create combined text for embedding
        combined_text = f"""
        Rule: {rule_data.get('spark_rule', '')}
        Migration: {rule_data.get('rule_name', '')}
        Source Version: {rule_data.get('source_spark_version', '')}
        Target Version: {rule_data.get('target_spark_version', '')}
        Before Script: {rule_data.get('before_script', '')[:500]}
        After Script: {rule_data.get('after_script', '')[:500]}
        Error: {rule_data.get('error_with_before_script', '')}
        Category: {rule_data.get('category', '')}
        Tags: {' '.join(rule_data.get('tags', []))}
        """.strip()
        
        # Generate embedding
        embedding = self.generate_embedding(combined_text)
        
        # Create vector with metadata
        vector = {
            "id": rule_data.get('rule_id', f"rule_{int(time.time())}"),
            "values": embedding,
            "metadata": {
                "rule_name": rule_data.get('rule_name', ''),
                "before_script": rule_data.get('before_script', ''),
                "after_script": rule_data.get('after_script', ''),
                "error_with_before_script": rule_data.get('error_with_before_script', ''),
                "source_spark_version": rule_data.get('source_spark_version', ''),
                "target_spark_version": rule_data.get('target_spark_version', ''),
                "spark_rule": rule_data.get('spark_rule', ''),
                "spark_doc_link": rule_data.get('spark_doc_link', ''),
                "migration_type": rule_data.get('migration_type', ''),
                "severity": rule_data.get('severity', 'medium'),
                "category": rule_data.get('category', ''),
                "tags": rule_data.get('tags', []),
                "created_at": rule_data.get('created_at', time.time())
            }
        }
        
        return vector
    
    def test_migration_scripts(self, rule_data: Dict[str, Any]) -> bool:
        """
        Test both before and after scripts to ensure they are valid Python code.
        
        Args:
            rule_data: Dictionary containing the migration rule data
            
        Returns:
            bool: True if both scripts are valid Python code
        """
        try:
            print("\n🧪 Testing migration scripts...")
            
            # Test before script
            before_script = rule_data.get('before_script', '')
            if before_script:
                try:
                    compile(before_script, '<before_script>', 'exec')
                    print("✅ Before script: Valid Python syntax")
                except SyntaxError as e:
                    print(f"❌ Before script: Syntax error - {e}")
                    return False
            else:
                print("⚠️  No before script found")
            
            # Test after script
            after_script = rule_data.get('after_script', '')
            if after_script:
                try:
                    compile(after_script, '<after_script>', 'exec')
                    print("✅ After script: Valid Python syntax")
                except SyntaxError as e:
                    print(f"❌ After script: Syntax error - {e}")
                    return False
            else:
                print("⚠️  No after script found")
            
            print("✅ All scripts passed syntax validation")
            return True
            
        except Exception as e:
            print(f"❌ Error testing scripts: {e}")
            return False
    
    def insert_rule_from_folder(self, rule_folder_path: str, test_scripts: bool = True) -> bool:
        """
        Load rule from folder and insert into Pinecone index.
        
        Args:
            rule_folder_path: Path to the rule folder
            test_scripts: Whether to test script syntax before insertion
            
        Returns:
            bool: True if successful
        """
        try:
            if not self.index:
                print("❌ Index not available. Please check connection.")
                return False
            
            print(f"\n📁 Processing rule folder: {rule_folder_path}")
            
            # Load rule from folder (with dynamic script loading)
            rule_data = self.load_rule_from_folder(rule_folder_path)
            if not rule_data:
                return False
            
            # Test scripts if requested
            if test_scripts:
                if not self.test_migration_scripts(rule_data):
                    print("❌ Script validation failed. Aborting insertion.")
                    return False
            
            # Create vector from rule data
            vector = self.create_vector_from_rule(rule_data)
            
            print(f"\n📝 Inserting migration rule: {rule_data.get('rule_name', 'Unknown Rule')}")
            print(f"   🆔 Rule ID: {vector['id']}")
            print(f"   📊 Versions: {rule_data.get('source_spark_version', 'N/A')} → {rule_data.get('target_spark_version', 'N/A')}")
            print(f"   🏷️  Category: {rule_data.get('category', 'N/A')} | Severity: {rule_data.get('severity', 'N/A')}")
            print(f"   🏷️  Tags: {', '.join(rule_data.get('tags', []))}")
            
            # Show script information
            before_script_len = len(rule_data.get('before_script', ''))
            after_script_len = len(rule_data.get('after_script', ''))
            print(f"   📄 Before script: {before_script_len} characters")
            print(f"   📄 After script: {after_script_len} characters")
            
            # Insert vector into Pinecone
            response = self.index.upsert(vectors=[vector])
            
            if response.get('upserted_count', 0) > 0:
                print(f"✅ Successfully inserted rule into index '{self.index_name}'!")
                print(f"   📈 Upserted count: {response['upserted_count']}")
                return True
            else:
                print("❌ Failed to insert rule - no vectors were upserted")
                return False
                
        except Exception as e:
            print(f"❌ Error inserting rule from folder: {e}")
            return False
    
    def insert_all_rules_from_directory(self, rules_directory: str, test_scripts: bool = True) -> Dict[str, bool]:
        """
        Insert all migration rules from a directory containing rule folders.
        
        Args:
            rules_directory: Path to directory containing rule folders
            test_scripts: Whether to test script syntax before insertion
            
        Returns:
            Dictionary mapping rule folder names to insertion success status
        """
        results = {}
        
        try:
            if not os.path.exists(rules_directory):
                print(f"❌ Rules directory not found: {rules_directory}")
                return results
            
            print(f"\n📂 Scanning rules directory: {rules_directory}")
            
            # Get all subdirectories (rule folders)
            rule_folders = [f for f in os.listdir(rules_directory) 
                          if os.path.isdir(os.path.join(rules_directory, f))]
            
            if not rule_folders:
                print("⚠️  No rule folders found in directory")
                return results
            
            print(f"📋 Found {len(rule_folders)} rule folders: {', '.join(rule_folders)}")
            
            # Process each rule folder
            for rule_folder in rule_folders:
                rule_folder_path = os.path.join(rules_directory, rule_folder)
                print(f"\n{'='*60}")
                print(f"Processing rule: {rule_folder}")
                print(f"{'='*60}")
                
                success = self.insert_rule_from_folder(rule_folder_path, test_scripts)
                results[rule_folder] = success
                
                if success:
                    print(f"✅ {rule_folder}: Successfully inserted")
                else:
                    print(f"❌ {rule_folder}: Failed to insert")
                
                # Small delay between insertions
                time.sleep(1)
            
            # Summary
            successful = sum(1 for success in results.values() if success)
            total = len(results)
            
            print(f"\n{'='*60}")
            print(f"📊 INSERTION SUMMARY")
            print(f"{'='*60}")
            print(f"✅ Successful: {successful}/{total}")
            print(f"❌ Failed: {total - successful}/{total}")
            
            if successful > 0:
                print(f"🎉 Successfully inserted {successful} migration rules!")
            
            return results
            
        except Exception as e:
            print(f"❌ Error processing rules directory: {e}")
            return results
    
    def search_migration_rules(self, query: str, top_k: int = 5) -> Optional[List[Dict[str, Any]]]:
        """
        Search for migration rules based on query.
        
        Args:
            query: Search query
            top_k: Number of results to return
            
        Returns:
            List of relevant migration rules
        """
        try:
            if not self.index:
                print("❌ Index not available. Please check connection.")
                return None
            
            # Generate embedding for query
            query_embedding = self.generate_embedding(query)
            
            print(f"🔍 Searching for rules related to: '{query}'")
            response = self.index.query(
                vector=query_embedding,
                top_k=top_k,
                include_metadata=True
            )
            
            results = []
            print(f"\n📋 Found {len(response['matches'])} relevant migration rules:")
            
            for i, match in enumerate(response['matches']):
                metadata = match['metadata']
                result = {
                    "id": match['id'],
                    "score": match['score'],
                    "rule_name": metadata.get('rule_name', ''),
                    "spark_rule": metadata.get('spark_rule', ''),
                    "source_version": metadata.get('source_spark_version', ''),
                    "target_version": metadata.get('target_spark_version', ''),
                    "category": metadata.get('category', ''),
                    "severity": metadata.get('severity', ''),
                    "tags": metadata.get('tags', []),
                    "documentation": metadata.get('spark_doc_link', '')
                }
                results.append(result)
                
                print(f"\n{i+1}. 📝 {metadata.get('rule_name', 'Unknown Rule')} (Score: {match['score']:.4f})")
                print(f"   🏷️  Category: {metadata.get('category', 'N/A')} | Severity: {metadata.get('severity', 'N/A')}")
                print(f"   📊 Versions: {metadata.get('source_spark_version', '')} → {metadata.get('target_spark_version', '')}")
                print(f"   🏷️  Tags: {', '.join(metadata.get('tags', []))}")
                print(f"   📄 Scripts: Available in metadata")
                print(f"   📖 Documentation: {metadata.get('spark_doc_link', 'N/A')}")
            
            return results
            
        except Exception as e:
            print(f"❌ Error searching for rules: {e}")
            return None
    
    def get_index_stats(self) -> Optional[Dict[str, Any]]:
        """
        Get statistics about the migration rules in the index.
        
        Returns:
            Index statistics
        """
        try:
            if not self.index:
                print("❌ Index not available. Please check connection.")
                return None
            
            stats = self.index.describe_index_stats()
            print("\n📊 Migration Rules Database Statistics:")
            print(f"   📈 Total rules: {stats.get('total_vector_count', 0)}")
            print(f"   📏 Vector dimension: {stats.get('dimension', 0)}")
            if 'namespaces' in stats:
                print(f"   🏷️  Namespaces: {list(stats['namespaces'].keys())}")
            
            return stats
            
        except Exception as e:
            print(f"❌ Error getting index stats: {e}")
            return None


def main():
    """Main function to demonstrate migration rule insertion from structured folders."""
    # Initialize with your API key
    api_key = "pcsk_4kn9f1_7pTT2RoGerBQkmCKTMgUsR6BzKXvYwrYmLCdM3dEZCu58LRtmMz9yoJ4NgWpDjv"
    
    print("🚀 Migration Rule Database Inserter")
    print("="*50)
    
    # Create inserter instance
    inserter = MigrationRuleInserter(api_key, "developer-quickstart-py")
    
    try:
        # 1. Insert single rule from folder
        print("\n1️⃣  Inserting Format String Migration Rule...")
        rule_folder = "src/migration_rules/format_string_rule"
        
        if inserter.insert_rule_from_folder(rule_folder):
            print("✅ Single rule insertion completed!")
        else:
            print("❌ Single rule insertion failed!")
        
        # Wait for indexing
        time.sleep(2)
        
        # 2. Get Index Stats
        print("\n2️⃣  Getting Database Statistics...")
        inserter.get_index_stats()
        
        # 3. Search for format string rules
        print("\n3️⃣  Searching for Format String Rules...")
        inserter.search_migration_rules("format_string printf argument index")
        
        # 4. Insert all rules from directory (if there are more)
        print("\n4️⃣  Scanning for additional rules...")
        rules_directory = "src/migration_rules"
        results = inserter.insert_all_rules_from_directory(rules_directory, test_scripts=True)
        
        print(f"\n📋 Insertion results: {results}")
        
    except KeyboardInterrupt:
        print("\n⏹️  Operation interrupted by user.")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    
    print("\n🏁 Migration Rule Insertion Process Complete!")


if __name__ == "__main__":
    main()