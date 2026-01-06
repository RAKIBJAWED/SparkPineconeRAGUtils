# Apache Spark Migration Rules System

A comprehensive system for managing and storing Apache Spark migration rules with RAG (Retrieval-Augmented Generation) capabilities using Pinecone vector database.

## 🎯 Overview

This project provides a structured approach to manage Spark migration rules across different versions, complete with:
- **Before/After Code Examples**: Working Python scripts demonstrating deprecated and updated patterns
- **Vector Database Storage**: Pinecone integration for semantic search and retrieval
- **RAG System Ready**: Optimized for LLM-based code migration assistance
- **Validation System**: Automatic syntax checking and testing of migration scripts

## 📁 Project Structure

```
├── src/
│   ├── migration_rules/           # Migration rules organized by rule name
│   │   ├── format_string_rule/    # Format string argument indexing migration
│   │   │   ├── rule_config.json   # Rule metadata and configuration
│   │   │   ├── before_script.py   # Deprecated pattern demonstration
│   │   │   ├── after_script.py    # Updated pattern demonstration
│   │   │   └── README.md          # Rule-specific documentation
│   │   └── cast_alias_rule/       # Cast alias column naming migration
│   │       ├── rule_config.json
│   │       ├── before_script.py
│   │       ├── after_script.py
│   │       └── README.md
│   └── database_operations/
│       └── migration_rule_inserter.py  # Pinecone integration and insertion logic
├── requirements.txt               # Python dependencies
├── setup.sh                      # Environment setup script
└── README.md                     # This file
```

## 🚀 Quick Start

### 1. Environment Setup

```bash
# Clone the repository
git clone https://github.com/RAKIBJAWED/keroTestProject.git
cd keroTestProject

# Run setup script
chmod +x setup.sh
./setup.sh

# Activate virtual environment
source spark_env/bin/activate
```

### 2. Test Existing Rules

```bash
# Test format string migration rule
python src/migration_rules/format_string_rule/before_script.py
python src/migration_rules/format_string_rule/after_script.py

# Test cast alias migration rule
python src/migration_rules/cast_alias_rule/before_script.py
python src/migration_rules/cast_alias_rule/after_script.py
```

### 3. Insert Rules into Pinecone Database

```bash
# Insert all migration rules
python src/database_operations/migration_rule_inserter.py
```

## 📝 Adding New Migration Rules

### Step 1: Create Rule Structure

Create a new directory under `src/migration_rules/` with your rule name:

```bash
mkdir src/migration_rules/your_rule_name
```

### Step 2: Create Rule Configuration

Create `rule_config.json` with the following structure:

```json
{
  "rule_id": "your_rule_migration_003",
  "rule_name": "Your Rule Name",
  "source_spark_version": "3.x",
  "target_spark_version": "3.y",
  "error_with_before_script": "Description of the error/issue with old pattern",
  "spark_rule": "Official Spark documentation description of the change",
  "spark_doc_link": "https://spark.apache.org/docs/latest/sql-migration-guide.html#...",
  "migration_type": "category_of_change",
  "severity": "high|medium|low",
  "category": "descriptive_category",
  "tags": ["tag1", "tag2", "tag3"],
  "created_at": "2026-01-06T18:30:00Z"
}
```

### Step 3: Create Before Script

Create `before_script.py` demonstrating the deprecated pattern:

```python
#!/usr/bin/env python3
"""
Your Rule Migration - BEFORE Script (Spark X.Y and earlier)
This script demonstrates the old/deprecated behavior.

Usage:
    python before_script.py

This script can be run independently to test the old behavior.
"""

from pyspark.sql import SparkSession
# Add other imports as needed

def main():
    # Create SparkSession
    spark = SparkSession.builder \
        .appName('YourRuleBefore') \
        .master('local[*]') \
        .getOrCreate()
    
    print("=== Your Rule Migration - BEFORE (Old behavior) ===")
    
    try:
        # Create sample data
        # Demonstrate the deprecated pattern
        # Show expected errors or issues
        
        print("\n⚠️  WARNING: This demonstrates deprecated behavior")
        print("   Expected issues: [describe what problems occur]")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("   This error demonstrates why migration is necessary")
    
    finally:
        spark.stop()
        print("\nSpark session stopped.")

if __name__ == "__main__":
    main()
```

### Step 4: Create After Script

Create `after_script.py` demonstrating the updated pattern:

```python
#!/usr/bin/env python3
"""
Your Rule Migration - AFTER Script (Spark X.Y+)
This script demonstrates the correct/updated behavior.

Usage:
    python after_script.py

This script can be run independently to test the updated behavior.
"""

from pyspark.sql import SparkSession
# Add other imports as needed

def main():
    # Create SparkSession
    spark = SparkSession.builder \
        .appName('YourRuleAfter') \
        .master('local[*]') \
        .getOrCreate()
    
    print("=== Your Rule Migration - AFTER (Updated behavior) ===")
    
    try:
        # Create sample data
        # Demonstrate the updated pattern
        # Show successful execution
        
        print("\n✅ SUCCESS: Updated pattern works correctly!")
        print("   Benefits: [describe improvements]")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("   If you see this error, check your Spark setup")
    
    finally:
        spark.stop()
        print("\nSpark session stopped.")

if __name__ == "__main__":
    main()
```

### Step 5: Create Documentation

Create `README.md` for your rule:

```markdown
# Your Rule Migration

## Overview
Brief description of what this migration rule addresses.

## Rule Details
- **Rule ID**: your_rule_migration_003
- **Source Version**: Spark X.Y and earlier
- **Target Version**: Spark X.Z+
- **Severity**: High/Medium/Low
- **Category**: Your Category

## Problem Description
Detailed explanation of the issue and why migration is needed.

## Solution
Explanation of the updated approach.

## Examples
### Before (Deprecated)
```python
# Old pattern code
```

### After (Updated)
```python
# New pattern code
```

## Testing
```bash
python before_script.py
python after_script.py
```
```

## 🔍 Validation and Insertion Process

### Step 1: Validate Scripts Locally

```bash
# Navigate to your rule directory
cd src/migration_rules/your_rule_name

# Test before script
python before_script.py

# Test after script  
python after_script.py
```

### Step 2: Validate Rule Configuration

Ensure your `rule_config.json` follows the correct format and contains all required fields:
- `rule_id`: Unique identifier
- `rule_name`: Descriptive name
- `source_spark_version` & `target_spark_version`: Version information
- `error_with_before_script`: Description of the issue
- `spark_rule`: Official rule description
- `spark_doc_link`: Documentation link
- `migration_type`, `severity`, `category`: Classification
- `tags`: Array of relevant tags
- `created_at`: ISO timestamp

### Step 3: Insert into Database

```bash
# Insert single rule
python -c "
from src.database_operations.migration_rule_inserter import MigrationRuleInserter
api_key = 'your_pinecone_api_key'
inserter = MigrationRuleInserter(api_key, 'developer-quickstart-py')
success = inserter.insert_rule_from_folder('src/migration_rules/your_rule_name')
print('✅ Success!' if success else '❌ Failed!')
"

# Or insert all rules
python src/database_operations/migration_rule_inserter.py
```

### Step 4: Verify Insertion

```bash
# Search for your rule
python -c "
from src.database_operations.migration_rule_inserter import MigrationRuleInserter
api_key = 'your_pinecone_api_key'
inserter = MigrationRuleInserter(api_key, 'developer-quickstart-py')
inserter.search_migration_rules('your search terms', top_k=5)
"
```

## 🛠️ Configuration

### Pinecone Setup

1. Get your Pinecone API key from [Pinecone Console](https://app.pinecone.io/)
2. Update the API key in `src/database_operations/migration_rule_inserter.py`
3. Ensure your index name matches (default: `developer-quickstart-py`)

### Environment Variables

```bash
export PINECONE_API_KEY="your_api_key_here"
export SPARK_LOCAL_IP="127.0.0.1"  # Optional: for local Spark setup
```

## 📊 Current Migration Rules

### 1. Format String Argument Index Migration
- **Versions**: 3.2 → 3.3
- **Issue**: `format_string()` and `printf()` no longer support `0$` indexing
- **Solution**: Use `1$`-based indexing for arguments

### 2. Cast Auto-Generation Column Alias Migration  
- **Versions**: 3.1 → 3.2
- **Issue**: Auto-generated CAST expressions appear in column names
- **Solution**: CAST expressions are stripped from column aliases

## 🔧 Troubleshooting

### Common Issues

1. **Spark Session Errors**
   ```bash
   export SPARK_LOCAL_IP=127.0.0.1
   ```

2. **Pinecone Connection Issues**
   - Verify API key is correct
   - Check index name exists
   - Ensure network connectivity

3. **Script Validation Failures**
   - Check Python syntax
   - Verify all imports are available
   - Test with minimal Spark setup

### Getting Help

1. Check individual rule README files
2. Review script comments and documentation
3. Test scripts independently before insertion
4. Verify Pinecone database connectivity

## 🤝 Contributing

1. Follow the rule creation steps above
2. Ensure scripts are well-documented and tested
3. Include comprehensive examples
4. Add appropriate tags and metadata
5. Test insertion into Pinecone database

## 📚 Resources

- [Apache Spark Migration Guide](https://spark.apache.org/docs/latest/sql-migration-guide.html)
- [Pinecone Documentation](https://docs.pinecone.io/)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)

---

**Note**: This system is designed for LLM-based code migration assistance. Each rule includes complete working examples that can be retrieved and used for automated code transformation.