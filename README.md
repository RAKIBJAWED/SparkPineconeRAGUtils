# Spark & Pinecone Integration Project

A comprehensive project demonstrating Apache Spark data processing with Pinecone vector database integration for RAG (Retrieval-Augmented Generation) applications.

## 🚀 Features

- **Apache Spark Integration**: Complete setup with RDD and DataFrame operations
- **Pinecone Vector Database**: CRUD operations and similarity search
- **RAG Implementation**: 15 curated Spark migration patterns for LLM assistance
- **Automated Setup**: One-command environment configuration
- **Production Ready**: Clean code structure with proper error handling

## 📋 Prerequisites

- **Operating System**: Linux/Ubuntu (tested on Ubuntu 24.04)
- **Python**: 3.12+ 
- **Java**: OpenJDK 11+ (automatically installed by setup script)
- **Memory**: 4GB+ RAM recommended for Spark operations

## 🛠️ Quick Start

### 1. Clone the Repository
```bash
git clone https://github.com/RAKIBJAWED/keroTestProject.git
cd keroTestProject
```

### 2. Run Setup Script
```bash
chmod +x setup.sh
./setup.sh
```

The setup script will:
- Install Java OpenJDK 11 (if not present)
- Create a Python virtual environment
- Install all required dependencies (PySpark, Pinecone, etc.)

### 3. Activate Virtual Environment
```bash
source spark_env/bin/activate
```

### 4. Run Examples
```bash
# Run Spark example
python spark_example.py

# Test Pinecone CRUD operations
python pinecone_crud_test.py

# Run Spark Migration RAG system
python spark_migration_rag.py
```

## 📁 Project Structure

```
keroTestProject/
├── README.md                    # Project documentation
├── requirements.txt             # Python dependencies
├── setup.sh                     # Environment setup script
├── .gitignore                   # Git ignore rules
├── spark_example.py             # Spark RDD/DataFrame examples
├── pinecone_crud_test.py        # Pinecone CRUD operations
├── spark_migration_rag.py       # RAG system for Spark migrations
└── spark_env/                   # Virtual environment (ignored)
```

## 🔧 Components Overview

### 1. Spark Example (`spark_example.py`)
Demonstrates core Apache Spark functionality:
- **RDD Operations**: Map, filter, reduce transformations
- **DataFrame API**: SQL-like operations and aggregations
- **Data Processing**: Sample employee data analysis
- **Performance Optimization**: Best practices implementation

**Key Features:**
- SparkSession configuration
- RDD transformations and actions
- DataFrame operations with built-in functions
- Group by and aggregation examples

### 2. Pinecone CRUD Test (`pinecone_crud_test.py`)
Complete vector database operations:
- **Index Management**: Create, configure, and delete indexes
- **Vector Operations**: Upsert, query, fetch, update, delete
- **Similarity Search**: Cosine similarity with metadata filtering
- **Statistics**: Index monitoring and performance metrics

**Key Features:**
- Serverless index creation
- Batch vector operations
- Metadata management
- Error handling and cleanup

### 3. Spark Migration RAG (`spark_migration_rag.py`)
RAG system for Spark version migrations:
- **15 Migration Patterns**: Curated Spark 2.4 → 3.5 upgrades
- **Vector Embeddings**: Hash-based embedding generation
- **Similarity Search**: Find relevant migration patterns
- **LLM Integration**: Ready for chatbot/assistant integration

**Migration Categories:**
- Performance optimizations (AQE, Arrow, caching)
- API modernization (DataFrame vs RDD)
- Configuration updates (partitioning, broadcasting)
- Best practices (UDF alternatives, storage levels)

## 🔑 Configuration

### Pinecone Setup
1. Get your Pinecone API key from [Pinecone Console](https://app.pinecone.io/)
2. Update the API key in the respective Python files:
   ```python
   api_key = "your-pinecone-api-key-here"
   ```

### Spark Configuration
The project uses local Spark configuration optimized for development:
- **Master**: `local[*]` (uses all available cores)
- **Memory**: Configured for local development
- **Java Integration**: Automatic JVM setup

## 📊 Sample Data

### Spark Example Data
- **Employee Dataset**: 5 sample records with name, age, job, salary
- **Number Processing**: RDD operations on integer sequences
- **Aggregations**: Group by job role with salary statistics

### RAG Migration Data
15 comprehensive migration patterns covering:
- Adaptive Query Execution (AQE) enablement
- Arrow-based columnar data transfers
- Dynamic allocation configuration
- Broadcast join optimizations
- Storage level best practices

## 🚀 Usage Examples

### Basic Spark Operations
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .master("local[*]") \
    .getOrCreate()

# Create DataFrame
df = spark.createDataFrame(data, columns)
df.show()

# Perform aggregations
result = df.groupBy("category").agg(avg("value"))
```

### Pinecone Vector Operations
```python
from pinecone_crud_test import PineconeCRUDTest

# Initialize
crud = PineconeCRUDTest(api_key="your-key")

# Create index
crud.create_index(dimension=128)

# Insert vectors
vectors = crud.generate_sample_vectors(count=10)
crud.upsert_vectors(vectors)

# Query similar vectors
results = crud.query_vectors(query_vector, top_k=5)
```

### RAG System Usage
```python
from spark_migration_rag import SparkMigrationRAG

# Initialize RAG system
rag = SparkMigrationRAG(api_key="your-key")

# Insert migration patterns
rag.insert_migration_data()

# Search for solutions
results = rag.search_migration_patterns("performance issues")
```

## 🔍 Troubleshooting

### Common Issues

**Java Not Found:**
```bash
sudo apt-get update
sudo apt-get install openjdk-11-jdk
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

**Python Virtual Environment Issues:**
```bash
sudo apt install python3.12-venv python3-pip
```

**Pinecone Connection Errors:**
- Verify API key is correct
- Check internet connectivity
- Ensure index name matches existing index

**Spark Memory Issues:**
- Reduce data size for local testing
- Adjust Spark configuration in code
- Increase system memory allocation

## 📈 Performance Tips

### Spark Optimization
- Use DataFrame API over RDD when possible
- Enable Adaptive Query Execution (AQE)
- Configure appropriate partition sizes
- Use broadcast joins for small tables
- Cache frequently accessed data

### Pinecone Optimization
- Batch vector operations for better throughput
- Use appropriate vector dimensions
- Implement proper metadata filtering
- Monitor index statistics regularly

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **Apache Spark** - Unified analytics engine for large-scale data processing
- **Pinecone** - Vector database for machine learning applications
- **Python Community** - For excellent libraries and tools

## 📞 Support

For questions and support:
- Create an issue in the GitHub repository
- Check the troubleshooting section above
- Review the official documentation for [Spark](https://spark.apache.org/docs/latest/) and [Pinecone](https://docs.pinecone.io/)

---

**Happy Coding! 🎉**