#!/bin/bash

echo "Setting up Apache Spark Python environment with modular structure..."

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "Python3 is not installed. Please install Python3 first."
    exit 1
fi

# Check if Java is installed (required for Spark)
if ! command -v java &> /dev/null; then
    echo "Java is not installed. Installing OpenJDK 11..."
    sudo apt-get update
    sudo apt-get install -y openjdk-11-jdk
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
    echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> ~/.bashrc
fi

# Create virtual environment
echo "Creating virtual environment..."
python3 -m venv spark_env
source spark_env/bin/activate

# Install dependencies
echo "Installing PySpark, Pinecone, and other dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Create necessary directories if they don't exist
mkdir -p logs
mkdir -p data

echo "Setup complete!"
echo ""
echo "🚀 To get started:"
echo "1. Activate the virtual environment: source spark_env/bin/activate"
echo "2. Run the main script: python main.py --help"
echo "3. Try specific components:"
echo "   • Spark examples: python main.py --spark-basic"
echo "   • DataFrame examples: python main.py --spark-dataframes"
echo "   • Format string migration: python main.py --format-string"
echo "   • All components: python main.py --all"
echo ""
echo "📁 Project structure:"
echo "   • src/spark_examples/ - Spark operation examples"
echo "   • src/pinecone_integration/ - Vector database operations"
echo "   • src/migration_rules/ - Migration rule demonstrations"
echo "   • tests/ - Unit tests"
echo ""
echo "🧪 To run tests: python -m pytest tests/"