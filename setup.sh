#!/bin/bash

echo "Setting up Apache Spark Python environment..."

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
echo "Installing PySpark and dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

echo "Setup complete!"
echo "To run the example:"
echo "1. Activate the virtual environment: source spark_env/bin/activate"
echo "2. Run the script: python spark_example.py"