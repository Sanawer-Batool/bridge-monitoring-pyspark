#!/bin/bash


set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}╔════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  Bridge Monitoring Pipeline - Setup       ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════╝${NC}"

# Check Python version
echo -e "\n${YELLOW}Checking Python version...${NC}"
python_version=$(python3 --version 2>&1 | awk '{print $2}')
echo -e "${GREEN}✓ Python $python_version${NC}"

echo -e "\n${YELLOW}Creating directory structure...${NC}"
mkdir -p data_generator
mkdir -p pipelines
mkdir -p notebooks
mkdir -p metadata
mkdir -p scripts
mkdir -p streams/bridge_temperature
mkdir -p streams/bridge_vibration
mkdir -p streams/bridge_tilt
mkdir -p bronze
mkdir -p silver
mkdir -p gold
mkdir -p checkpoints
mkdir -p reports
echo -e "${GREEN}✓ Directories created${NC}"

echo -e "\n${YELLOW}Creating .gitignore...${NC}"
cat > .gitignore << 'EOF'
# Streaming data
streams/
bronze/
silver/
gold/
checkpoints/
reports/

# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
env/
venv/
.venv
.env

# Jupyter
.ipynb_checkpoints/
*.ipynb_checkpoints

# Spark
spark-warehouse/
derby.log
metastore_db/

# IDE
.idea/
.vscode/
*.swp
*.swo
*~

# OS
.DS_Store
Thumbs.db
EOF
echo -e "${GREEN}✓ .gitignore created${NC}"

echo -e "\n${YELLOW}Installing Python dependencies...${NC}"
if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt
    echo -e "${GREEN}✓ Dependencies installed${NC}"
else
    echo -e "${YELLOW}⚠ requirements.txt not found. Install manually:${NC}"
    echo "  pip install pyspark jupyter pandas"
fi

if [ ! -f "metadata/bridges.csv" ]; then
    echo -e "\n${YELLOW}Creating sample bridge metadata...${NC}"
    cat > metadata/bridges.csv << 'EOF'
bridge_id,bridge_name,location,installation_date,bridge_type,length_meters,construction_year
bridge_001,Golden Gate Bridge,San Francisco,2020-01-15,Suspension,2737,1937
bridge_002,Brooklyn Bridge,New York,2019-06-22,Suspension,1825,1883
bridge_003,London Bridge,London,2021-03-10,Arch,283,1973
bridge_004,Sydney Harbour Bridge,Sydney,2020-11-05,Arch,1149,1932
bridge_005,Tower Bridge,London,2022-02-18,Bascule,244,1894
EOF
    echo -e "${GREEN}✓ Metadata file created${NC}"
else
    echo -e "${GREEN}✓ Metadata file already exists${NC}"
fi

if [ -d "scripts" ]; then
    echo -e "\n${YELLOW}Making scripts executable...${NC}"
    chmod +x scripts/*.sh 2>/dev/null || true
    echo -e "${GREEN}✓ Scripts are executable${NC}"
fi

echo -e "\n${YELLOW}Creating test script...${NC}"
cat > test_setup.py << 'EOF'
#!/usr/bin/env python3
"""Quick test to verify PySpark installation"""

try:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("SetupTest") \
        .master("local[*]") \
        .getOrCreate()
    
    # Create simple DataFrame
    data = [("bridge_001", 25.5), ("bridge_002", 30.2)]
    df = spark.createDataFrame(data, ["bridge_id", "temperature"])
    
    print("\n✓ PySpark is working correctly!")
    print("\nSample DataFrame:")
    df.show()
    
    spark.stop()
    print("\n✓ Setup test completed successfully!")
    
except Exception as e:
    print(f"\n✗ Setup test failed: {str(e)}")
    print("\nPlease check your PySpark installation.")
    exit(1)
EOF
chmod +x test_setup.py

echo -e "\n${YELLOW}Running setup test...${NC}"
python3 test_setup.py

echo -e "\n${BLUE}╔════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  Setup Complete!                           ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════╝${NC}"

echo -e "\n${GREEN}Next Steps:${NC}"
echo -e "  1. Review the README.md for documentation"
echo -e "  2. Place your pipeline Python files in:"
echo -e "     • data_generator/data_generator.py"
echo -e "     • pipelines/bronze_ingest.py"
echo -e "     • pipelines/silver_enrichment.py"
echo -e "     • pipelines/gold_aggregation.py"
echo -e "  3. Run the data generator:"
echo -e "     ${YELLOW}python data_generator/data_generator.py${NC}"
echo -e "  4. Start the pipeline:"
echo -e "     ${YELLOW}bash scripts/run_all.sh${NC}"
echo -e "  5. Validate results:"
echo -e "     ${YELLOW}python test_pipeline.py${NC}"

echo -e "\n${GREEN}Directory Structure:${NC}"
tree -L 2 -I 'streams|bronze|silver|gold|checkpoints|__pycache__|*.pyc' || \
    find . -maxdepth 2 -type d | grep -v -E '(streams|bronze|silver|gold|checkpoints|__pycache__|\.git)' | sort

echo -e "\n${BLUE}Happy Streaming! 🚀${NC}\n"
