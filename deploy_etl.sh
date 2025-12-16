#!/bin/bash

# ETL Pipeline Deployment Script for OpenAlex Morocco Star Schema
# This script sets up the environment and runs the complete ETL pipeline

set -e  # Exit on error

echo "🚀 OpenAlex Morocco Star Schema ETL Pipeline"
echo "=============================================="
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Step 1: Check Docker is running
echo -e "${YELLOW}Step 1: Checking Docker status...${NC}"
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}❌ Docker is not running. Please start Docker and try again.${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Docker is running${NC}"
echo ""

# Step 2: Check if postgres container exists
echo -e "${YELLOW}Step 2: Starting PostgreSQL container...${NC}"

echo " Stopping any existing containers..."
docker-compose -f compose.yml down -v
echo "  Starting Docker Compose services..."
docker-compose -f compose.yml up -d postgres
echo "  Waiting for PostgreSQL to be ready..."

# Step 4: Install Python dependencies
echo -e "${YELLOW}Step 3: Installing Python dependencies...${NC}"
pip install -r requirements_etl.txt
echo -e "${GREEN}✅ Dependencies installed${NC}"
echo ""

# Step 3: Initialize database schema
echo -e "${YELLOW}Step 4: Initializing star schema...${NC}"
docker exec -i td-postgres-1 psql -U postgres -d openalex_db -f - < openalex_schema_simplified.sql 2>&1 | head -20
echo -e "${GREEN}✅ Schema initialized${NC}"
echo ""

# Step 5: Run ETL pipeline
echo -e "${YELLOW}Step 5: Running ETL pipeline...${NC}"
echo "  (This may take several minutes depending on data volume)"
echo ""

python etl_openalex_simplified.py

echo ""
echo -e "${GREEN}✅ ETL Pipeline completed successfully!${NC}"
echo ""
echo "📊 Next steps:"
echo "  1. Connect to PostgreSQL: psql -h localhost -U postgres -d openalex_db"
echo "  2. Query analytical views: SELECT * FROM vw_works_normalized LIMIT 10;"
echo "  3. Connect Power BI or Apache Superset to the database"
echo ""
