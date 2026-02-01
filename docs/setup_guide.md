# Setup and Installation Guide

> Step-by-step instructions for setting up the Brazilian Municipalities Analytics project.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Installation](#installation)
3. [GCP/BigQuery Configuration](#gcpbigquery-configuration)
4. [Environment Configuration](#environment-configuration)
5. [Data Extraction](#data-extraction)
6. [Running the dbt Pipeline](#running-the-dbt-pipeline)
7. [Launching the Dashboard](#launching-the-dashboard)
8. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### System Requirements

- **Operating System:** Windows 10+, macOS 10.15+, or Linux
- **Python:** 3.11 or higher
- **Disk Space:** ~2 GB for data and dependencies
- **Memory:** 8 GB RAM recommended

### Required Software

| Software | Version | Purpose |
|----------|---------|---------|
| Python | 3.11+ | Runtime environment |
| Git | 2.0+ | Version control |
| uv or pip | Latest | Package management |

### Required Accounts

- **Google Cloud Platform (GCP) Account:** Required for BigQuery access
  - Free tier sufficient for this project
  - ~10 GB BigQuery queries per month

---

## Installation

### Step 1: Clone the Repository

```bash
git clone https://github.com/yourusername/municipios-projeto.git
cd municipios-projeto
```

### Step 2: Create Virtual Environment

**Option A: Using uv (Recommended)**

```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create and activate virtual environment
uv venv
source .venv/bin/activate  # Linux/macOS
# OR
.venv\Scripts\activate     # Windows
```

**Option B: Using standard venv**

```bash
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
# OR
.venv\Scripts\activate     # Windows
```

### Step 3: Install Dependencies

**Install all dependencies (recommended):**

```bash
# Using uv
uv pip install -e ".[all]"

# OR using pip
pip install -e ".[all]"
```

**Install specific dependency groups:**

```bash
# Core dependencies only
pip install -e .

# Add development tools
pip install -e ".[dev]"

# Add Jupyter notebooks support
pip install -e ".[notebooks]"

# Add dbt for transformations
pip install -e ".[dbt]"
```

### Step 4: Verify Installation

```bash
# Check Python version
python --version  # Should be 3.11+

# Check key packages
python -c "import polars; print(f'Polars: {polars.__version__}')"
python -c "import duckdb; print(f'DuckDB: {duckdb.__version__}')"
python -c "import streamlit; print(f'Streamlit: {streamlit.__version__}')"

# Check dbt
dbt --version
```

---

## GCP/BigQuery Configuration

Base dos Dados provides free access to Brazilian public data through BigQuery. You need a GCP project for billing (free tier covers typical usage).

### Step 1: Create a GCP Project

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Click "Select a project" in the top bar
3. Click "New Project"
4. Enter project name: `municipios-analytics` (or similar)
5. Click "Create"

### Step 2: Enable BigQuery API

1. In the GCP Console, go to "APIs & Services" > "Library"
2. Search for "BigQuery API"
3. Click on "BigQuery API"
4. Click "Enable"

### Step 3: Create a Service Account

1. Go to "IAM & Admin" > "Service Accounts"
2. Click "Create Service Account"
3. Enter:
   - Name: `municipios-extractor`
   - Description: "Service account for Base dos Dados queries"
4. Click "Create and Continue"
5. Grant role: "BigQuery User"
6. Click "Done"

### Step 4: Create and Download Credentials

1. Click on the service account you just created
2. Go to "Keys" tab
3. Click "Add Key" > "Create new key"
4. Select "JSON" format
5. Click "Create"
6. Save the downloaded file as `credentials.json` in the project root

**Important:** Never commit `credentials.json` to version control!

### Step 5: Verify BigQuery Access

```bash
# Set environment variable temporarily
export GOOGLE_APPLICATION_CREDENTIALS="./credentials.json"

# Test connection (Python)
python -c "
from google.cloud import bigquery
client = bigquery.Client()
print('Successfully connected to BigQuery!')
"
```

---

## Environment Configuration

### Step 1: Create Environment File

```bash
cp .env.example .env
```

### Step 2: Edit .env File

Open `.env` in your editor and configure:

```bash
# .env file

# Google Cloud Configuration
GOOGLE_CLOUD_PROJECT=your-project-id          # Your GCP project ID
GOOGLE_APPLICATION_CREDENTIALS=./credentials.json
BASEDOSDADOS_BILLING_PROJECT_ID=your-project-id  # Usually same as above

# Data Paths (optional - defaults work fine)
RAW_DATA_PATH=./data/raw
WAREHOUSE_PATH=./data/warehouse
DUCKDB_DATABASE_PATH=./data/warehouse/analytics.duckdb

# Application Settings
ENVIRONMENT=development
LOG_LEVEL=INFO
```

### Step 3: Verify Configuration

```bash
# Load environment and test
python -c "
from dotenv import load_dotenv
import os
load_dotenv()
print(f\"Project ID: {os.getenv('BASEDOSDADOS_BILLING_PROJECT_ID')}\")
print(f\"Credentials: {os.getenv('GOOGLE_APPLICATION_CREDENTIALS')}\")
"
```

---

## Data Extraction

The extraction script downloads data from Base dos Dados BigQuery and saves as Parquet files.

### Step 1: Run Extraction Script

```bash
# Extract all tables
python scripts/extract_data.py

# OR extract with force refresh
python scripts/extract_data.py --force

# OR extract specific table
python scripts/extract_data.py --table municipio
```

### Step 2: Monitor Progress

The script displays progress for each table:

```
[1/8] Processing municipio
Extracting: br_bd_diretorios_brasil.municipio
Extracted municipio: 5,570 rows, 1.23 MB

[2/8] Processing populacao
Extracting: br_ibge_populacao.municipio
Extracted populacao: 195,950 rows, 4.56 MB
...
```

### Step 3: Verify Extraction

```bash
# List extracted files
python scripts/extract_data.py status

# OR manually check
ls -la data/raw/*.parquet
```

**Expected files:**

| File | Approx. Size | Rows |
|------|--------------|------|
| municipio.parquet | 1-2 MB | 5,570 |
| populacao.parquet | 4-5 MB | ~200,000 |
| idhm.parquet | 10-15 MB | ~100,000 |
| resultados_candidato_municipio.parquet | 50-100 MB | ~2,000,000 |
| municipio_despesas_funcao.parquet | 100-200 MB | ~5,000,000 |
| municipio_receitas_orcamentarias.parquet | 50-100 MB | ~2,000,000 |

### Available Tables

```bash
# List all available tables
python scripts/extract_data.py list-tables
```

---

## Running the dbt Pipeline

dbt transforms the raw Parquet files into a dimensional star schema.

### Step 1: Navigate to dbt Project

```bash
cd dbt_project
```

### Step 2: Install dbt Packages

```bash
dbt deps
```

This installs:
- `dbt-labs/dbt_utils` - Utility macros
- `dbt-labs/codegen` - Code generation helpers

### Step 3: Test Database Connection

```bash
dbt debug
```

Expected output:

```
  profiles.yml file [OK found and valid]
  dbt_project.yml file [OK found and valid]
  Connection test: [OK connection ok]
```

### Step 4: Run dbt Build

```bash
# Build all models (recommended)
dbt build

# OR run just models (skip tests)
dbt run

# OR run specific model
dbt run --select dim_municipio
```

### Step 5: Verify Build

```bash
# Run tests
dbt test

# Generate documentation
dbt docs generate

# Serve documentation locally
dbt docs serve
```

### Step 6: Explore Results

```bash
# Check created tables
dbt show --select dim_municipio --limit 5
```

Or connect directly to DuckDB:

```python
import duckdb
conn = duckdb.connect('../data/warehouse/analytics.duckdb')
print(conn.execute("SHOW TABLES").fetchall())
```

---

## Launching the Dashboard

The Streamlit dashboard provides interactive visualizations.

### Step 1: Return to Project Root

```bash
cd ..  # If still in dbt_project directory
```

### Step 2: Launch Streamlit

```bash
streamlit run dashboard/app.py
```

### Step 3: Access Dashboard

Open your browser to: [http://localhost:8501](http://localhost:8501)

### Dashboard Pages

| Page | Description |
|------|-------------|
| Home | Project overview and navigation |
| Overview | National KPIs and choropleth maps |
| Municipality Profile | Deep-dive into any municipality |
| Rankings | Sortable tables by any indicator |
| Correlations | Interactive scatter plot explorer |

### Custom Port

```bash
streamlit run dashboard/app.py --server.port 8080
```

---

## Troubleshooting

### Common Issues

#### 1. BigQuery Authentication Error

**Error:** `DefaultCredentialsError: Could not automatically determine credentials`

**Solution:**
```bash
# Set credentials path explicitly
export GOOGLE_APPLICATION_CREDENTIALS="./credentials.json"

# OR authenticate via gcloud
gcloud auth application-default login
```

#### 2. dbt Connection Error

**Error:** `Could not find profile named 'municipios_analytics'`

**Solution:**
```bash
# Ensure profiles.yml exists
cp dbt_project/profiles.yml.example dbt_project/profiles.yml

# Verify path in profiles.yml
cat dbt_project/profiles.yml
```

#### 3. Missing Parquet Files

**Error:** `FileNotFoundError: data/raw/municipio.parquet`

**Solution:**
```bash
# Run extraction first
python scripts/extract_data.py

# Or check if files exist
ls data/raw/
```

#### 4. DuckDB Database Not Found

**Error:** `Database not found at data/warehouse/analytics.duckdb`

**Solution:**
```bash
# Run dbt to create database
cd dbt_project
dbt build
```

#### 5. Memory Error During Extraction

**Error:** `MemoryError` or slow extraction

**Solution:**
- Extract tables one at a time: `python scripts/extract_data.py --table municipio`
- Increase system swap space
- Close other applications

#### 6. Streamlit ModuleNotFoundError

**Error:** `ModuleNotFoundError: No module named 'dashboard'`

**Solution:**
```bash
# Install package in development mode
pip install -e .

# Run from project root
cd /path/to/municipios-projeto
streamlit run dashboard/app.py
```

### Getting Help

1. Check logs in `logs/` directory
2. Run `dbt debug` for connection issues
3. Open an issue on GitHub with:
   - Error message
   - Python version (`python --version`)
   - Operating system
   - Steps to reproduce

---

## Quick Reference

### Complete Setup (Copy-Paste)

```bash
# 1. Clone and setup
git clone https://github.com/yourusername/municipios-projeto.git
cd municipios-projeto
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows
pip install -e ".[all]"

# 2. Configure environment
cp .env.example .env
# Edit .env with your GCP project ID

# 3. Extract data
python scripts/extract_data.py

# 4. Run dbt
cd dbt_project
dbt deps
dbt build

# 5. Launch dashboard
cd ..
streamlit run dashboard/app.py
```

### Daily Development

```bash
# Activate environment
source .venv/bin/activate

# Update data (if needed)
python scripts/extract_data.py --force

# Rebuild dbt models
cd dbt_project && dbt build && cd ..

# Start dashboard
streamlit run dashboard/app.py
```

---

## Next Steps

After completing setup:

1. Explore the [Architecture Documentation](./architecture.md)
2. Review the [Data Dictionary](./data_dictionary.md)
3. Learn the [Dashboard Query API](./api_reference.md)
4. Run analysis in Jupyter notebooks
