# Technical Architecture

> System architecture documentation for the Brazilian Municipalities Analytics project.

## Overview

This project implements a modern data analytics pipeline for analyzing socio-economic indicators across all 5,570 Brazilian municipalities. The architecture follows the Medallion pattern (Bronze/Silver/Gold layers) with a dimensional star schema for analytical queries.

## System Architecture Diagram

```
+-----------------------------------------------------------------------------------+
|                              DATA ARCHITECTURE                                     |
+-----------------------------------------------------------------------------------+
|                                                                                   |
|   EXTRACTION LAYER                TRANSFORMATION LAYER          PRESENTATION     |
|   (Bronze)                        (Silver/Gold)                  LAYER            |
|                                                                                   |
|   +---------------+               +------------------+          +-------------+  |
|   |               |               |                  |          |             |  |
|   | Base dos      |    Parquet    |    DuckDB        |   SQL    | Streamlit   |  |
|   | Dados         +-------------->+    Database      +--------->+ Dashboard   |  |
|   | (BigQuery)    |    Files      |                  |          |             |  |
|   |               |               |  +------------+  |          +------+------+  |
|   +---------------+               |  |    dbt     |  |                 |         |
|                                   |  | (SQL       |  |                 v         |
|   Tables:                         |  | transforms)|  |          +-------------+  |
|   - br_ibge_populacao             |  +------------+  |          |   Plotly    |  |
|   - br_pnud_atlas                 |                  |          |   Charts    |  |
|   - br_tse_eleicoes               +------------------+          +-------------+  |
|   - br_me_siconfi                                                                |
|   - br_bd_diretorios                                                             |
|                                                                                   |
+-----------------------------------------------------------------------------------+
```

## Data Flow Diagram

```
+-------------+      +-------------+      +-------------+      +-------------+
|             |      |             |      |             |      |             |
|  BigQuery   |----->|   Parquet   |----->|   dbt       |----->|  DuckDB     |
|  (Source)   |      |   (Bronze)  |      |   (ETL)     |      |  (Gold)     |
|             |      |             |      |             |      |             |
+------+------+      +------+------+      +------+------+      +------+------+
       |                    |                    |                    |
       v                    v                    v                    v
  basedosdados         data/raw/          models/staging/       data/warehouse/
  Python SDK           *.parquet          models/marts/         analytics.duckdb
```

### Pipeline Steps

1. **Extraction** (Python + basedosdados)
   - Connect to Base dos Dados BigQuery
   - Execute SQL queries for each table
   - Save results as Parquet files in `data/raw/`

2. **Staging** (dbt views)
   - Read Parquet files via `read_parquet()`
   - Clean and standardize column names
   - Apply data type casting
   - Filter invalid records

3. **Transformation** (dbt tables)
   - Create dimension tables (dim_municipio, dim_calendario)
   - Create fact tables (fct_indicadores_sociais, fct_eleicoes, fct_financas_municipais)
   - Implement star schema relationships

4. **Presentation** (Streamlit)
   - Query DuckDB via Polars DataFrames
   - Generate interactive visualizations
   - Provide drill-down capabilities

## Technology Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| **Data Source** | [Base dos Dados](https://basedosdados.org/) | Curated Brazilian public data on BigQuery |
| **Extraction** | basedosdados + Polars | Query BigQuery, convert to Parquet |
| **Storage** | Parquet (Bronze) | Columnar format for raw data |
| **Database** | [DuckDB](https://duckdb.org/) | In-process OLAP database |
| **Transformation** | [dbt-duckdb](https://github.com/duckdb/dbt-duckdb) | SQL-based transformations |
| **Analysis** | [Polars](https://pola.rs/) | Fast DataFrame operations |
| **Visualization** | [Streamlit](https://streamlit.io/) | Interactive dashboards |
| **Charts** | [Plotly](https://plotly.com/) | Interactive charts and maps |
| **Geography** | geopandas + geobr | Brazilian geographic boundaries |

## Data Pipeline Architecture

### Layer Architecture (Medallion Pattern)

```
+------------------------------------------------------------------+
|                        MEDALLION ARCHITECTURE                     |
+------------------------------------------------------------------+
|                                                                   |
|  BRONZE LAYER          SILVER LAYER           GOLD LAYER          |
|  (Raw Data)            (Cleaned)              (Business-Ready)    |
|                                                                   |
|  +-------------+       +-------------+        +----------------+  |
|  | municipio   |       | stg_munic   |        | dim_municipio  |  |
|  | .parquet    +------>+ ipios       +------->+ (5,570 rows)   |  |
|  +-------------+       +-------------+        +----------------+  |
|                                                                   |
|  +-------------+       +-------------+        +----------------+  |
|  | populacao   |       | stg_popula  |        | dim_calendario |  |
|  | .parquet    +------>+ cao         +------->+ (41 years)     |  |
|  +-------------+       +-------------+        +----------------+  |
|                                                                   |
|  +-------------+       +-------------+        +----------------+  |
|  | idhm        |       | stg_idhm    |        | fct_indicadores|  |
|  | .parquet    +------>+             +------->+ _sociais       |  |
|  +-------------+       +-------------+        +----------------+  |
|                                                                   |
|  +-------------+       +-------------+        +----------------+  |
|  | eleicoes    |       | stg_eleicoes|        | fct_eleicoes   |  |
|  | .parquet    +------>+             +------->+                |  |
|  +-------------+       +-------------+        +----------------+  |
|                                                                   |
|  +-------------+       +-------------+        +----------------+  |
|  | despesas    |       | stg_despesas|        | fct_financas   |  |
|  | receitas    +------>| stg_receitas+------->+ _municipais    |  |
|  +-------------+       +-------------+        +----------------+  |
|                                                                   |
+------------------------------------------------------------------+
```

### Layer Descriptions

| Layer | Schema | Materialization | Description |
|-------|--------|-----------------|-------------|
| Bronze | `data/raw/` | Parquet files | Raw data extracted from BigQuery |
| Silver | `main_staging` | Views | Cleaned, typed, and filtered data |
| Gold | `main_marts` | Tables | Business-ready dimensional model |

## Star Schema Design

```
                           +------------------+
                           |  dim_calendario  |
                           +--------+---------+
                                    |
                                    | sk_ano
                                    |
+-------------------+      +--------v---------+      +-------------------+
|  dim_municipio    |      | fct_indicadores  |      |                   |
|  (Conformed)      +<-----+    _sociais      +----->+  Metrics:         |
+-------------------+      +------------------+      |  - IDHM           |
        ^                                            |  - IVS            |
        |  sk_municipio                              |  - Gini           |
        |                                            +-------------------+
        |
        |                  +------------------+
        +------------------+  fct_eleicoes    |
        |                  +------------------+
        |
        |                  +------------------+
        +------------------+ fct_financas    |
                           |   _municipais   |
                           +------------------+
```

### Schema Naming Convention

dbt-duckdb creates schemas using the pattern: `{database}_{schema}`

| dbt Schema | DuckDB Schema | Contents |
|------------|---------------|----------|
| staging | `main_staging` | Staging views (stg_*) |
| intermediate | `main_intermediate` | Intermediate models |
| marts | `main_marts` | Dimension and fact tables |
| seeds | `main_seeds` | Seed/reference data |

## Component Architecture

### Extraction Component

```
src/extraction/
+-- base_dos_dados.py     # BaseDadosExtractor class
    |
    +-- TableConfig       # Table configuration dataclass
    +-- DEFAULT_TABLES    # List of tables to extract
    +-- extract_table()   # Extract single table
    +-- extract_all()     # Extract all configured tables
```

**Key Classes:**

- `BaseDadosExtractor`: Main extraction class
- `TableConfig`: Configuration for each source table

### dbt Project Structure

```
dbt_project/
+-- dbt_project.yml       # Project configuration
+-- profiles.yml          # Connection profiles
+-- packages.yml          # Package dependencies
|
+-- models/
|   +-- staging/          # Bronze -> Silver
|   |   +-- stg_municipios.sql
|   |   +-- stg_populacao.sql
|   |   +-- stg_idhm.sql
|   |   +-- stg_eleicoes.sql
|   |   +-- stg_despesas.sql
|   |   +-- stg_receitas.sql
|   |
|   +-- marts/
|       +-- dimensions/   # Dimension tables
|       |   +-- dim_municipio.sql
|       |   +-- dim_calendario.sql
|       |
|       +-- facts/        # Fact tables
|           +-- fct_indicadores_sociais.sql
|           +-- fct_eleicoes.sql
|           +-- fct_financas_municipais.sql
```

### Dashboard Architecture

```
dashboard/
+-- app.py                # Main Streamlit entry point
+-- data/
|   +-- queries.py        # Database query functions
|
+-- pages/
    +-- 1_Overview.py     # National KPIs and maps
    +-- 2_Municipality_Profile.py  # Municipality deep-dive
    +-- 3_Rankings.py     # Sortable rankings
    +-- 4_Correlations.py # Scatter plot explorer
```

## Data Volume Estimates

| Table | Estimated Rows | Update Frequency |
|-------|---------------|------------------|
| dim_municipio | 5,570 | Static |
| dim_calendario | 41 | Annual |
| fct_indicadores_sociais | ~11,000 | Census years (2000, 2010) |
| fct_eleicoes | ~150,000 | Every 4 years |
| fct_financas_municipais | ~60,000 | Annual (2013+) |

## Security Considerations

### Credentials Management

- GCP credentials stored in `.env` file (git-ignored)
- Service account with minimal BigQuery User role
- Read-only connection to DuckDB in dashboard

### Data Privacy

- All data sourced from public Brazilian government datasets
- No PII (Personally Identifiable Information)
- Aggregated to municipality level (no individual records)

## Performance Optimization

### Query Optimization

1. **DuckDB** provides columnar storage and vectorized execution
2. **Parquet** files enable predicate pushdown and column pruning
3. **Materialized tables** for frequently queried marts
4. **LRU caching** in dashboard query functions

### dbt Optimizations

```yaml
# dbt_project.yml
models:
  staging:
    +materialized: view     # Lightweight, compute on demand
  marts:
    +materialized: table    # Pre-computed for fast queries
```

## Scalability

The current architecture supports:

- **5,570 municipalities** (all Brazilian municipalities)
- **30+ years** of historical data
- **Sub-second** query response times
- **Single-node** deployment (no cluster required)

For larger datasets, consider:

- MotherDuck (cloud DuckDB) for shared access
- Apache Spark for distributed processing
- Delta Lake for ACID transactions

## Deployment Options

### Local Development

```bash
# Default: SQLite-like embedded DuckDB
dbt_project/profiles.yml -> path: '../data/warehouse/analytics.duckdb'
```

### Production (Options)

1. **Docker container** with mounted volume
2. **Cloud VM** with persistent disk
3. **MotherDuck** for serverless cloud deployment
4. **Streamlit Cloud** for dashboard hosting

## Monitoring and Observability

### dbt Artifacts

- `target/manifest.json` - Model metadata
- `target/run_results.json` - Execution results
- `logs/dbt.log` - Debug logs

### Application Logging

```python
# Using loguru for structured logging
from loguru import logger
logger.add("logs/extraction_{time}.log", rotation="10 MB")
```

## Related Documentation

- [Data Dictionary](./data_dictionary.md) - Column descriptions and data types
- [Setup Guide](./setup_guide.md) - Installation and configuration
- [API Reference](./api_reference.md) - Dashboard query functions
