# Brazilian Municipalities Analytics

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

A comprehensive data analytics project analyzing socio-economic and governance indicators across all **5,570 Brazilian municipalities**. This project demonstrates modern data engineering practices with a focus on analytical insights and interactive visualizations.

## Overview

This project integrates multiple Brazilian public data sources to explore correlations between:
- **Human Development Index (IDHM)** and its components
- **Municipal finances** (revenue, expenses, federal transfers)
- **Electoral patterns** (mayors, parties, turnout)
- **Infrastructure** (sanitation, water, sewage coverage)
- **Demographics** (population, urbanization, literacy)

## Key Features

- **Automated ETL Pipeline**: Extract data from [Base dos Dados](https://basedosdados.org/) BigQuery
- **Star Schema Data Model**: Optimized dimensional model with dbt transformations
- **Interactive Dashboard**: Streamlit application with choropleth maps and drill-down capabilities
- **Analytical Notebooks**: Correlation studies and pattern analysis

## Architecture

```
Base dos Dados ──► Parquet (Bronze) ──► DuckDB ──► dbt (Silver/Gold) ──► Streamlit
   (BigQuery)         (raw/)           (warehouse/)    (Star Schema)     (Dashboard)
```

### Tech Stack

| Layer | Technology |
|-------|------------|
| Data Source | [Base dos Dados](https://basedosdados.org/) (BigQuery) |
| Processing | [Polars](https://pola.rs/) |
| Database | [DuckDB](https://duckdb.org/) |
| Transformation | [dbt](https://www.getdbt.com/) |
| Visualization | [Streamlit](https://streamlit.io/) + [Plotly](https://plotly.com/) |

## Data Sources

| Source | Content | Coverage |
|--------|---------|----------|
| **IBGE** | Population, GDP, geography | 1991-2025 |
| **Atlas Brasil (PNUD)** | IDHM, 200+ indicators | 1991, 2000, 2010 |
| **TSE** | Electoral results, parties | 1996-2024 |
| **SICONFI/Tesouro** | Municipal finances | 2013-2023 |
| **SNIS** | Sanitation indicators | 1995-2022 |

## Project Structure

```
municipios-projeto/
├── src/                    # Python source code
│   ├── extraction/         # Data extraction modules
│   ├── utils/              # Utility functions
│   └── analysis/           # Analytical code
├── dbt_project/            # dbt transformation project
│   ├── models/
│   │   ├── staging/        # Bronze → Silver
│   │   ├── intermediate/   # Business logic
│   │   └── marts/          # Star schema (Gold)
│   └── seeds/              # Reference data
├── dashboard/              # Streamlit application
│   ├── app.py              # Main entry point
│   └── pages/              # Dashboard pages
├── notebooks/              # Jupyter analysis notebooks
├── docs/                   # Documentation
└── scripts/                # Utility scripts
```

## Quick Start

### Prerequisites

- Python 3.11+
- [uv](https://github.com/astral-sh/uv) or [Poetry](https://python-poetry.org/)
- Google Cloud account (for BigQuery access)

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/municipios-projeto.git
   cd municipios-projeto
   ```

2. **Create virtual environment and install dependencies**
   ```bash
   # Using uv (recommended)
   uv venv
   source .venv/bin/activate  # or `.venv\Scripts\activate` on Windows
   uv pip install -e ".[all]"

   # Or using pip
   python -m venv .venv
   source .venv/bin/activate
   pip install -e ".[all]"
   ```

3. **Configure environment**
   ```bash
   cp .env.example .env
   # Edit .env with your Google Cloud project ID and credentials path
   ```

4. **Set up Google Cloud credentials**
   - Go to [Google Cloud Console](https://console.cloud.google.com/)
   - Create a new project (e.g., "municipios-analytics")
   - Enable the BigQuery API
   - Create a service account with BigQuery User role
   - Download JSON credentials and save as `credentials.json`

### Running the Project

1. **Extract data from Base dos Dados**
   ```bash
   python scripts/extract_data.py
   ```

2. **Run dbt transformations**
   ```bash
   cd dbt_project
   dbt deps
   dbt build
   dbt docs generate
   ```

3. **Launch the dashboard**
   ```bash
   streamlit run dashboard/app.py
   ```

## Data Model

### Star Schema

```
                    ┌─────────────────┐
                    │  dim_municipio  │
                    │   (5,570 rows)  │
                    └────────┬────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
        ▼                    ▼                    ▼
┌───────────────┐   ┌───────────────┐   ┌───────────────┐
│fct_indicadores│   │  fct_eleicoes │   │ fct_financas  │
│   _sociais    │   │               │   │  _municipais  │
└───────────────┘   └───────────────┘   └───────────────┘
```

### Key Dimensions

- **dim_municipio**: Central dimension with IBGE/TSE codes, geography, population
- **dim_calendario**: Time dimension with census/election year flags
- **dim_partido**: Political parties with spectrum classification

### Key Facts

- **fct_indicadores_sociais**: IDHM, demographics, sanitation, GDP
- **fct_eleicoes**: Electoral results, turnout, winning parties
- **fct_financas_municipais**: Revenue, expenses, transfer dependency

## Analyses

### 1. IDHM vs. Municipal Finances
Correlation between revenue per capita and human development outcomes.

### 2. Electoral Patterns
Voter turnout and party dominance by development quintiles.

### 3. Sanitation-Health Correlation
Water/sewage coverage impact on longevity indices.

### 4. Transfer Dependency
FPM/ICMS dependency ratio by municipality size and development level.

## Dashboard

The Streamlit dashboard includes:

1. **Overview**: National KPIs with choropleth map
2. **Municipality Profile**: Deep-dive into any municipality
3. **Comparisons**: Side-by-side municipality comparison
4. **Rankings**: Sortable tables by any metric
5. **Correlations**: Interactive scatter plot explorer

## Development

### Running Tests
```bash
pytest
```

### Linting
```bash
ruff check .
ruff format .
```

### Type Checking
```bash
mypy src/
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [Base dos Dados](https://basedosdados.org/) for providing treated, standardized Brazilian public data
- [IBGE](https://www.ibge.gov.br/) for demographic and economic data
- [Atlas Brasil / PNUD](http://www.atlasbrasil.org.br/) for human development indicators
- [TSE](https://dadosabertos.tse.jus.br/) for electoral data
- [Tesouro Nacional](https://siconfi.tesouro.gov.br/) for municipal finance data
