# Dashboard API Reference

> Reference documentation for the query functions in `dashboard/data/queries.py`.

## Overview

The dashboard queries module provides Python functions to query the DuckDB analytics database. All functions return data as Polars DataFrames for efficient processing.

## Module Location

```
dashboard/data/queries.py
```

## Constants

```python
# Path to DuckDB database
WAREHOUSE_PATH = PROJECT_ROOT / "data" / "warehouse" / "analytics.duckdb"

# Schema names (dbt-duckdb convention: database_schema)
MARTS_SCHEMA = "main_marts"
STAGING_SCHEMA = "main_staging"
```

---

## Connection Functions

### get_connection

Get a read-only connection to the analytics database.

```python
def get_connection() -> duckdb.DuckDBPyConnection
```

**Returns:**
- `DuckDBPyConnection`: DuckDB connection object

**Raises:**
- `FileNotFoundError`: If the database file doesn't exist

**Example:**

```python
from dashboard.data.queries import get_connection

conn = get_connection()
result = conn.execute("SELECT COUNT(*) FROM main_marts.dim_municipio").fetchone()
print(f"Total municipalities: {result[0]}")
```

---

## Statistics Functions

### get_database_stats

Get summary statistics from the database.

```python
def get_database_stats() -> dict
```

**Returns:**
- `dict`: Dictionary containing:
  - `total_municipios` (int): Number of municipalities
  - `total_estados` (int): Number of states
  - `total_populacao` (int): Total population
  - `avg_idhm` (float | None): Average IDHM

**Example:**

```python
from dashboard.data.queries import get_database_stats

stats = get_database_stats()
print(f"Municipalities: {stats['total_municipios']:,}")
print(f"Avg IDHM: {stats['avg_idhm']:.3f}")
```

**Output:**

```
Municipalities: 5,570
Avg IDHM: 0.659
```

---

## Municipality Functions

### load_municipalities_summary

Load municipality summary data for the dashboard.

```python
@lru_cache(maxsize=32)
def load_municipalities_summary(
    region: str = "All"
) -> pl.DataFrame
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `region` | str | "All" | Filter by region or "All" for all regions |

**Returns:**
- `pl.DataFrame`: Polars DataFrame with columns:
  - `sk_municipio`, `id_municipio_ibge`, `nome_municipio`
  - `sigla_uf`, `nome_uf`, `regiao`
  - `populacao`, `porte_municipio`
  - `is_capital`, `is_amazonia_legal`
  - `idhm_2010`, `idhm_educacao`, `idhm_longevidade`, `idhm_renda`
  - `faixa_idhm`, `ivs_2010`, `faixa_ivs`
  - `gini_2010`, `renda_per_capita_2010`, `esperanca_vida_2010`

**Example:**

```python
from dashboard.data.queries import load_municipalities_summary

# All municipalities
df = load_municipalities_summary()
print(f"Total: {len(df)} municipalities")

# Filter by region
df_northeast = load_municipalities_summary(region="Nordeste")
print(f"Northeast: {len(df_northeast)} municipalities")
```

**Note:** Results are cached using `@lru_cache` for performance.

---

### get_municipality_profile

Get detailed profile for a single municipality.

```python
@lru_cache(maxsize=128)
def get_municipality_profile(id_municipio: str) -> dict
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `id_municipio` | str | IBGE 7-digit municipality code |

**Returns:**
- `dict`: Dictionary with all dim_municipio columns, or empty dict if not found

**Example:**

```python
from dashboard.data.queries import get_municipality_profile

# Sao Paulo
profile = get_municipality_profile("3550308")

print(f"City: {profile['nome_municipio']}")
print(f"State: {profile['sigla_uf']}")
print(f"IDHM: {profile['idhm_2010']:.3f}")
print(f"Population: {profile['populacao']:,}")
```

**Output:**

```
City: Sao Paulo
State: SP
IDHM: 0.805
Population: 12,325,000
```

---

### search_municipalities

Search municipalities by name (partial match).

```python
def search_municipalities(
    query_str: str,
    limit: int = 20
) -> pl.DataFrame
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `query_str` | str | required | Search string (case-insensitive partial match) |
| `limit` | int | 20 | Maximum number of results |

**Returns:**
- `pl.DataFrame`: Matching municipalities with columns:
  - `id_municipio_ibge`, `nome_municipio`, `sigla_uf`
  - `regiao`, `populacao`, `idhm_2010`

**Example:**

```python
from dashboard.data.queries import search_municipalities

# Search for cities with "Paulo" in name
results = search_municipalities("Paulo")
print(results.select(["nome_municipio", "sigla_uf"]).head(5))
```

**Output:**

```
shape: (5, 2)
+-------------------+----------+
| nome_municipio    | sigla_uf |
+-------------------+----------+
| Sao Paulo         | SP       |
| Rio Paulo         | RS       |
| Paulo Afonso      | BA       |
| Sao Paulo do Potengi | RN    |
| Paulo Lopes       | SC       |
+-------------------+----------+
```

---

## Social Indicators Functions

### load_social_indicators

Load social indicators data from fct_indicadores_sociais.

```python
@lru_cache(maxsize=32)
def load_social_indicators(
    year: int = 2010,
    region: str = "All"
) -> pl.DataFrame
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `year` | int | 2010 | Census year (2000 or 2010) |
| `region` | str | "All" | Filter by region or "All" |

**Returns:**
- `pl.DataFrame`: Social indicators with 20+ columns including:
  - Municipality info (name, state, region, population)
  - IDHM components (idhm, idhm_educacao, idhm_longevidade, idhm_renda)
  - Demographics (esperanca_vida, taxa_analfabetismo_18_mais)
  - Income (renda_per_capita, indice_gini, taxa_pobreza)
  - Vulnerability (ivs, ivs_infraestrutura, ivs_capital_humano)
  - Labor (taxa_desemprego, taxa_informalidade)
  - Infrastructure (taxa_energia_eletrica, taxa_sem_saneamento)

**Example:**

```python
from dashboard.data.queries import load_social_indicators

# Load 2010 census data for Southeast
df = load_social_indicators(year=2010, region="Sudeste")

# Calculate correlation between IDHM and Gini
correlation = df.select([
    pl.corr("idhm", "indice_gini").alias("correlation")
]).item()
print(f"IDHM-Gini correlation: {correlation:.3f}")
```

---

### get_municipality_indicators_history

Get historical IDHM data for a municipality across census years.

```python
def get_municipality_indicators_history(
    id_municipio: str
) -> pl.DataFrame
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `id_municipio` | str | IBGE 7-digit municipality code |

**Returns:**
- `pl.DataFrame`: Time series with columns:
  - `ano`, `idhm`, `idhm_educacao`, `idhm_longevidade`, `idhm_renda`
  - `esperanca_vida`, `renda_per_capita`, `indice_gini`
  - `ivs`, `taxa_pobreza`

**Example:**

```python
from dashboard.data.queries import get_municipality_indicators_history

# Get Sao Paulo's IDHM evolution
history = get_municipality_indicators_history("3550308")
print(history.select(["ano", "idhm", "idhm_educacao", "idhm_renda"]))
```

**Output:**

```
shape: (2, 4)
+------+-------+----------------+------------+
| ano  | idhm  | idhm_educacao  | idhm_renda |
+------+-------+----------------+------------+
| 2000 | 0.733 | 0.678          | 0.845      |
| 2010 | 0.805 | 0.748          | 0.891      |
+------+-------+----------------+------------+
```

---

## Electoral Functions

### get_electoral_summary

Get electoral results summary.

```python
def get_electoral_summary(
    id_municipio: Optional[str] = None
) -> pl.DataFrame
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `id_municipio` | str | None | Optional filter by municipality IBGE code |

**Returns:**
- `pl.DataFrame`: Electoral data with columns:
  - `nome_municipio`, `sigla_uf`, `regiao`
  - `ano`, `turno`, `total_votos`, `total_candidatos`
  - `partido_vencedor`, `votos_vencedor`, `percentual_vencedor`

**Example:**

```python
from dashboard.data.queries import get_electoral_summary

# All elections for Sao Paulo
elections = get_electoral_summary(id_municipio="3550308")
print(elections.select(["ano", "turno", "partido_vencedor", "percentual_vencedor"]))
```

**Output:**

```
shape: (6, 4)
+------+-------+------------------+---------------------+
| ano  | turno | partido_vencedor | percentual_vencedor |
+------+-------+------------------+---------------------+
| 2024 | 2     | MDB              | 59.35               |
| 2020 | 2     | PSDB             | 59.38               |
| 2016 | 2     | PSDB             | 53.29               |
| 2012 | 1     | PT               | 55.57               |
| 2008 | 1     | DEM              | 60.72               |
| 2004 | 2     | PT               | 65.67               |
+------+-------+------------------+---------------------+
```

---

## Financial Functions

### get_financial_summary

Get municipal finance summary.

```python
def get_financial_summary(
    id_municipio: Optional[str] = None
) -> pl.DataFrame
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `id_municipio` | str | None | Optional filter by municipality IBGE code |

**Returns:**
- `pl.DataFrame`: Financial data with columns:
  - `nome_municipio`, `sigla_uf`, `regiao`, `populacao`
  - `ano`, `despesa_empenhada`, `despesa_liquidada`, `despesa_paga`
  - `receita_bruta`, `deducoes`, `receita_liquida`
  - `saldo_fiscal`, `taxa_execucao_percentual`

**Example:**

```python
from dashboard.data.queries import get_financial_summary

# Get Sao Paulo's finances
finances = get_financial_summary(id_municipio="3550308")

# Calculate average fiscal balance
avg_balance = finances["saldo_fiscal"].mean()
print(f"Avg fiscal balance: R$ {avg_balance:,.2f}")
```

---

## Rankings Functions

### get_rankings

Get municipality rankings by indicator.

```python
def get_rankings(
    indicator: str = "idhm_2010",
    limit: int = 100,
    ascending: bool = False
) -> pl.DataFrame
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `indicator` | str | "idhm_2010" | Column name to rank by |
| `limit` | int | 100 | Maximum number of results |
| `ascending` | bool | False | If True, rank lowest to highest |

**Valid Indicators:**
- `idhm_2010`, `idhm_educacao`, `idhm_longevidade`, `idhm_renda`
- `ivs_2010`, `gini_2010`, `renda_per_capita_2010`
- `esperanca_vida_2010`, `populacao`

**Returns:**
- `pl.DataFrame`: Rankings with columns:
  - `ranking`, `nome_municipio`, `sigla_uf`, `regiao`
  - `populacao`, `porte_municipio`, `valor`

**Example:**

```python
from dashboard.data.queries import get_rankings

# Top 10 by IDHM
top_idhm = get_rankings(indicator="idhm_2010", limit=10)
print(top_idhm.select(["ranking", "nome_municipio", "sigla_uf", "valor"]))

# Bottom 10 by IDHM (most vulnerable)
bottom_idhm = get_rankings(indicator="idhm_2010", limit=10, ascending=True)
```

**Output:**

```
shape: (10, 4)
+---------+-------------------+----------+-------+
| ranking | nome_municipio    | sigla_uf | valor |
+---------+-------------------+----------+-------+
| 1       | Sao Caetano do Sul| SP       | 0.862 |
| 2       | Aguas de Sao Pedro| SP       | 0.854 |
| 3       | Florianopolis     | SC       | 0.847 |
| 4       | Vitoria           | ES       | 0.845 |
| 5       | Balneario Camboriu| SC       | 0.845 |
+---------+-------------------+----------+-------+
```

---

## Correlation Functions

### get_correlation_data

Get data for correlation scatter plots.

```python
def get_correlation_data(
    x_indicator: str,
    y_indicator: str
) -> pl.DataFrame
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `x_indicator` | str | Column name for X axis |
| `y_indicator` | str | Column name for Y axis |

**Valid Indicators:**
- `idhm_2010`, `idhm_educacao`, `idhm_longevidade`, `idhm_renda`
- `ivs_2010`, `gini_2010`, `renda_per_capita_2010`
- `esperanca_vida_2010`, `populacao`

**Returns:**
- `pl.DataFrame`: Data for plotting with columns:
  - `nome_municipio`, `sigla_uf`, `regiao`, `porte_municipio`
  - `populacao`, `x_value`, `y_value`

**Example:**

```python
from dashboard.data.queries import get_correlation_data
import plotly.express as px

# Get IDHM vs Gini correlation data
df = get_correlation_data("idhm_2010", "gini_2010")

# Create scatter plot
fig = px.scatter(
    df.to_pandas(),
    x="x_value",
    y="y_value",
    color="regiao",
    hover_name="nome_municipio",
    labels={"x_value": "IDHM", "y_value": "Gini"}
)
fig.show()

# Calculate correlation coefficient
correlation = df.select([
    pl.corr("x_value", "y_value")
]).item()
print(f"Correlation: {correlation:.3f}")
```

---

## Aggregation Functions

### get_regional_summary

Get aggregated statistics by region.

```python
def get_regional_summary() -> pl.DataFrame
```

**Returns:**
- `pl.DataFrame`: Regional statistics with columns:
  - `regiao`
  - `num_municipios`: Count of municipalities
  - `total_populacao`: Sum of population
  - `avg_idhm`, `min_idhm`, `max_idhm`: IDHM statistics
  - `avg_ivs`: Average vulnerability index
  - `avg_gini`: Average Gini coefficient
  - `avg_esperanca_vida`: Average life expectancy

**Example:**

```python
from dashboard.data.queries import get_regional_summary

regions = get_regional_summary()
print(regions.select(["regiao", "num_municipios", "avg_idhm", "total_populacao"]))
```

**Output:**

```
shape: (5, 4)
+--------------+----------------+----------+------------------+
| regiao       | num_municipios | avg_idhm | total_populacao  |
+--------------+----------------+----------+------------------+
| Sudeste      | 1,668          | 0.699    | 89,012,240       |
| Sul          | 1,191          | 0.714    | 30,192,315       |
| Centro-Oeste | 467            | 0.690    | 16,707,336       |
| Norte        | 450            | 0.608    | 18,672,591       |
| Nordeste     | 1,794          | 0.590    | 57,374,243       |
+--------------+----------------+----------+------------------+
```

---

### get_state_summary

Get aggregated statistics by state.

```python
def get_state_summary() -> pl.DataFrame
```

**Returns:**
- `pl.DataFrame`: State statistics with columns:
  - `sigla_uf`, `nome_uf`, `regiao`
  - `num_municipios`, `total_populacao`
  - `avg_idhm`, `min_idhm`, `max_idhm`
  - `avg_ivs`, `avg_gini`, `avg_esperanca_vida`

**Example:**

```python
from dashboard.data.queries import get_state_summary

states = get_state_summary()
top_5 = states.head(5).select(["sigla_uf", "nome_uf", "avg_idhm"])
print(top_5)
```

---

### get_regions

Get list of unique regions.

```python
def get_regions() -> list[str]
```

**Returns:**
- `list[str]`: List of region names

**Example:**

```python
from dashboard.data.queries import get_regions

regions = get_regions()
print(regions)  # ['Centro-Oeste', 'Nordeste', 'Norte', 'Sudeste', 'Sul']
```

---

### get_states

Get list of unique states.

```python
def get_states() -> list[str]
```

**Returns:**
- `list[str]`: List of state abbreviations (sorted alphabetically)

**Example:**

```python
from dashboard.data.queries import get_states

states = get_states()
print(states)  # ['AC', 'AL', 'AM', 'AP', 'BA', ...]
```

---

## Performance Notes

### Caching

Several functions use `@lru_cache` for performance:

| Function | Cache Size | Notes |
|----------|------------|-------|
| `load_municipalities_summary` | 32 | Cached by region |
| `load_social_indicators` | 32 | Cached by year+region |
| `get_municipality_profile` | 128 | Cached by municipality ID |

**Clear cache programmatically:**

```python
load_municipalities_summary.cache_clear()
get_municipality_profile.cache_clear()
```

### Query Optimization Tips

1. **Filter early:** Use region/year parameters instead of filtering after load
2. **Select columns:** Use Polars `.select()` to reduce memory
3. **Batch lookups:** For multiple municipalities, use `search_municipalities` instead of multiple `get_municipality_profile` calls

---

## Error Handling

All query functions handle errors gracefully:

```python
from dashboard.data.queries import load_municipalities_summary

try:
    df = load_municipalities_summary()
    if df.is_empty():
        print("No data returned")
except FileNotFoundError:
    print("Database not found - run dbt build first")
except Exception as e:
    print(f"Query failed: {e}")
```

---

## Usage in Streamlit

```python
import streamlit as st
from dashboard.data.queries import (
    load_municipalities_summary,
    get_rankings,
    get_correlation_data,
)

# Cached data load
@st.cache_data
def load_data(region):
    return load_municipalities_summary(region).to_pandas()

# Sidebar filter
region = st.sidebar.selectbox("Region", ["All", "Sudeste", "Sul", "Nordeste"])

# Load and display
df = load_data(region)
st.dataframe(df)
```
