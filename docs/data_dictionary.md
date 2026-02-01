# Data Dictionary

> Complete reference of all tables, columns, and business definitions for the Brazilian Municipalities Analytics project.

## Table of Contents

1. [Dimension Tables](#dimension-tables)
   - [dim_municipio](#dim_municipio)
   - [dim_calendario](#dim_calendario)
2. [Fact Tables](#fact-tables)
   - [fct_indicadores_sociais](#fct_indicadores_sociais)
   - [fct_eleicoes](#fct_eleicoes)
   - [fct_financas_municipais](#fct_financas_municipais)
3. [Staging Tables](#staging-tables)
4. [Source Tables](#source-tables)
5. [Key Relationships](#key-relationships)
6. [Business Glossary](#business-glossary)

---

## Dimension Tables

### dim_municipio

**Description:** Conformed municipality dimension containing geographic, demographic, and social development information for all 5,570+ Brazilian municipalities. This is the central dimension linking all fact tables.

**Schema:** `main_marts`
**Grain:** One row per municipality
**Row Count:** ~5,570

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `sk_municipio` | VARCHAR | No | Surrogate key (MD5 hash of id_municipio_ibge) |
| `id_municipio_ibge` | VARCHAR(7) | No | IBGE 7-digit municipality code (natural key) |
| `id_municipio_tse` | VARCHAR(5) | Yes | TSE municipality code for electoral data joins |
| `id_municipio_6` | VARCHAR(6) | Yes | IBGE 6-digit code (without check digit) |
| `id_municipio_rf` | VARCHAR | Yes | Receita Federal (tax authority) code |
| `id_municipio_bcb` | VARCHAR | Yes | Central Bank (BCB) code |
| `nome_municipio` | VARCHAR | No | Official municipality name |
| `sigla_uf` | VARCHAR(2) | No | State abbreviation (e.g., SP, RJ, MG) |
| `nome_uf` | VARCHAR | Yes | Full state name |
| `regiao` | VARCHAR | Yes | Geographic region (Norte, Nordeste, Sudeste, Sul, Centro-Oeste) |
| `mesorregiao` | VARCHAR | Yes | IBGE mesoregion name |
| `microrregiao` | VARCHAR | Yes | IBGE microregion name |
| `id_regiao_imediata` | VARCHAR | Yes | IBGE immediate geographic region ID |
| `nome_regiao_imediata` | VARCHAR | Yes | Immediate geographic region name |
| `id_regiao_intermediaria` | VARCHAR | Yes | IBGE intermediate geographic region ID |
| `nome_regiao_intermediaria` | VARCHAR | Yes | Intermediate geographic region name |
| `id_regiao_metropolitana` | VARCHAR | Yes | Metropolitan region ID (if applicable) |
| `nome_regiao_metropolitana` | VARCHAR | Yes | Metropolitan region name |
| `is_capital` | BOOLEAN | Yes | True if municipality is a state capital |
| `is_amazonia_legal` | BOOLEAN | Yes | True if municipality is in Legal Amazon |
| `centroide` | VARCHAR | Yes | Geographic centroid (geometry string) |
| `ddd` | VARCHAR | Yes | Phone area code (DDD) |
| `populacao` | BIGINT | Yes | Latest population estimate |
| `ano_populacao` | INTEGER | Yes | Year of population estimate |
| `porte_municipio` | VARCHAR | Yes | Size classification (Micro, Pequeno, Medio, Grande, Metropole) |
| `idhm_2010` | DECIMAL(5,4) | Yes | Human Development Index (2010 census) |
| `idhm_educacao` | DECIMAL(5,4) | Yes | IDHM Education component |
| `idhm_longevidade` | DECIMAL(5,4) | Yes | IDHM Longevity component |
| `idhm_renda` | DECIMAL(5,4) | Yes | IDHM Income component |
| `faixa_idhm` | VARCHAR | Yes | IDHM classification (Muito Alto, Alto, Medio, Baixo, Muito Baixo) |
| `ivs_2010` | DECIMAL(5,4) | Yes | Social Vulnerability Index (2010) |
| `faixa_ivs` | VARCHAR | Yes | IVS classification (Muito Baixa to Muito Alta) |
| `gini_2010` | DECIMAL(5,4) | Yes | Gini coefficient (income inequality) |
| `renda_per_capita_2010` | DECIMAL(12,2) | Yes | Per capita income in BRL (2010) |
| `esperanca_vida_2010` | DECIMAL(5,2) | Yes | Life expectancy at birth (years) |
| `_loaded_at` | TIMESTAMP | Yes | Record load timestamp |

**Size Classifications (porte_municipio):**

| Classification | Population Range |
|---------------|------------------|
| Micro (< 5k) | Less than 5,000 |
| Pequeno (5k-20k) | 5,000 to 19,999 |
| Medio (20k-100k) | 20,000 to 99,999 |
| Grande (100k-500k) | 100,000 to 499,999 |
| Metropole (500k+) | 500,000 or more |

**IDHM Classifications (faixa_idhm):**

| Classification | IDHM Range |
|---------------|------------|
| Muito Alto | 0.800 - 1.000 |
| Alto | 0.700 - 0.799 |
| Medio | 0.600 - 0.699 |
| Baixo | 0.500 - 0.599 |
| Muito Baixo | 0.000 - 0.499 |

---

### dim_calendario

**Description:** Calendar dimension with year-level grain containing flags for census years, election years, and data availability periods.

**Schema:** `main_marts`
**Grain:** One row per year
**Row Count:** 41 (1990-2030)

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `sk_ano` | INTEGER | No | Surrogate key (same as year) |
| `ano` | INTEGER | No | Calendar year |
| `decada` | INTEGER | Yes | Decade (e.g., 1990, 2000, 2010) |
| `is_ano_censo` | BOOLEAN | Yes | True if IBGE census year (1991, 2000, 2010, 2022) |
| `is_ano_eleitoral_municipal` | BOOLEAN | Yes | True if municipal election year (divisible by 4) |
| `has_idhm_data` | BOOLEAN | Yes | True if IDHM data available (1991, 2000, 2010) |
| `has_siconfi_data` | BOOLEAN | Yes | True if fiscal data available (2013+) |
| `_loaded_at` | TIMESTAMP | Yes | Record load timestamp |

**Census Years:** 1991, 2000, 2010, 2022
**Municipal Election Years:** 1996, 2000, 2004, 2008, 2012, 2016, 2020, 2024
**SICONFI Data Available:** 2013 onwards

---

## Fact Tables

### fct_indicadores_sociais

**Description:** Periodic snapshot fact table containing socio-economic indicators for Brazilian municipalities across census years. Data sourced from IPEA AVS (Atlas da Vulnerabilidade Social).

**Schema:** `main_marts`
**Grain:** One row per municipality per census year
**Row Count:** ~11,000 (5,570 x 2 census years)

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `sk_municipio` | VARCHAR | No | Foreign key to dim_municipio |
| `sk_ano` | INTEGER | No | Foreign key to dim_calendario |
| `id_municipio` | VARCHAR(7) | Yes | IBGE code (for debugging) |
| `ano` | INTEGER | Yes | Census year (2000 or 2010) |
| `idhm` | DECIMAL(5,4) | Yes | Human Development Index (0-1 scale) |
| `idhm_educacao` | DECIMAL(5,4) | Yes | Education component of IDHM |
| `idhm_longevidade` | DECIMAL(5,4) | Yes | Longevity component of IDHM |
| `idhm_renda` | DECIMAL(5,4) | Yes | Income component of IDHM |
| `idhm_subescolaridade` | DECIMAL(5,4) | Yes | Schooling sub-index |
| `idhm_subfrequencia` | DECIMAL(5,4) | Yes | School attendance sub-index |
| `esperanca_vida` | DECIMAL(5,2) | Yes | Life expectancy at birth (years) |
| `taxa_envelhecimento` | DECIMAL(5,2) | Yes | Aging rate (% population 65+) |
| `taxa_analfabetismo_18_mais` | DECIMAL(5,2) | Yes | Illiteracy rate (18+ years) |
| `taxa_fundamental_completo` | DECIMAL(5,2) | Yes | % with complete primary education (18+) |
| `taxa_medio_completo` | DECIMAL(5,2) | Yes | % with complete secondary education (18-20) |
| `renda_per_capita` | DECIMAL(12,2) | Yes | Per capita income in BRL |
| `indice_gini` | DECIMAL(5,4) | Yes | Gini coefficient (0=equal, 1=unequal) |
| `taxa_pobreza` | DECIMAL(5,2) | Yes | Poverty rate (% vulnerable population) |
| `ivs` | DECIMAL(5,4) | Yes | Social Vulnerability Index (0-1) |
| `ivs_infraestrutura` | DECIMAL(5,4) | Yes | IVS Urban Infrastructure component |
| `ivs_capital_humano` | DECIMAL(5,4) | Yes | IVS Human Capital component |
| `ivs_renda_trabalho` | DECIMAL(5,4) | Yes | IVS Income and Labor component |
| `taxa_desemprego` | DECIMAL(5,2) | Yes | Unemployment rate (18+) |
| `taxa_informalidade` | DECIMAL(5,2) | Yes | Informal employment rate (18+) |
| `taxa_energia_eletrica` | DECIMAL(5,2) | Yes | % with electricity access |
| `taxa_sem_saneamento` | DECIMAL(5,2) | Yes | % without water/sewage |
| `idhm_percentual` | DECIMAL(5,2) | Yes | IDHM as percentage (0-100) |
| `ivs_percentual` | DECIMAL(5,2) | Yes | IVS as percentage (0-100) |
| `_loaded_at` | TIMESTAMP | Yes | Record load timestamp |

**Note:** Data aggregated from UDH (Human Development Units) to municipality level using mean values.

---

### fct_eleicoes

**Description:** Electoral results fact table containing mayoral election results by municipality. Aggregated to show winning party and vote distribution per election.

**Schema:** `main_marts`
**Grain:** One row per municipality per election year per turno (round)
**Row Count:** ~150,000

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `sk_municipio` | VARCHAR | No | Foreign key to dim_municipio |
| `sk_ano` | INTEGER | No | Foreign key to dim_calendario |
| `id_municipio` | VARCHAR(7) | Yes | IBGE code (for debugging) |
| `ano` | INTEGER | Yes | Election year |
| `turno` | INTEGER | Yes | Election round (1 or 2) |
| `total_votos` | BIGINT | Yes | Total votes cast |
| `total_candidatos` | INTEGER | Yes | Number of candidates |
| `total_partidos` | INTEGER | Yes | Number of parties |
| `partido_vencedor` | VARCHAR | Yes | Winning party abbreviation |
| `votos_vencedor` | BIGINT | Yes | Votes received by winner |
| `percentual_vencedor` | DECIMAL(5,2) | Yes | Winner's vote percentage |
| `indice_competicao` | INTEGER | Yes | Competition index (number of candidates) |
| `_loaded_at` | TIMESTAMP | Yes | Record load timestamp |

**Election Years:** 1996, 2000, 2004, 2008, 2012, 2016, 2020, 2024
**Turno:** 1 (first round), 2 (runoff - cities with 200k+ voters)

---

### fct_financas_municipais

**Description:** Municipal finances fact table combining expenses and revenues aggregated at municipality-year level. Values in BRL at nominal prices.

**Schema:** `main_marts`
**Grain:** One row per municipality per fiscal year
**Row Count:** ~60,000 (5,570 x 10+ years)

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `sk_municipio` | VARCHAR | No | Foreign key to dim_municipio |
| `sk_ano` | INTEGER | No | Foreign key to dim_calendario |
| `id_municipio` | VARCHAR(7) | Yes | IBGE code (for debugging) |
| `ano` | INTEGER | Yes | Fiscal year |
| `despesa_empenhada` | DECIMAL(18,2) | Yes | Committed expenses (reserved budget) |
| `despesa_liquidada` | DECIMAL(18,2) | Yes | Accrued expenses (services delivered) |
| `despesa_paga` | DECIMAL(18,2) | Yes | Paid expenses (actual disbursement) |
| `receita_bruta` | DECIMAL(18,2) | Yes | Gross revenues collected |
| `deducoes` | DECIMAL(18,2) | Yes | Total deductions |
| `deducao_fundeb` | DECIMAL(18,2) | Yes | FUNDEB education fund deductions |
| `receita_liquida` | DECIMAL(18,2) | Yes | Net revenue (gross - deductions) |
| `saldo_fiscal` | DECIMAL(18,2) | Yes | Fiscal balance (revenue - expenses) |
| `taxa_execucao_percentual` | DECIMAL(5,2) | Yes | Execution rate (paid/committed %) |
| `_loaded_at` | TIMESTAMP | Yes | Record load timestamp |

**Budget Execution Stages:**

| Stage | Portuguese | Description |
|-------|------------|-------------|
| Empenhado | Committed | Budget reserved for obligation |
| Liquidado | Accrued | Services/goods delivered and verified |
| Pago | Paid | Actual payment made |

**Data Availability:** 2013 onwards (SICONFI system)

---

## Staging Tables

### stg_municipios

**Schema:** `main_staging` (view)
**Source:** `data/raw/municipio.parquet`

| Column | Description |
|--------|-------------|
| id_municipio_ibge | IBGE 7-digit code |
| id_municipio_tse | TSE code |
| nome_municipio | Trimmed municipality name |
| sigla_uf | Uppercase state abbreviation |
| nome_uf | State name |
| regiao | Geographic region |
| mesorregiao | IBGE mesoregion |
| microrregiao | IBGE microregion |
| is_capital | Boolean capital flag |
| is_amazonia_legal | Boolean Amazon flag |
| centroide | Geographic centroid |
| _loaded_at | Load timestamp |

### stg_populacao

**Schema:** `main_staging` (view)
**Source:** `data/raw/populacao.parquet`

| Column | Description |
|--------|-------------|
| id_municipio | IBGE 7-digit code |
| ano | Reference year (1991-2025) |
| sigla_uf | State abbreviation |
| populacao | Population count |
| _loaded_at | Load timestamp |

### stg_idhm

**Schema:** `main_staging` (view)
**Source:** `data/raw/idhm.parquet`

| Column | Description |
|--------|-------------|
| id_municipio | IBGE 7-digit code |
| ano | Census year (2000, 2010) |
| sigla_uf | State abbreviation |
| idhm | Human Development Index |
| idhm_educacao | Education component |
| idhm_longevidade | Longevity component |
| idhm_renda | Income component |
| (20+ additional indicators) | See fct_indicadores_sociais |
| udh_count | Number of UDHs aggregated |
| _loaded_at | Load timestamp |

**Filters Applied:**
- `raca_cor = 'total'` (total population, not by race)
- `sexo = 'total'` (total population, not by gender)
- `localizacao = 'total'` (total area, not urban/rural split)

### stg_eleicoes

**Schema:** `main_staging` (view)
**Source:** `data/raw/resultados_candidato_municipio.parquet`

| Column | Description |
|--------|-------------|
| ano | Election year |
| turno | Election round (1 or 2) |
| id_municipio | IBGE 7-digit code |
| id_municipio_tse | TSE code |
| sigla_uf | State abbreviation |
| numero_partido | Party number |
| sigla_partido | Party abbreviation |
| cargo | Position (filtered for 'prefeito') |
| numero_candidato | Candidate number |
| resultado | Election result |
| votos | Vote count |
| is_eleito | Boolean elected flag |
| is_segundo_turno | Boolean second round flag |
| _loaded_at | Load timestamp |

### stg_despesas

**Schema:** `main_staging` (view)
**Source:** `data/raw/municipio_despesas_funcao.parquet`

| Column | Description |
|--------|-------------|
| ano | Fiscal year |
| id_municipio | IBGE 7-digit code |
| sigla_uf | State abbreviation |
| estagio | Budget execution stage |
| conta | Account/function |
| valor | Amount in BRL |
| tipo_estagio | Classified stage (empenhado, liquidado, pago) |
| _loaded_at | Load timestamp |

### stg_receitas

**Schema:** `main_staging` (view)
**Source:** `data/raw/municipio_receitas_orcamentarias.parquet`

| Column | Description |
|--------|-------------|
| ano | Fiscal year |
| id_municipio | IBGE 7-digit code |
| sigla_uf | State abbreviation |
| estagio | Revenue stage |
| conta | Account/source |
| valor | Amount in BRL |
| tipo_receita | Revenue type classification |
| is_deducao | Boolean deduction flag |
| _loaded_at | Load timestamp |

---

## Source Tables

Raw data extracted from Base dos Dados BigQuery:

| Source Table | BigQuery Path | Description |
|--------------|---------------|-------------|
| municipio | `basedosdados.br_bd_diretorios_brasil.municipio` | Municipality directory with code crosswalk |
| populacao | `basedosdados.br_ibge_populacao.municipio` | Historical population (1991-2025) |
| idhm | `basedosdados.br_pnud_atlas.municipio` | IDHM and 200+ indicators |
| eleicoes | `basedosdados.br_tse_eleicoes.resultados_candidato_municipio` | Electoral results (mayors only) |
| despesas | `basedosdados.br_me_siconfi.municipio_despesas_funcao` | Municipal expenses |
| receitas | `basedosdados.br_me_siconfi.municipio_receitas_orcamentarias` | Municipal revenues |
| saneamento | `basedosdados.br_mdr_snis.municipio` | Sanitation indicators |

---

## Key Relationships

### Entity Relationship Diagram

```
+------------------+        +-------------------------+
|  dim_municipio   |        |  fct_indicadores_sociais|
+------------------+        +-------------------------+
| PK: sk_municipio |<-------| FK: sk_municipio        |
| NK: id_municipio |        | FK: sk_ano              |
|     _ibge        |        +------------+------------+
+--------+---------+                     |
         |                               |
         |        +------------------+   |
         |        |  dim_calendario  |   |
         |        +------------------+   |
         |        | PK: sk_ano       |<--+
         |        | NK: ano          |<--+
         |        +--------+---------+   |
         |                 |             |
         |                 |             |
         |    +------------v-------------+
         |    |      fct_eleicoes        |
         +--->| FK: sk_municipio         |
         |    | FK: sk_ano               |
         |    +--------------------------+
         |
         |    +--------------------------+
         +--->|  fct_financas_municipais |
              | FK: sk_municipio         |
              | FK: sk_ano               |
              +--------------------------+
```

### Join Keys

| From Table | To Table | Join Columns |
|------------|----------|--------------|
| fct_indicadores_sociais | dim_municipio | sk_municipio |
| fct_indicadores_sociais | dim_calendario | sk_ano |
| fct_eleicoes | dim_municipio | sk_municipio |
| fct_eleicoes | dim_calendario | sk_ano |
| fct_financas_municipais | dim_municipio | sk_municipio |
| fct_financas_municipais | dim_calendario | sk_ano |

---

## Business Glossary

### IDHM (Indice de Desenvolvimento Humano Municipal)

The Municipal Human Development Index, adapted from the UN's HDI methodology for Brazilian municipalities. Calculated as the geometric mean of three components:

- **IDHM Educacao (Education):** Combines adult schooling (weight 1) and school-age attendance (weight 2)
- **IDHM Longevidade (Longevity):** Based on life expectancy at birth
- **IDHM Renda (Income):** Based on per capita income (log scale)

Scale: 0 to 1 (higher is better)

### IVS (Indice de Vulnerabilidade Social)

Social Vulnerability Index measuring inadequate access to assets and opportunities:

- **IVS Infraestrutura:** Urban infrastructure (housing, sanitation, transportation)
- **IVS Capital Humano:** Health and education indicators
- **IVS Renda e Trabalho:** Income levels and employment conditions

Scale: 0 to 1 (higher means more vulnerable)

### Gini Coefficient

Measure of income inequality within a municipality:
- 0 = Perfect equality (everyone has same income)
- 1 = Perfect inequality (one person has all income)

Brazilian municipalities typically range from 0.40 to 0.70.

### UDH (Unidades de Desenvolvimento Humano)

Human Development Units - sub-municipal geographic areas defined by IPEA for detailed analysis. Data is aggregated to municipality level using mean values.

### SICONFI

Sistema de Informacoes Contabeis e Fiscais do Setor Publico Brasileiro - the accounting system for Brazilian public sector entities, managed by the National Treasury.

### FUNDEB

Fundo de Manutencao e Desenvolvimento da Educacao Basica - Brazil's education fund that redistributes tax revenue to states and municipalities based on enrollment.

### FPM (Fundo de Participacao dos Municipios)

Municipal Participation Fund - federal transfer to municipalities based on population.

---

## Data Quality Notes

1. **IDHM Data:** Only available for census years (2000, 2010). Next update expected with 2022 census results.

2. **Electoral Data:** Complete for municipal elections since 1996. Some historical records may have missing party information.

3. **Financial Data:** SICONFI data available from 2013. Earlier years have limited coverage.

4. **Population:** Annual estimates from IBGE. Census years (1991, 2000, 2010, 2022) have exact counts.

5. **Municipality Changes:** Some municipalities were created, merged, or renamed over time. The directory uses current (2024) boundaries.
