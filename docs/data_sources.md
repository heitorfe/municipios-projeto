# Data Sources Documentation

> Complete reference of all external data sources used in the Brazilian Municipalities Analytics project.

**Last Updated:** 2026-02-01
**Data Provider:** [Base dos Dados](https://basedosdados.org) (BigQuery Public Datasets)

---

## Table of Contents

1. [Overview](#overview)
2. [Data Architecture](#data-architecture)
3. [Core Data Sources](#core-data-sources)
   - [Municipality Directory](#1-municipality-directory)
   - [Population](#2-population)
   - [Municipal GDP](#3-municipal-gdp)
   - [Human Development (IDHM)](#4-human-development-idhm)
4. [Electoral Data Sources](#electoral-data-sources)
   - [Election Results](#5-election-results)
   - [Candidates](#6-candidates)
   - [Political Parties](#7-political-parties)
5. [Fiscal Data Sources](#fiscal-data-sources)
   - [Municipal Expenses](#8-municipal-expenses)
   - [Municipal Revenues](#9-municipal-revenues)
6. [Education Data Sources](#education-data-sources)
   - [IDEB (Education Quality Index)](#10-ideb-education-quality-index)
   - [School Census](#11-school-census)
7. [Health Data Sources](#health-data-sources)
   - [Mortality Data](#12-mortality-data)
   - [Live Births](#13-live-births)
8. [Social Program Data Sources](#social-program-data-sources)
   - [Bolsa Família](#14-bolsa-família)
   - [Cadastro Único](#15-cadastro-único)
9. [Infrastructure Data Sources](#infrastructure-data-sources)
   - [Sanitation (SNIS)](#16-sanitation-snis)
10. [Seed Data (Internal)](#seed-data-internal)
    - [Political Parties Ideology](#17-political-parties-ideology)
11. [Data Extraction Guide](#data-extraction-guide)
12. [Data Quality & Limitations](#data-quality--limitations)

---

## Overview

This project combines **17 data sources** from multiple Brazilian government agencies, accessed through Base dos Dados BigQuery. The data enables analysis of:

| Domain | Key Indicators | Time Coverage |
|--------|---------------|---------------|
| **Demographics** | Population, geography, urbanization | 1991-2025 |
| **Social Development** | IDHM, IVS, Gini, poverty | 1991, 2000, 2010 |
| **Politics** | Elections, parties, ideology | 1996-2024 |
| **Public Finance** | Revenue, expenses, transfers | 2013-2023 |
| **Education** | IDEB scores, enrollment, teachers | 2005-2023 |
| **Health** | Mortality, infant deaths, births | 1996-2022 |
| **Social Programs** | Bolsa Família recipients, values | 2004-2023 |
| **Infrastructure** | Water, sewage, waste management | 2000-2022 |

---

## Data Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           BASE DOS DADOS (BigQuery)                         │
│                     basedosdados.{dataset}.{table}                          │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          BRONZE LAYER (data/raw/)                           │
│                          Parquet files, raw data                            │
│  municipio.parquet, populacao.parquet, eleicoes.parquet, ideb.parquet ...   │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SILVER LAYER (dbt staging)                          │
│                    stg_municipios, stg_eleicoes, etc.                       │
│                    Cleaning, typing, standardization                        │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          GOLD LAYER (dbt marts)                             │
│              dim_municipio, fct_eleicoes, fct_mandato_completo              │
│                    Business logic, aggregations, metrics                    │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Core Data Sources

### 1. Municipality Directory

> **Source Agency:** IBGE (Brazilian Institute of Geography and Statistics)

| Attribute | Value |
|-----------|-------|
| **BigQuery Table** | `basedosdados.br_bd_diretorios_brasil.municipio` |
| **Local File** | `data/raw/municipio.parquet` |
| **Grain** | One row per municipality |
| **Row Count** | ~5,570 |
| **Update Frequency** | Yearly (when new municipalities are created) |

**Description:**
Master directory of all Brazilian municipalities with code crosswalks between different government systems (IBGE, TSE, BCB, Receita Federal).

**Key Columns:**

| Column | Type | Description |
|--------|------|-------------|
| `id_municipio` | VARCHAR(7) | IBGE 7-digit code (primary key) |
| `id_municipio_6` | VARCHAR(6) | IBGE 6-digit code (without check digit) |
| `id_municipio_tse` | VARCHAR(5) | Electoral court (TSE) code |
| `id_municipio_bcb` | VARCHAR | Central Bank code |
| `id_municipio_rf` | VARCHAR | Tax authority (Receita Federal) code |
| `nome` | VARCHAR | Municipality official name |
| `sigla_uf` | VARCHAR(2) | State abbreviation (SP, RJ, MG...) |
| `id_uf` | VARCHAR(2) | State IBGE code |
| `nome_regiao` | VARCHAR | Geographic region (Norte, Nordeste, Sudeste, Sul, Centro-Oeste) |
| `nome_mesorregiao` | VARCHAR | IBGE mesoregion |
| `nome_microrregiao` | VARCHAR | IBGE microregion |
| `capital` | BOOLEAN | Is state capital |
| `amazonia_legal` | BOOLEAN | Is in Legal Amazon biome |

**Use Cases:**
- Join key between electoral, fiscal, and social datasets
- Geographic aggregations (state, region, mesoregion)
- Filter for capitals, Amazon municipalities

---

### 2. Population

> **Source Agency:** IBGE

| Attribute | Value |
|-----------|-------|
| **BigQuery Table** | `basedosdados.br_ibge_populacao.municipio` |
| **Local File** | `data/raw/populacao.parquet` |
| **Grain** | One row per municipality per year |
| **Coverage** | 1991-2025 |
| **Update Frequency** | Annual (estimates), Decennial (census) |

**Description:**
Historical population time series combining census counts (1991, 2000, 2010, 2022) with annual intercensus estimates.

**Key Columns:**

| Column | Type | Description |
|--------|------|-------------|
| `id_municipio` | VARCHAR(7) | IBGE municipality code |
| `ano` | INTEGER | Reference year |
| `populacao` | BIGINT | Population count |

**Data Quality Notes:**
- Census years (1991, 2000, 2010, 2022) have actual counts
- Other years are IBGE estimates based on demographic models
- Some small municipalities may have estimation noise

**Use Cases:**
- Per capita calculations (revenue, expenses, transfers)
- Municipality size classification
- Population growth analysis

---

### 3. Municipal GDP

> **Source Agency:** IBGE

| Attribute | Value |
|-----------|-------|
| **BigQuery Table** | `basedosdados.br_ibge_pib.municipio` |
| **Local File** | `data/raw/pib_municipio.parquet` |
| **Grain** | One row per municipality per year |
| **Coverage** | 2002-2021 |
| **Update Frequency** | Annual (2-year lag) |

**Description:**
Municipal Gross Domestic Product with sector breakdown (agriculture, industry, services, public administration).

**Key Columns:**

| Column | Type | Description |
|--------|------|-------------|
| `id_municipio` | VARCHAR(7) | IBGE municipality code |
| `ano` | INTEGER | Reference year |
| `pib` | DECIMAL | Total GDP in BRL (current prices) |
| `pib_per_capita` | DECIMAL | Per capita GDP in BRL |
| `va_agropecuaria` | DECIMAL | Agricultural value added |
| `va_industria` | DECIMAL | Industrial value added |
| `va_servicos` | DECIMAL | Services value added |
| `va_adm_publica` | DECIMAL | Public administration value added |

**Use Cases:**
- Economic structure analysis (agricultural vs industrial vs service economies)
- Wealth comparisons between municipalities
- Economic development controls in regression models

---

### 4. Human Development (IDHM)

> **Source Agency:** PNUD (UN Development Programme), IPEA, Fundação João Pinheiro

| Attribute | Value |
|-----------|-------|
| **BigQuery Table** | `basedosdados.br_pnud_atlas.municipio` |
| **Local File** | `data/raw/idhm.parquet` |
| **Grain** | One row per municipality per census year |
| **Coverage** | 1991, 2000, 2010 |
| **Update Frequency** | Decennial (census-based) |

**Description:**
The Atlas Brasil dataset contains the Municipal Human Development Index (IDHM) and 200+ socioeconomic indicators derived from census microdata.

**Key Columns:**

| Column | Type | Description |
|--------|------|-------------|
| `id_municipio` | VARCHAR(7) | IBGE municipality code |
| `ano` | INTEGER | Census year (1991, 2000, 2010) |
| `idhm` | DECIMAL(5,4) | IDHM composite index (0-1) |
| `idhm_e` | DECIMAL(5,4) | IDHM Education component |
| `idhm_l` | DECIMAL(5,4) | IDHM Longevity component |
| `idhm_r` | DECIMAL(5,4) | IDHM Income component |
| `espvida` | DECIMAL | Life expectancy at birth (years) |
| `rdpc` | DECIMAL | Per capita income (BRL) |
| `gini` | DECIMAL(5,4) | Gini coefficient (inequality) |
| `pobre` | DECIMAL | Poverty rate (%) |
| `t_des18m` | DECIMAL | Unemployment rate 18+ (%) |
| `agua_esgoto` | DECIMAL | % with water and sewage |
| `t_env` | DECIMAL | Aging rate (% population 65+) |

**IDHM Scale:**

| Range | Classification |
|-------|----------------|
| 0.800 - 1.000 | Very High |
| 0.700 - 0.799 | High |
| 0.600 - 0.699 | Medium |
| 0.500 - 0.599 | Low |
| 0.000 - 0.499 | Very Low |

**Important Notes:**
- ⚠️ **Sparse Data**: Only available for census years (decade intervals)
- Data is aggregated from UDH (Human Development Units) to municipality level
- 2022 census data expected to be released 2024-2025
- Filter for `raca_cor='total'`, `sexo='total'`, `localizacao='total'` for aggregate values

---

## Electoral Data Sources

### 5. Election Results

> **Source Agency:** TSE (Superior Electoral Court)

| Attribute | Value |
|-----------|-------|
| **BigQuery Table** | `basedosdados.br_tse_eleicoes.resultados_candidato_municipio` |
| **Local File** | `data/raw/resultados_candidato_municipio.parquet` |
| **Grain** | One row per candidate per municipality per election |
| **Coverage** | 1996-2024 |
| **Extraction Filter** | `cargo = 'prefeito'` (mayors only) |

**Description:**
Complete electoral results for mayoral elections at candidate level, including vote counts, party affiliation, and election outcomes.

**Key Columns:**

| Column | Type | Description |
|--------|------|-------------|
| `ano` | INTEGER | Election year |
| `turno` | INTEGER | Round (1 = first, 2 = runoff) |
| `id_municipio` | VARCHAR(7) | IBGE code |
| `id_municipio_tse` | VARCHAR(5) | TSE code |
| `numero_candidato` | VARCHAR | Candidate ballot number |
| `nome_candidato` | VARCHAR | Candidate name |
| `numero_partido` | INTEGER | Party number |
| `sigla_partido` | VARCHAR | Party abbreviation |
| `resultado` | VARCHAR | Result (eleito, nao_eleito, 2_turno) |
| `votos` | BIGINT | Vote count |
| `sequencial_candidato` | VARCHAR | Unique candidate ID |

**Electoral Calendar (Municipal):**
- 1996, 2000, 2004, 2008, 2012, 2016, 2020, 2024
- Elections held first Sunday of October
- Runoff (turno 2) in municipalities with 200k+ voters if no majority

**Use Cases:**
- Winning party identification per municipality
- Electoral competition metrics
- Vote concentration analysis
- Political continuity/alternance patterns

---

### 6. Candidates

> **Source Agency:** TSE

| Attribute | Value |
|-----------|-------|
| **BigQuery Table** | `basedosdados.br_tse_eleicoes.candidatos` |
| **Local File** | `data/raw/candidatos.parquet` |
| **Grain** | One row per candidate per election |
| **Coverage** | 1994-2024 |
| **Extraction Filter** | `cargo = 'prefeito'` (mayors only) |

**Description:**
Candidate personal characteristics including demographics, education, occupation, and declared assets.

**Key Columns:**

| Column | Type | Description |
|--------|------|-------------|
| `ano` | INTEGER | Election year |
| `sequencial_candidato` | VARCHAR | Unique candidate ID |
| `nome_candidato` | VARCHAR | Full name |
| `numero_partido` | INTEGER | Party number |
| `sigla_partido` | VARCHAR | Party abbreviation |
| `genero` | VARCHAR | Gender (masculino, feminino) |
| `raca_cor` | VARCHAR | Race/color (branca, parda, preta, amarela, indigena) |
| `data_nascimento` | DATE | Birth date |
| `grau_instrucao` | VARCHAR | Education level |
| `ocupacao` | VARCHAR | Occupation/profession |
| `total_bens` | DECIMAL | Total declared assets (BRL) |

**Use Cases:**
- Mayor demographic profiles (gender, age, education)
- Wealth analysis of elected officials
- Occupation background analysis
- Representation studies

---

### 7. Political Parties

> **Source Agency:** TSE

| Attribute | Value |
|-----------|-------|
| **BigQuery Table** | `basedosdados.br_tse_eleicoes.partidos` |
| **Local File** | `data/raw/partidos.parquet` |
| **Grain** | One row per party |
| **Coverage** | All registered parties |

**Description:**
Official registry of Brazilian political parties with creation and dissolution dates.

**Key Columns:**

| Column | Type | Description |
|--------|------|-------------|
| `numero_partido` | INTEGER | Party ballot number |
| `sigla_partido` | VARCHAR | Party abbreviation (PT, PSDB, MDB...) |
| `nome_partido` | VARCHAR | Full party name |
| `data_criacao` | DATE | Party registration date |
| `data_extincao` | DATE | Party dissolution date (if applicable) |

**Note:** This source provides official TSE data. Party ideology classification comes from the internal seed file `seed_partidos.csv` (see [Section 17](#17-political-parties-ideology)).

---

## Fiscal Data Sources

### 8. Municipal Expenses

> **Source Agency:** STN/Tesouro Nacional via SICONFI

| Attribute | Value |
|-----------|-------|
| **BigQuery Table** | `basedosdados.br_me_siconfi.municipio_despesas_funcao` |
| **Local File** | `data/raw/municipio_despesas_funcao.parquet` |
| **Grain** | One row per municipality per year per function per stage |
| **Coverage** | 2013-2023 |
| **Update Frequency** | Annual |

**Description:**
Municipal budget execution data by government function (education, health, administration, etc.) and execution stage (committed, accrued, paid).

**Key Columns:**

| Column | Type | Description |
|--------|------|-------------|
| `ano` | INTEGER | Fiscal year |
| `id_municipio` | VARCHAR(7) | IBGE municipality code |
| `estagio` | VARCHAR | Execution stage |
| `funcao` | VARCHAR | Government function code |
| `valor` | DECIMAL | Amount in BRL |

**Budget Execution Stages:**

| Stage | Portuguese | Description |
|-------|------------|-------------|
| `Despesas Empenhadas` | Committed | Budget legally reserved |
| `Despesas Liquidadas` | Accrued | Service/good delivered and verified |
| `Despesas Pagas` | Paid | Actual cash disbursement |

**Government Functions (Selected):**

| Code | Function |
|------|----------|
| 04 | Administração (Administration) |
| 10 | Saúde (Health) |
| 12 | Educação (Education) |
| 15 | Urbanismo (Urban Development) |
| 26 | Transporte (Transportation) |
| 27 | Desporto e Lazer (Sports & Leisure) |

---

### 9. Municipal Revenues

> **Source Agency:** STN/Tesouro Nacional via SICONFI

| Attribute | Value |
|-----------|-------|
| **BigQuery Table** | `basedosdados.br_me_siconfi.municipio_receitas_orcamentarias` |
| **Local File** | `data/raw/municipio_receitas_orcamentarias.parquet` |
| **Grain** | One row per municipality per year per source |
| **Coverage** | 2013-2023 |
| **Update Frequency** | Annual |

**Description:**
Municipal revenue data by source, including own-source revenues (taxes, fees) and transfers (FPM, FUNDEB, SUS).

**Key Columns:**

| Column | Type | Description |
|--------|------|-------------|
| `ano` | INTEGER | Fiscal year |
| `id_municipio` | VARCHAR(7) | IBGE municipality code |
| `estagio` | VARCHAR | Revenue stage |
| `conta` | VARCHAR | Account/source code |
| `valor` | DECIMAL | Amount in BRL |

**Major Revenue Sources:**

| Category | Description |
|----------|-------------|
| `1.1 - Impostos` | Taxes (IPTU, ISS, ITBI) |
| `1.2 - Taxas` | Fees and charges |
| `1.7 - Transferências Correntes` | Current transfers |
| `1.7.2 - Transferências Intergovernamentais` | Intergovernmental transfers |
| `Deduções - FUNDEB` | Education fund deductions |

**Key Transfers:**

| Transfer | Description |
|----------|-------------|
| **FPM** | Municipal Participation Fund (constitutional) |
| **FUNDEB** | Basic Education Fund |
| **SUS** | Health system transfers |
| **ICMS** | State sales tax share |
| **IPVA** | Vehicle tax share |

---

## Education Data Sources

### 10. IDEB (Education Quality Index)

> **Source Agency:** INEP/MEC (Ministry of Education)

| Attribute | Value |
|-----------|-------|
| **BigQuery Table** | `basedosdados.br_inep_ideb.municipio` |
| **Local File** | `data/raw/ideb_municipio.parquet` |
| **Grain** | One row per municipality per cycle per education level |
| **Coverage** | 2005-2023 (biennial) |
| **Update Frequency** | Every 2 years |

**Description:**
The Índice de Desenvolvimento da Educação Básica (IDEB) combines standardized test scores with school flow indicators (pass/fail rates) to measure education quality.

**Key Columns:**

| Column | Type | Description |
|--------|------|-------------|
| `ano` | INTEGER | Evaluation year |
| `id_municipio` | VARCHAR(7) | IBGE municipality code |
| `rede` | VARCHAR | Network (municipal, estadual, total) |
| `ensino` | VARCHAR | Level (fundamental_1, fundamental_2, medio) |
| `ideb` | DECIMAL | IDEB score (0-10 scale) |
| `nota_saeb_matematica` | DECIMAL | Math standardized score |
| `nota_saeb_portugues` | DECIMAL | Portuguese standardized score |
| `taxa_aprovacao` | DECIMAL | Pass rate (%) |

**Education Levels:**

| Level | Portuguese | Grades |
|-------|------------|--------|
| `fundamental_1` | Anos Iniciais | 1-5 (ages 6-10) |
| `fundamental_2` | Anos Finais | 6-9 (ages 11-14) |
| `medio` | Ensino Médio | 10-12 (ages 15-17) |

**IDEB Calculation:**
```
IDEB = Proficiency Score × Flow Indicator
where:
  Proficiency = average of standardized math + Portuguese scores
  Flow = average pass rate
```

**Use Cases:**
- Education quality trends over time
- Compare municipal vs state school networks
- Correlate education quality with political factors
- Annual indicator (unlike census-only IDHM)

---

### 11. School Census

> **Source Agency:** INEP/MEC

| Attribute | Value |
|-----------|-------|
| **BigQuery Table** | `basedosdados.br_inep_censo_escolar.municipio` |
| **Local File** | `data/raw/censo_escolar_municipio.parquet` |
| **Grain** | One row per municipality per year |
| **Coverage** | 2000-2023 |
| **Update Frequency** | Annual |

**Description:**
Annual education census with school counts, enrollment numbers, teacher counts, and infrastructure indicators aggregated at municipality level.

**Key Columns:**

| Column | Type | Description |
|--------|------|-------------|
| `ano` | INTEGER | Census year |
| `id_municipio` | VARCHAR(7) | IBGE municipality code |
| `quantidade_escolas` | INTEGER | Number of schools |
| `quantidade_matriculas` | INTEGER | Total enrollments |
| `quantidade_docentes` | INTEGER | Number of teachers |
| `quantidade_turmas` | INTEGER | Number of classes |
| `alunos_por_turma` | DECIMAL | Students per class (ratio) |

**Use Cases:**
- Education infrastructure analysis
- Teacher-student ratios
- School network expansion tracking
- Annual time series (more frequent than IDHM)

---

## Health Data Sources

### 12. Mortality Data

> **Source Agency:** DATASUS/Ministry of Health (SIM - Sistema de Informações sobre Mortalidade)

| Attribute | Value |
|-----------|-------|
| **BigQuery Table** | `basedosdados.br_ms_sim.microdados` |
| **Local File** | `data/raw/mortalidade_municipio.parquet` |
| **Grain** | Aggregated: one row per municipality per year per cause |
| **Coverage** | 1996-2022 |
| **Update Frequency** | Annual (1-2 year lag) |

**Extraction Query:**
```sql
SELECT
    ano,
    id_municipio,
    causa_basica_categoria,
    COUNT(*) as obitos,
    SUM(CASE WHEN idade_obito_anos < 1 THEN 1 ELSE 0 END) as obitos_infantis,
    SUM(CASE WHEN idade_obito_anos < 5 THEN 1 ELSE 0 END) as obitos_menores_5
FROM `basedosdados.br_ms_sim.microdados`
WHERE ano >= 1996
GROUP BY ano, id_municipio, causa_basica_categoria
```

**Key Columns:**

| Column | Type | Description |
|--------|------|-------------|
| `ano` | INTEGER | Death year |
| `id_municipio` | VARCHAR(7) | Municipality where death occurred |
| `causa_basica_categoria` | VARCHAR | ICD-10 cause category |
| `obitos` | INTEGER | Total deaths |
| `obitos_infantis` | INTEGER | Deaths under 1 year |
| `obitos_menores_5` | INTEGER | Deaths under 5 years |

**Major Cause Categories (ICD-10):**

| Code | Category |
|------|----------|
| I | Infectious and parasitic diseases |
| II | Neoplasms (cancer) |
| IX | Circulatory system diseases |
| X | Respiratory system diseases |
| XX | External causes (accidents, violence) |

**Use Cases:**
- Infant mortality rate calculation
- Cause-of-death analysis
- Health outcome correlations with political factors
- Annual time series

---

### 13. Live Births

> **Source Agency:** DATASUS/Ministry of Health (SINASC - Sistema de Informações sobre Nascidos Vivos)

| Attribute | Value |
|-----------|-------|
| **BigQuery Table** | `basedosdados.br_ms_sinasc.microdados` |
| **Local File** | `data/raw/nascimentos_municipio.parquet` |
| **Grain** | Aggregated: one row per municipality per year |
| **Coverage** | 1996-2022 |
| **Update Frequency** | Annual (1-2 year lag) |

**Extraction Query:**
```sql
SELECT
    ano,
    id_municipio_nascimento as id_municipio,
    COUNT(*) as nascidos_vivos
FROM `basedosdados.br_ms_sinasc.microdados`
WHERE ano >= 1996
GROUP BY ano, id_municipio_nascimento
```

**Key Columns:**

| Column | Type | Description |
|--------|------|-------------|
| `ano` | INTEGER | Birth year |
| `id_municipio` | VARCHAR(7) | Municipality where birth occurred |
| `nascidos_vivos` | INTEGER | Live birth count |

**Use Cases:**
- Denominator for mortality rate calculations
- Fertility analysis
- Birth registration coverage assessment

**Mortality Rate Calculation:**
```
Infant Mortality Rate = (obitos_infantis / nascidos_vivos) × 1000
```

---

## Social Program Data Sources

### 14. Bolsa Família

> **Source Agency:** MDS (Ministry of Social Development) / CadÚnico

| Attribute | Value |
|-----------|-------|
| **BigQuery Table** | `basedosdados.br_mds_bolsa_familia.municipio` |
| **Local File** | `data/raw/bolsa_familia_municipio.parquet` |
| **Grain** | One row per municipality per year (or month) |
| **Coverage** | 2004-2023 |
| **Update Frequency** | Monthly |

**Description:**
Brazil's main conditional cash transfer program (CCT). Provides monthly stipends to low-income families conditional on children's school attendance and health checkups.

**Key Columns:**

| Column | Type | Description |
|--------|------|-------------|
| `ano` | INTEGER | Reference year |
| `mes` | INTEGER | Reference month (if monthly) |
| `id_municipio` | VARCHAR(7) | IBGE municipality code |
| `quantidade_beneficiarios` | INTEGER | Number of beneficiary families |
| `valor_total` | DECIMAL | Total transfers in BRL |

**Program History:**
- **2003:** Created by President Lula (unifying earlier programs)
- **2021:** Replaced by Auxílio Brasil under Bolsonaro
- **2023:** Restored as Bolsa Família under Lula

**Use Cases:**
- Federal dependency analysis
- Social protection coverage
- Poverty reduction correlations
- Political economy of social transfers

---

### 15. Cadastro Único

> **Source Agency:** MDS / SENARC

| Attribute | Value |
|-----------|-------|
| **BigQuery Table** | `basedosdados.br_mds_cadastro_unico.municipio` |
| **Local File** | `data/raw/cadastro_unico_municipio.parquet` |
| **Grain** | One row per municipality per year |
| **Coverage** | 2010-2023 |
| **Update Frequency** | Monthly |

**Description:**
The Cadastro Único (Single Registry) is Brazil's database of low-income families used to target social programs. Registration is required for Bolsa Família and other federal programs.

**Key Columns:**

| Column | Type | Description |
|--------|------|-------------|
| `ano` | INTEGER | Reference year |
| `id_municipio` | VARCHAR(7) | IBGE municipality code |
| `quantidade_familias` | INTEGER | Registered families |
| `quantidade_pessoas` | INTEGER | Registered individuals |
| `renda_media` | DECIMAL | Average declared income |

**Use Cases:**
- Poverty mapping
- Program targeting analysis
- Registration coverage assessment

---

## Infrastructure Data Sources

### 16. Sanitation (SNIS)

> **Source Agency:** MDR (Ministry of Regional Development) / SNIS

| Attribute | Value |
|-----------|-------|
| **BigQuery Table** | `basedosdados.br_mdr_snis.municipio` |
| **Local File** | `data/raw/snis_municipio.parquet` |
| **Grain** | One row per municipality per year |
| **Coverage** | 2000-2022 |
| **Update Frequency** | Annual |

**Description:**
National Sanitation Information System with indicators on water supply, sewage collection, and solid waste management.

**Key Columns:**

| Column | Type | Description |
|--------|------|-------------|
| `ano` | INTEGER | Reference year |
| `id_municipio` | VARCHAR(7) | IBGE municipality code |
| `indice_atendimento_agua` | DECIMAL | Water supply coverage (%) |
| `indice_atendimento_esgoto` | DECIMAL | Sewage collection coverage (%) |
| `indice_tratamento_esgoto` | DECIMAL | Sewage treatment rate (%) |
| `indice_coleta_residuos` | DECIMAL | Waste collection coverage (%) |
| `tarifa_media_agua` | DECIMAL | Average water tariff (BRL/m³) |

**Use Cases:**
- Infrastructure development analysis
- Sanitation universalization tracking
- Urban development correlations
- Quality of life indicators

---

## Seed Data (Internal)

### 17. Political Parties Ideology

> **Source:** Academic literature (Power & Zucco, Tarouco & Madeira, Bolognesi et al.)

| Attribute | Value |
|-----------|-------|
| **File** | `dbt_project/seeds/seed_partidos.csv` |
| **Grain** | One row per party |
| **Row Count** | ~40 parties |

**Description:**
Internal seed file classifying Brazilian political parties by ideology based on academic consensus from political science literature.

**Key Columns:**

| Column | Type | Description |
|--------|------|-------------|
| `numero_partido` | INTEGER | TSE party number |
| `sigla_partido` | VARCHAR | Party abbreviation |
| `nome_partido` | VARCHAR | Full party name |
| `espectro_ideologico` | VARCHAR | 5-point scale |
| `bloco_ideologico` | VARCHAR | 3-point grouping |
| `score_ideologico` | INTEGER | Numeric (-2 to +2) |
| `ano_fundacao` | INTEGER | Foundation year |
| `ano_extincao` | INTEGER | Dissolution year (if applicable) |
| `is_big_tent` | BOOLEAN | Catch-all party flag |

**Ideology Classifications:**

| espectro_ideologico | bloco_ideologico | score |
|--------------------|------------------|-------|
| esquerda | esquerda | -2 |
| centro-esquerda | esquerda | -1 |
| centro | centro | 0 |
| centro-direita | direita | +1 |
| direita | direita | +2 |

**Major Parties by Bloc:**

| Bloc | Parties |
|------|---------|
| **Esquerda** | PT, PSOL, PCdoB, PDT, PSB |
| **Centro** | MDB, PSDB, PSD, Cidadania |
| **Direita** | PL, União Brasil, PP, Republicanos, NOVO |

**Academic Sources:**
- Power, T. & Zucco, C. (2009, 2012). Brazilian Legislative Surveys
- Tarouco, G. & Madeira, R. (2013). Partidos, programas e o debate sobre esquerda e direita
- Bolognesi, B., Ribeiro, E. & Codato, A. (2023). Updated party classifications

---

## Data Extraction Guide

### Prerequisites

```bash
# 1. Google Cloud SDK with BigQuery access
gcloud auth application-default login

# 2. Environment variable for billing project
export BASEDOSDADOS_BILLING_PROJECT_ID="your-gcp-project-id"

# 3. Python dependencies
pip install basedosdados polars loguru python-dotenv
```

### Extraction Commands

```bash
# Default extraction (basic tables)
python src/extraction/base_dos_dados.py --mode default

# Full political-economy analysis
python src/extraction/base_dos_dados.py --mode political-economy

# Force re-extraction of all tables
python src/extraction/base_dos_dados.py --mode political-economy --force
```

### Table Groups

| Mode | Tables Included | Use Case |
|------|-----------------|----------|
| `default` | municipio, populacao, pib, idhm, eleicoes, despesas, receitas, snis | Basic analysis |
| `political-economy` | All default + candidates, partidos, ideb, censo_escolar, mortality, births, bolsa_familia, cadastro_unico | Full research |

### Output

All tables are saved as Parquet files in `data/raw/`:

```
data/raw/
├── municipio.parquet
├── populacao.parquet
├── pib_municipio.parquet
├── idhm.parquet
├── resultados_candidato_municipio.parquet
├── candidatos.parquet
├── partidos.parquet
├── municipio_despesas_funcao.parquet
├── municipio_receitas_orcamentarias.parquet
├── ideb_municipio.parquet
├── censo_escolar_municipio.parquet
├── mortalidade_municipio.parquet
├── nascimentos_municipio.parquet
├── bolsa_familia_municipio.parquet
├── cadastro_unico_municipio.parquet
└── snis_municipio.parquet
```

---

## Data Quality & Limitations

### Known Issues

| Data Source | Issue | Impact | Mitigation |
|-------------|-------|--------|------------|
| **IDHM** | Only census years (2000, 2010) | Cannot track annual changes | Use IDEB for education trends |
| **SICONFI** | Available only from 2013 | Limited fiscal time series | Focus on 2012+ mandates |
| **Mortality** | Some underreporting in remote areas | Biased mortality rates | Use state-level corrections |
| **Bolsa Família** | Program restructuring 2021-2023 | Discontinuity in series | Note regime change in analysis |
| **TSE codes** | Inconsistent across election years | Join failures | Use IBGE codes as primary |
| **Population** | Intercensus estimates have noise | Per capita metrics affected | Use census years when possible |

### Municipality Changes

Some municipalities were created, merged, or renamed over time:
- **1996-2024:** ~200 municipalities created
- The directory uses **current (2024) boundaries**
- Historical data may need recoding for new municipalities

### Temporal Coverage Matrix

| Data Source | 1996 | 2000 | 2004 | 2008 | 2010 | 2012 | 2016 | 2020 | 2024 |
|-------------|------|------|------|------|------|------|------|------|------|
| Population | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| IDHM | | ✓ | | | ✓ | | | | |
| Elections | ✓ | ✓ | ✓ | ✓ | | ✓ | ✓ | ✓ | ✓ |
| SICONFI | | | | | | | ✓ | ✓ | ✓ |
| IDEB | | | | ✓ | ✓ | ✓ | ✓ | ✓ | |
| Mortality | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | |
| Bolsa Família | | | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | |

### Recommended Analysis Periods

| Analysis Type | Recommended Period | Reason |
|--------------|-------------------|--------|
| Fiscal analysis | 2013-2023 | SICONFI data quality |
| Social indicators | 2000-2010 | Census years with IDHM |
| Political-economy | 2012-2024 | Fiscal + electoral overlap |
| Education trends | 2005-2023 | IDEB biennial series |
| Health outcomes | 2000-2022 | Mortality data coverage |

---

## References

### Official Sources

- **IBGE:** [ibge.gov.br](https://www.ibge.gov.br)
- **TSE:** [tse.jus.br](https://www.tse.jus.br)
- **INEP:** [inep.gov.br](https://www.inep.gov.br)
- **DATASUS:** [datasus.saude.gov.br](https://datasus.saude.gov.br)
- **Tesouro Nacional:** [tesourotransparente.gov.br](https://www.tesourotransparente.gov.br)

### Base dos Dados

- **Documentation:** [basedosdados.org/docs](https://basedosdados.org/docs)
- **Dataset Catalog:** [basedosdados.org/dataset](https://basedosdados.org/dataset)
- **GitHub:** [github.com/basedosdados](https://github.com/basedosdados)

### Academic References (Party Ideology)

1. Power, T. J., & Zucco, C. (2012). Elite preferences in a consolidating democracy: The Brazilian legislative surveys, 1990-2009. *Latin American Politics and Society*, 54(4), 1-27.

2. Tarouco, G. S., & Madeira, R. M. (2013). Partidos, programas e o debate sobre esquerda e direita no Brasil. *Revista de Sociologia e Política*, 21(45), 149-165.

3. Bolognesi, B., Ribeiro, E., & Codato, A. (2023). A new classification of Brazilian political parties. *Brazilian Political Science Review*, 17(1).

---

*Document generated: 2026-02-01*
