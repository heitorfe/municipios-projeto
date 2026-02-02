"""
Base dos Dados BigQuery Extractor.

This module handles data extraction from Base dos Dados BigQuery
and exports to local Parquet files for the Bronze layer.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
from loguru import logger

if TYPE_CHECKING:
    import pandas as pd

# Configure loguru
logger.add(
    "logs/extraction_{time}.log",
    rotation="10 MB",
    retention="7 days",
    level="INFO",
)


@dataclass
class TableConfig:
    """Configuration for a table to extract."""

    dataset: str
    table: str
    description: str
    query: str | None = None  # Custom query, if None extracts full table
    output_name: str | None = None  # Custom output filename (without .parquet), avoids conflicts

    @property
    def filename(self) -> str:
        """Get the output filename (without extension)."""
        return self.output_name or self.table


# =============================================================================
# BASE TABLES - Core municipality data
# =============================================================================
BASE_TABLES: list[TableConfig] = [
    TableConfig(
        dataset="br_bd_diretorios_brasil",
        table="municipio",
        description="Municipality directory with code crosswalk (IBGE, TSE, BCB)",
        # Keep as 'municipio' for backward compatibility with stg_municipios.sql
    ),
    TableConfig(
        dataset="br_ibge_populacao",
        table="municipio",
        description="Historical population by municipality (1991-2025)",
        output_name="populacao",  # Matches existing stg_populacao.sql
    ),
    TableConfig(
        dataset="br_ibge_pib",
        table="municipio",
        description="Municipal GDP (2002-2021)",
        output_name="pib_municipio",
    ),
    TableConfig(
        dataset="br_pnud_atlas",
        table="municipio",
        description="Human Development Index (IDHM) and 200+ indicators",
        output_name="idhm",  # Matches existing stg_idhm.sql
    ),
]

# =============================================================================
# ELECTORAL TABLES - Political data from TSE
# =============================================================================
ELECTORAL_TABLES: list[TableConfig] = [
    TableConfig(
        dataset="br_tse_eleicoes",
        table="resultados_candidato_municipio",
        description="Electoral results by municipality - mayors only (1996-2024)",
        query="""
        SELECT *
        FROM `basedosdados.br_tse_eleicoes.resultados_candidato_municipio`
        WHERE cargo = 'prefeito'
        """,
    ),
    TableConfig(
        dataset="br_tse_eleicoes",
        table="candidatos",
        description="Candidate details - gender, age, education, occupation (1994-2024)",
        query="""
        SELECT *
        FROM `basedosdados.br_tse_eleicoes.candidatos`
        WHERE cargo = 'prefeito'
        """,
    ),
    TableConfig(
        dataset="br_tse_eleicoes",
        table="partidos",
        description="Political parties registry - creation and extinction dates",
    ),
]

# =============================================================================
# FISCAL TABLES - Municipal finances from SICONFI
# =============================================================================
FISCAL_TABLES: list[TableConfig] = [
    TableConfig(
        dataset="br_me_siconfi",
        table="municipio_despesas_funcao",
        description="Municipal expenses by function (2013-2023)",
    ),
    TableConfig(
        dataset="br_me_siconfi",
        table="municipio_receitas_orcamentarias",
        description="Municipal revenues with transfer breakdown (2013-2023)",
    ),
]

# =============================================================================
# EDUCATION TABLES - Annual education metrics
# =============================================================================
EDUCATION_TABLES: list[TableConfig] = [
    TableConfig(
        dataset="br_inep_ideb",
        table="municipio",
        description="IDEB - Education Development Index by municipality (2005-2023, biennial)",
        output_name="ideb_municipio",  # Avoid conflict with other 'municipio' tables
    ),
    TableConfig(
        dataset="br_inep_censo_escolar",
        table="municipio",
        description="School Census - enrollment, schools, teachers by municipality (annual)",
        query="""
        SELECT *
        FROM `basedosdados.br_inep_censo_escolar.municipio`
        WHERE ano >= 2000
        """,
        output_name="censo_escolar_municipio",
    ),
]

# =============================================================================
# HEALTH TABLES - Mortality and health indicators
# =============================================================================
HEALTH_TABLES: list[TableConfig] = [
    TableConfig(
        dataset="br_ms_sim",
        table="municipio_causa",
        description="Mortality by municipality and cause of death (1996-2022)",
        query="""
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
        """,
        output_name="mortalidade_municipio",  # Clearer name for output
    ),
    TableConfig(
        dataset="br_ms_sinasc",
        table="municipio",
        description="Live births by municipality - for mortality rate calculation",
        query="""
        SELECT
            ano,
            id_municipio_nascimento as id_municipio,
            COUNT(*) as nascidos_vivos
        FROM `basedosdados.br_ms_sinasc.microdados`
        WHERE ano >= 1996
        GROUP BY ano, id_municipio_nascimento
        """,
        output_name="nascimentos_municipio",  # Avoid conflict with other 'municipio' tables
    ),
]

# =============================================================================
# SOCIAL TRANSFER TABLES - Federal programs
# =============================================================================
SOCIAL_TABLES: list[TableConfig] = [
    TableConfig(
        dataset="br_mds_bolsa_familia",
        table="municipio",
        description="Bolsa Familia transfers by municipality (2004-2023)",
        query="""
        SELECT *
        FROM `basedosdados.br_mds_bolsa_familia.municipio`
        WHERE ano >= 2004
        """,
        output_name="bolsa_familia_municipio",  # Descriptive name
    ),
    TableConfig(
        dataset="br_mds_cadastro_unico",
        table="municipio",
        description="Cadastro Unico - families registered for social programs",
        query="""
        SELECT *
        FROM `basedosdados.br_mds_cadastro_unico.municipio`
        WHERE ano >= 2010
        """,
        output_name="cadastro_unico_municipio",  # Avoid conflict
    ),
]

# =============================================================================
# INFRASTRUCTURE TABLES
# =============================================================================
INFRASTRUCTURE_TABLES: list[TableConfig] = [
    TableConfig(
        dataset="br_mdr_snis",
        table="municipio",
        description="Sanitation indicators (water, sewage, waste) - annual",
        output_name="saneamento_municipio",  # Descriptive name
    ),
]

# =============================================================================
# COMBINED TABLE LISTS
# =============================================================================

# Default tables (original set for backward compatibility)
DEFAULT_TABLES: list[TableConfig] = (
    BASE_TABLES +
    [ELECTORAL_TABLES[0]] +  # Just the main election results
    FISCAL_TABLES +
    [INFRASTRUCTURE_TABLES[0]]
)

# Full political-economy analysis tables
POLITICAL_ECONOMY_TABLES: list[TableConfig] = (
    BASE_TABLES +
    ELECTORAL_TABLES +
    FISCAL_TABLES +
    EDUCATION_TABLES +
    HEALTH_TABLES +
    SOCIAL_TABLES +
    INFRASTRUCTURE_TABLES
)

# All available tables
ALL_TABLES: list[TableConfig] = POLITICAL_ECONOMY_TABLES


class BaseDadosExtractor:
    """
    Extract data from Base dos Dados BigQuery to local Parquet files.

    This class handles the Bronze layer of our data pipeline,
    extracting raw data from BigQuery and storing it as Parquet files.

    Example:
        >>> extractor = BaseDadosExtractor(
        ...     billing_project="your-gcp-project",
        ...     output_dir=Path("data/raw")
        ... )
        >>> extractor.extract_all()
    """

    def __init__(
        self,
        billing_project: str | None = None,
        output_dir: Path | str = "data/raw",
    ) -> None:
        """
        Initialize the extractor.

        Args:
            billing_project: Google Cloud project ID for billing.
                            If None, reads from BASEDOSDADOS_BILLING_PROJECT_ID env var.
            output_dir: Directory to save Parquet files.
        """
        self.billing_project = billing_project or os.getenv(
            "BASEDOSDADOS_BILLING_PROJECT_ID"
        )
        if not self.billing_project:
            raise ValueError(
                "billing_project must be provided or set via "
                "BASEDOSDADOS_BILLING_PROJECT_ID environment variable"
            )

        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Initialized BaseDadosExtractor with project: {self.billing_project}")
        logger.info(f"Output directory: {self.output_dir.absolute()}")

    def _build_query(self, config: TableConfig) -> str:
        """Build the SQL query for extraction."""
        if config.query:
            return config.query
        return f"SELECT * FROM `basedosdados.{config.dataset}.{config.table}`"

    def extract_table(
        self,
        config: TableConfig,
        *,
        force: bool = False,
    ) -> pl.DataFrame:
        """
        Extract a single table from BigQuery and save as Parquet.

        Args:
            config: Table configuration with dataset, table, and optional query.
            force: If True, re-extract even if file exists.

        Returns:
            Polars DataFrame with the extracted data.
        """
        output_path = self.output_dir / f"{config.filename}.parquet"

        # Check if file already exists
        if output_path.exists() and not force:
            logger.info(f"Skipping {config.filename} - file already exists. Use force=True to re-extract.")
            return pl.read_parquet(output_path)

        logger.info(f"Extracting: {config.dataset}.{config.table}")
        logger.info(f"Description: {config.description}")

        try:
            # Import basedosdados here to avoid import errors if not installed
            import basedosdados as bd

            query = self._build_query(config)
            logger.debug(f"Query: {query[:200]}...")

            # Execute query and get pandas DataFrame
            df_pandas: pd.DataFrame = bd.read_sql(
                query,
                billing_project_id=self.billing_project,
            )

            # Convert to Polars for better performance
            df = pl.from_pandas(df_pandas)

            # Save as Parquet
            df.write_parquet(output_path, compression="zstd")

            logger.success(
                f"Saved {config.filename}.parquet: {len(df):,} rows, "
                f"{df.estimated_size() / 1024 / 1024:.2f} MB"
            )

            return df

        except ImportError:
            logger.error(
                "basedosdados package not installed. "
                "Install with: pip install basedosdados"
            )
            raise
        except Exception as e:
            logger.error(f"Failed to extract {config.table}: {e}")
            raise

    def extract_all(
        self,
        tables: list[TableConfig] | None = None,
        *,
        force: bool = False,
    ) -> dict[str, pl.DataFrame]:
        """
        Extract all configured tables.

        Args:
            tables: List of table configurations. If None, uses DEFAULT_TABLES.
            force: If True, re-extract all tables even if files exist.

        Returns:
            Dictionary mapping table names to DataFrames.
        """
        tables = tables or DEFAULT_TABLES
        results: dict[str, pl.DataFrame] = {}

        logger.info(f"Starting extraction of {len(tables)} tables...")

        for i, config in enumerate(tables, 1):
            logger.info(f"[{i}/{len(tables)}] Processing {config.table}")
            try:
                df = self.extract_table(config, force=force)
                results[config.table] = df
            except Exception as e:
                logger.error(f"Failed to extract {config.table}: {e}")
                # Continue with other tables
                continue

        logger.success(f"Extraction complete. {len(results)}/{len(tables)} tables extracted.")
        return results

    def get_table_info(self, table_name: str) -> dict:
        """
        Get information about an extracted table.

        Args:
            table_name: Name of the table file (without .parquet extension).

        Returns:
            Dictionary with table metadata.
        """
        path = self.output_dir / f"{table_name}.parquet"
        if not path.exists():
            raise FileNotFoundError(f"Table not found: {path}")

        df = pl.read_parquet(path)
        return {
            "table_name": table_name,
            "path": str(path),
            "rows": len(df),
            "columns": len(df.columns),
            "column_names": df.columns,
            "dtypes": {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)},
            "size_mb": path.stat().st_size / 1024 / 1024,
            "estimated_memory_mb": df.estimated_size() / 1024 / 1024,
        }

    def list_extracted_tables(self) -> list[str]:
        """List all extracted Parquet files."""
        return [p.stem for p in self.output_dir.glob("*.parquet")]


def extract_political_economy(
    billing_project: str | None = None,
    output_dir: Path | str = "data/raw",
    force: bool = False,
) -> dict[str, pl.DataFrame]:
    """
    Extract all tables needed for political-economy analysis.

    This includes:
    - Base municipality data (population, GDP, IDHM)
    - Full electoral data (results, candidates, parties)
    - Fiscal data (revenues, expenses)
    - Education indicators (IDEB, census)
    - Health indicators (mortality, births)
    - Social transfers (Bolsa Familia, Cadastro Unico)
    - Infrastructure (sanitation)

    Args:
        billing_project: Google Cloud project ID for billing.
        output_dir: Directory to save Parquet files.
        force: If True, re-extract all tables even if files exist.

    Returns:
        Dictionary mapping table names to DataFrames.
    """
    extractor = BaseDadosExtractor(
        billing_project=billing_project,
        output_dir=output_dir,
    )
    return extractor.extract_all(tables=POLITICAL_ECONOMY_TABLES, force=force)


def main() -> None:
    """Main entry point for extraction script."""
    import argparse
    from dotenv import load_dotenv

    # Load environment variables
    load_dotenv()

    parser = argparse.ArgumentParser(description="Extract data from Base dos Dados")
    parser.add_argument(
        "--mode",
        choices=["default", "political-economy", "all"],
        default="default",
        help="Extraction mode: default (basic tables), political-economy (full analysis), all (everything)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-extraction even if files exist",
    )
    args = parser.parse_args()

    # Select tables based on mode
    if args.mode == "political-economy":
        tables = POLITICAL_ECONOMY_TABLES
        logger.info("Mode: Political-Economy Analysis (full dataset)")
    elif args.mode == "all":
        tables = ALL_TABLES
        logger.info("Mode: All available tables")
    else:
        tables = DEFAULT_TABLES
        logger.info("Mode: Default (basic tables)")

    # Create extractor
    extractor = BaseDadosExtractor()

    # Extract tables
    extractor.extract_all(tables=tables, force=args.force)

    # Print summary
    logger.info("\n=== Extraction Summary ===")
    for table_name in extractor.list_extracted_tables():
        info = extractor.get_table_info(table_name)
        logger.info(
            f"{table_name}: {info['rows']:,} rows, "
            f"{info['columns']} columns, "
            f"{info['size_mb']:.2f} MB"
        )


if __name__ == "__main__":
    main()
