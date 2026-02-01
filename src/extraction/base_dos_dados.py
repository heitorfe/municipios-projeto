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


# Default tables to extract from Base dos Dados
DEFAULT_TABLES: list[TableConfig] = [
    TableConfig(
        dataset="br_bd_diretorios_brasil",
        table="municipio",
        description="Municipality directory with code crosswalk (IBGE, TSE, BCB)",
    ),
    TableConfig(
        dataset="br_ibge_populacao",
        table="municipio",
        description="Historical population by municipality (1991-2025)",
    ),
    TableConfig(
        dataset="br_ibge_pib",
        table="municipio",
        description="Municipal GDP (2002-2021)",
    ),
    TableConfig(
        dataset="br_pnud_atlas",
        table="municipio",
        description="Human Development Index (IDHM) and 200+ indicators",
    ),
    TableConfig(
        dataset="br_tse_eleicoes",
        table="resultados_candidato_municipio",
        description="Electoral results by municipality (1996-2024)",
        query="""
        SELECT *
        FROM `basedosdados.br_tse_eleicoes.resultados_candidato_municipio`
        WHERE cargo = 'prefeito'
        """,
    ),
    TableConfig(
        dataset="br_me_siconfi",
        table="municipio_despesas_funcao",
        description="Municipal expenses by function (2013-2023)",
    ),
    TableConfig(
        dataset="br_me_siconfi",
        table="municipio_receitas_orcamentarias",
        description="Municipal revenues (2013-2023)",
    ),
    TableConfig(
        dataset="br_mdr_snis",
        table="municipio",
        description="Sanitation indicators (water, sewage, waste)",
    ),
]


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
        output_path = self.output_dir / f"{config.table}.parquet"

        # Check if file already exists
        if output_path.exists() and not force:
            logger.info(f"Skipping {config.table} - file already exists. Use force=True to re-extract.")
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
                f"Extracted {config.table}: {len(df):,} rows, "
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


def main() -> None:
    """Main entry point for extraction script."""
    from dotenv import load_dotenv

    # Load environment variables
    load_dotenv()

    # Create extractor
    extractor = BaseDadosExtractor()

    # Extract all tables
    extractor.extract_all()

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
