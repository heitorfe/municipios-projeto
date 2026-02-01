#!/usr/bin/env python3
"""
Data Extraction Script.

This script extracts data from Base dos Dados BigQuery
and saves it as Parquet files in the Bronze layer.

Usage:
    python scripts/extract_data.py [OPTIONS]

Options:
    --force     Re-extract all tables even if files exist
    --table     Extract specific table only (e.g., --table municipio)
"""

import sys
from pathlib import Path

import typer
from dotenv import load_dotenv
from loguru import logger
from rich.console import Console
from rich.table import Table

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.extraction.base_dos_dados import BaseDadosExtractor, DEFAULT_TABLES

# Initialize
load_dotenv()
console = Console()
app = typer.Typer(help="Extract data from Base dos Dados BigQuery")


@app.command()
def extract(
    force: bool = typer.Option(False, "--force", "-f", help="Re-extract even if files exist"),
    table: str = typer.Option(None, "--table", "-t", help="Extract specific table only"),
    output_dir: str = typer.Option("data/raw", "--output", "-o", help="Output directory"),
) -> None:
    """
    Extract data from Base dos Dados BigQuery to local Parquet files.

    This command queries BigQuery and saves the results as Parquet files
    in the specified output directory (default: data/raw/).

    Prerequisites:
        1. Google Cloud project with BigQuery API enabled
        2. Service account with BigQuery User role
        3. BASEDOSDADOS_BILLING_PROJECT_ID environment variable set
        4. GOOGLE_APPLICATION_CREDENTIALS pointing to credentials JSON
    """
    console.print("\n[bold blue]🇧🇷 Brazilian Municipalities Data Extractor[/bold blue]\n")

    # Validate environment
    import os

    project_id = os.getenv("BASEDOSDADOS_BILLING_PROJECT_ID")
    if not project_id:
        console.print(
            "[red]❌ Error: BASEDOSDADOS_BILLING_PROJECT_ID not set.[/red]\n"
            "Please set this environment variable to your Google Cloud project ID.\n"
            "Example: export BASEDOSDADOS_BILLING_PROJECT_ID=your-project-id"
        )
        raise typer.Exit(1)

    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not credentials_path:
        console.print(
            "[yellow]⚠️ Warning: GOOGLE_APPLICATION_CREDENTIALS not set.[/yellow]\n"
            "You may need to authenticate with: gcloud auth application-default login"
        )

    console.print(f"[green]✓[/green] Using project: [bold]{project_id}[/bold]")
    console.print(f"[green]✓[/green] Output directory: [bold]{output_dir}[/bold]\n")

    # Initialize extractor
    try:
        extractor = BaseDadosExtractor(
            billing_project=project_id,
            output_dir=Path(output_dir),
        )
    except Exception as e:
        console.print(f"[red]❌ Failed to initialize extractor: {e}[/red]")
        raise typer.Exit(1)

    # Filter tables if specific table requested
    tables_to_extract = DEFAULT_TABLES
    if table:
        tables_to_extract = [t for t in DEFAULT_TABLES if t.table == table]
        if not tables_to_extract:
            console.print(f"[red]❌ Table not found: {table}[/red]")
            console.print("\nAvailable tables:")
            for t in DEFAULT_TABLES:
                console.print(f"  - {t.table}")
            raise typer.Exit(1)

    # Show extraction plan
    console.print("[bold]Extraction Plan:[/bold]")
    plan_table = Table(show_header=True, header_style="bold magenta")
    plan_table.add_column("Table", style="cyan")
    plan_table.add_column("Dataset")
    plan_table.add_column("Description")

    for t in tables_to_extract:
        plan_table.add_row(t.table, t.dataset, t.description[:50] + "...")

    console.print(plan_table)
    console.print()

    # Confirm
    if not force:
        existing_files = list(Path(output_dir).glob("*.parquet"))
        if existing_files:
            console.print(
                f"[yellow]Found {len(existing_files)} existing files. "
                "Use --force to re-extract.[/yellow]\n"
            )

    # Extract
    console.print("[bold]Starting extraction...[/bold]\n")

    try:
        results = extractor.extract_all(tables=tables_to_extract, force=force)
    except Exception as e:
        console.print(f"[red]❌ Extraction failed: {e}[/red]")
        logger.exception("Extraction failed")
        raise typer.Exit(1)

    # Summary
    console.print("\n[bold green]✅ Extraction Complete![/bold green]\n")

    summary_table = Table(show_header=True, header_style="bold green")
    summary_table.add_column("Table", style="cyan")
    summary_table.add_column("Rows", justify="right")
    summary_table.add_column("Columns", justify="right")
    summary_table.add_column("Size (MB)", justify="right")

    for table_name, df in results.items():
        info = extractor.get_table_info(table_name)
        summary_table.add_row(
            table_name,
            f"{info['rows']:,}",
            str(info["columns"]),
            f"{info['size_mb']:.2f}",
        )

    console.print(summary_table)

    console.print(
        "\n[bold]Next steps:[/bold]\n"
        "  1. cd dbt_project\n"
        "  2. cp profiles.yml.example profiles.yml\n"
        "  3. dbt deps\n"
        "  4. dbt build\n"
        "  5. streamlit run ../dashboard/app.py\n"
    )


@app.command()
def list_tables() -> None:
    """List available tables for extraction."""
    console.print("\n[bold]Available Tables:[/bold]\n")

    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Table", style="cyan")
    table.add_column("Dataset")
    table.add_column("Description")

    for t in DEFAULT_TABLES:
        table.add_row(t.table, t.dataset, t.description)

    console.print(table)


@app.command()
def status() -> None:
    """Check status of extracted data."""
    output_dir = Path("data/raw")

    if not output_dir.exists():
        console.print("[yellow]⚠️ No data directory found. Run 'extract' first.[/yellow]")
        return

    files = list(output_dir.glob("*.parquet"))

    if not files:
        console.print("[yellow]⚠️ No Parquet files found. Run 'extract' first.[/yellow]")
        return

    console.print(f"\n[bold]Extracted Data Status:[/bold] ({output_dir})\n")

    table = Table(show_header=True, header_style="bold green")
    table.add_column("File", style="cyan")
    table.add_column("Size (MB)", justify="right")
    table.add_column("Modified")

    import datetime

    total_size = 0
    for f in sorted(files):
        size_mb = f.stat().st_size / 1024 / 1024
        total_size += size_mb
        modified = datetime.datetime.fromtimestamp(f.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
        table.add_row(f.name, f"{size_mb:.2f}", modified)

    console.print(table)
    console.print(f"\n[bold]Total size:[/bold] {total_size:.2f} MB")


def main() -> None:
    """Entry point."""
    app()


if __name__ == "__main__":
    main()
