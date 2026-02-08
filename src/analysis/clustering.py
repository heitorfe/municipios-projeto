"""
Municipality clustering analysis module.

Implements K-Means clustering to segment Brazilian municipalities
into development tiers based on socio-economic indicators.

Usage:
    python -m src.analysis.clustering

Or from Python:
    from src.analysis.clustering import main
    df, profiles = main()
"""

from pathlib import Path
from typing import Tuple

import duckdb
import numpy as np
import polars as pl
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score

# Configuration
PROJECT_ROOT = Path(__file__).parent.parent.parent
WAREHOUSE_PATH = PROJECT_ROOT / "data" / "warehouse" / "analytics.duckdb"
SEED_OUTPUT_PATH = PROJECT_ROOT / "dbt_project" / "seeds" / "seed_cluster_assignments.csv"

# Schema prefix
MARTS_SCHEMA = "main_marts"

# Cluster labels in Portuguese (ordered from most to least developed)
CLUSTER_LABELS = {
    0: "Polos de Desenvolvimento",
    1: "Desenvolvimento Avancado",
    2: "Em Desenvolvimento",
    3: "Vulneraveis",
    4: "Criticos",
}

# Features used for clustering
CLUSTERING_FEATURES = [
    "idhm_2010",
    "idhm_educacao",
    "idhm_renda",
    "ivs_2010_inverted",
    "gini_2010_inverted",
    "renda_per_capita_log",
]


def load_municipality_data() -> pl.DataFrame:
    """
    Load municipality data from DuckDB warehouse.

    Returns:
        Polars DataFrame with municipality data and indicators.

    Raises:
        FileNotFoundError: If database file doesn't exist.
    """
    if not WAREHOUSE_PATH.exists():
        raise FileNotFoundError(
            f"Database not found at {WAREHOUSE_PATH}. "
            "Please run 'dbt run' first to create the data warehouse."
        )

    conn = duckdb.connect(str(WAREHOUSE_PATH), read_only=True)

    query = f"""
    SELECT
        id_municipio_ibge,
        nome_municipio,
        sigla_uf,
        regiao,
        populacao,
        porte_municipio,
        idhm_2010,
        idhm_educacao,
        idhm_longevidade,
        idhm_renda,
        ivs_2010,
        gini_2010,
        renda_per_capita_2010
    FROM {MARTS_SCHEMA}.dim_municipio
    WHERE idhm_2010 IS NOT NULL
      AND ivs_2010 IS NOT NULL
      AND gini_2010 IS NOT NULL
      AND renda_per_capita_2010 IS NOT NULL
    """

    return conn.execute(query).pl()


def prepare_features(df: pl.DataFrame) -> Tuple[np.ndarray, pl.DataFrame]:
    """
    Prepare and scale features for clustering.

    Transforms raw indicators into normalized features:
    - Inverts IVS and Gini so higher values = better
    - Log-scales income per capita
    - Standardizes all features

    Args:
        df: DataFrame with municipality data.

    Returns:
        Tuple of (scaled feature matrix, enhanced DataFrame).
    """
    # Create derived features
    df = df.with_columns([
        # Invert IVS (vulnerability) so higher = better
        (1 - pl.col("ivs_2010")).alias("ivs_2010_inverted"),
        # Invert Gini (inequality) so higher = better
        (1 - pl.col("gini_2010")).alias("gini_2010_inverted"),
        # Log-scale income per capita for better distribution
        pl.col("renda_per_capita_2010").log().alias("renda_per_capita_log"),
    ])

    # Select feature columns
    feature_cols = [
        "idhm_2010",
        "idhm_educacao",
        "idhm_renda",
        "ivs_2010_inverted",
        "gini_2010_inverted",
        "renda_per_capita_log",
    ]

    # Extract feature matrix
    X = df.select(feature_cols).to_numpy()

    # Handle any remaining NaN/inf values with column medians
    for i in range(X.shape[1]):
        col = X[:, i]
        mask = ~np.isfinite(col)
        if mask.any():
            median_val = np.nanmedian(col[np.isfinite(col)])
            X[mask, i] = median_val

    # Standardize features (z-score normalization)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    return X_scaled, df


def find_optimal_clusters(X: np.ndarray, max_k: int = 10) -> dict:
    """
    Analyze clustering performance for different k values.

    Uses elbow method (inertia) and silhouette score.

    Args:
        X: Scaled feature matrix.
        max_k: Maximum number of clusters to test.

    Returns:
        Dictionary with analysis results and recommended k.
    """
    inertias = []
    silhouettes = []

    for k in range(2, max_k + 1):
        kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = kmeans.fit_predict(X)
        inertias.append(kmeans.inertia_)
        silhouettes.append(silhouette_score(X, labels))

    # Find optimal k by silhouette score
    best_k_silhouette = silhouettes.index(max(silhouettes)) + 2

    return {
        "k_range": list(range(2, max_k + 1)),
        "inertias": inertias,
        "silhouettes": silhouettes,
        "best_k_silhouette": best_k_silhouette,
        "recommended_k": 5,  # Fixed for interpretability
    }


def run_clustering(X: np.ndarray, n_clusters: int = 5) -> Tuple[np.ndarray, KMeans]:
    """
    Run K-Means clustering.

    Args:
        X: Scaled feature matrix.
        n_clusters: Number of clusters.

    Returns:
        Tuple of (cluster labels, fitted KMeans model).
    """
    kmeans = KMeans(
        n_clusters=n_clusters,
        random_state=42,
        n_init=10,
        max_iter=300,
    )
    labels = kmeans.fit_predict(X)

    # Calculate silhouette score
    score = silhouette_score(X, labels)
    print(f"Silhouette Score: {score:.4f}")

    return labels, kmeans


def order_clusters_by_development(
    df: pl.DataFrame,
    labels: np.ndarray,
) -> pl.DataFrame:
    """
    Reorder clusters from most to least developed based on mean IDHM.

    Ensures cluster 0 = most developed, cluster 4 = least developed.

    Args:
        df: DataFrame with municipality data.
        labels: Raw cluster labels from K-Means.

    Returns:
        DataFrame with ordered cluster assignments and labels.
    """
    df = df.with_columns(pl.Series("cluster_raw", labels))

    # Calculate mean IDHM per cluster
    cluster_means = (
        df.group_by("cluster_raw")
        .agg(pl.col("idhm_2010").mean().alias("mean_idhm"))
        .sort("mean_idhm", descending=True)
    )

    # Create mapping from raw cluster to ordered cluster
    cluster_mapping = {
        row["cluster_raw"]: idx
        for idx, row in enumerate(cluster_means.iter_rows(named=True))
    }

    # Apply mapping to create ordered cluster IDs
    df = df.with_columns(
        pl.col("cluster_raw")
        .replace_strict(cluster_mapping, default=None)
        .alias("cluster_id")
    )

    # Add Portuguese labels
    df = df.with_columns(
        pl.col("cluster_id")
        .replace_strict(CLUSTER_LABELS, default="Desconhecido")
        .alias("cluster_label")
    )

    return df.drop("cluster_raw")


def generate_cluster_profiles(df: pl.DataFrame) -> pl.DataFrame:
    """
    Generate summary statistics for each cluster.

    Args:
        df: DataFrame with cluster assignments.

    Returns:
        DataFrame with cluster profiles.
    """
    return (
        df.group_by("cluster_id", "cluster_label")
        .agg([
            pl.len().alias("num_municipios"),
            pl.col("populacao").sum().alias("total_populacao"),
            pl.col("idhm_2010").mean().alias("avg_idhm"),
            pl.col("idhm_2010").std().alias("std_idhm"),
            pl.col("idhm_2010").min().alias("min_idhm"),
            pl.col("idhm_2010").max().alias("max_idhm"),
            pl.col("ivs_2010").mean().alias("avg_ivs"),
            pl.col("gini_2010").mean().alias("avg_gini"),
            pl.col("renda_per_capita_2010").mean().alias("avg_renda_pc"),
        ])
        .sort("cluster_id")
    )


def export_to_dbt_seed(df: pl.DataFrame) -> Path:
    """
    Export cluster assignments to dbt seed CSV.

    Args:
        df: DataFrame with cluster assignments.

    Returns:
        Path to the created CSV file.
    """
    seed_df = df.select([
        "id_municipio_ibge",
        "cluster_id",
        "cluster_label",
    ])

    # Ensure directory exists
    SEED_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Write CSV with proper formatting
    seed_df.write_csv(SEED_OUTPUT_PATH)

    return SEED_OUTPUT_PATH


def main() -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    Run the full clustering pipeline.

    Returns:
        Tuple of (municipality DataFrame with clusters, cluster profiles).
    """
    print("=" * 60)
    print("Municipality Clustering Analysis")
    print("=" * 60)

    print("\n1. Loading municipality data...")
    df = load_municipality_data()
    print(f"   Loaded {len(df):,} municipalities with complete data")

    print("\n2. Preparing features...")
    X_scaled, df = prepare_features(df)
    print(f"   Features: {CLUSTERING_FEATURES}")

    print("\n3. Analyzing optimal cluster count...")
    analysis = find_optimal_clusters(X_scaled)
    print(f"   Best k by silhouette: {analysis['best_k_silhouette']}")
    print(f"   Using k=5 for interpretability")

    print("\n4. Running K-Means clustering (k=5)...")
    labels, kmeans = run_clustering(X_scaled, n_clusters=5)

    print("\n5. Ordering clusters by development level...")
    df = order_clusters_by_development(df, labels)

    print("\n6. Generating cluster profiles...")
    profiles = generate_cluster_profiles(df)

    print("\n" + "=" * 60)
    print("CLUSTER PROFILES")
    print("=" * 60)
    for row in profiles.iter_rows(named=True):
        print(f"\n[Cluster {row['cluster_id']}] {row['cluster_label']}")
        print(f"   Municipios: {row['num_municipios']:,}")
        print(f"   Populacao: {row['total_populacao']:,.0f}")
        print(f"   IDHM: {row['avg_idhm']:.3f} (range: {row['min_idhm']:.3f}-{row['max_idhm']:.3f})")
        print(f"   IVS: {row['avg_ivs']:.3f}")
        print(f"   Gini: {row['avg_gini']:.3f}")
        print(f"   Renda PC: R$ {row['avg_renda_pc']:,.2f}")

    print("\n7. Exporting to dbt seed...")
    seed_path = export_to_dbt_seed(df)
    print(f"   Saved to: {seed_path}")

    print("\n" + "=" * 60)
    print("NEXT STEPS")
    print("=" * 60)
    print("1. Run dbt seed: cd dbt_project && dbt seed")
    print("2. Build mart: dbt run --select mart_cluster_analysis")
    print("3. Launch dashboard: streamlit run dashboard/app.py")
    print("=" * 60)

    return df, profiles


if __name__ == "__main__":
    main()
