"""
Analysis module for Brazilian municipality analytics.

Contains clustering, statistical analysis, and data science utilities.
"""

from .clustering import (
    run_clustering,
    load_municipality_data,
    generate_cluster_profiles,
    export_to_dbt_seed,
    CLUSTER_LABELS,
)

__all__ = [
    "run_clustering",
    "load_municipality_data",
    "generate_cluster_profiles",
    "export_to_dbt_seed",
    "CLUSTER_LABELS",
]
