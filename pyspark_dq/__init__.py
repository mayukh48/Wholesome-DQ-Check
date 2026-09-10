"""pyspark_dq: a reusable, config-driven data quality framework for PySpark.

Designed to run against any table at any medallion layer (bronze/silver/gold) -
the layer is just metadata attached to results, it never changes check
behavior. Import DQEngine and load_config to use this as a library, or run
pyspark_dq/cli.py directly via spark-submit for a standalone job.
"""
from .config import load_config, load_configs
from .engine import DQEngine
from .models import CheckDefinition, CheckOutcome, CheckResult, DatasetConfig, DQCriticalFailure

__all__ = [
    "DQEngine",
    "load_config",
    "load_configs",
    "CheckDefinition",
    "CheckOutcome",
    "CheckResult",
    "DatasetConfig",
    "DQCriticalFailure",
]
