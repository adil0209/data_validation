"""Generic input readers. ``path`` may be a local path or an abfss:// URI."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from data_validation.settings import SUPPORTED_INPUT_FORMATS


def read_input(spark: SparkSession, path: str, fmt: str = "csv") -> DataFrame:
    fmt = fmt.lower()
    if fmt not in SUPPORTED_INPUT_FORMATS:
        raise ValueError(f"Unsupported input format '{fmt}'. Supported: {sorted(SUPPORTED_INPUT_FORMATS)}")

    reader = spark.read
    if fmt == "csv":
        return reader.option("header", "true").option("inferSchema", "true").csv(path)
    if fmt == "json":
        return reader.json(path)
    return reader.parquet(path)
