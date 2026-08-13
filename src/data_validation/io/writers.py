"""Writes the validation results DataFrame out (e.g. to ADLS Gen2 as Parquet)."""
from __future__ import annotations

from pyspark.sql import DataFrame

from data_validation.settings import DEFAULT_NUM_OUTPUT_FILES, SUPPORTED_OUTPUT_FORMATS


def write_results(
    df: DataFrame,
    path: str,
    fmt: str = "parquet",
    mode: str = "append",
    partition_by: list[str] | None = None,
    num_output_files: int = DEFAULT_NUM_OUTPUT_FILES,
) -> None:
    fmt = fmt.lower()
    if fmt not in SUPPORTED_OUTPUT_FORMATS:
        raise ValueError(f"Unsupported output format '{fmt}'. Supported: {sorted(SUPPORTED_OUTPUT_FORMATS)}")

    # A validation run produces one row per rule -- a handful of rows at most
    # -- so without this the default parallelism (often = local core count)
    # scatters those few rows across many tiny partition files per run.
    writer = df.coalesce(num_output_files).write.mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    if fmt == "csv":
        writer.option("header", "true").csv(path)
    elif fmt == "json":
        writer.json(path)
    else:
        writer.parquet(path)
