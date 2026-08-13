"""Central home for hardcoded, code-level defaults shared across modules.

These are implementation constants (protocol/class names, format
capabilities, internal defaults) rather than per-deployment settings --
those live in config/pipeline_config.yaml instead. Keeping them here means
each value has exactly one definition instead of being duplicated or
redefined per module.
"""
from __future__ import annotations

# -- spark_session.py -------------------------------------------------------
SPARK_APP_NAME_DEFAULT = "data-validation-pipeline"
# hadoop-azure's version must match the Hadoop client jars bundled with the
# installed pyspark wheel, or abfss:// reads/writes fail with class-loading
# errors. That bundled version varies by pyspark release (3.5.x ships Hadoop
# 3.3.4; 4.x releases have shipped anywhere from 3.4.0 to 3.5.0), so it's
# detected from pyspark's own jars/ directory rather than hardcoded. This is
# only the fallback used when detection fails; override at runtime with the
# AZURE_HADOOP_VERSION env var.
FALLBACK_HADOOP_VERSION = "3.3.4"
AZURE_STORAGE_JAR = "com.microsoft.azure:azure-storage:8.6.6"

# -- azure_auth.py ------------------------------------------------------------
OAUTH_PROVIDER = "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"

# -- io/readers.py, io/writers.py ---------------------------------------------
SUPPORTED_INPUT_FORMATS = {"csv", "parquet", "json"}
SUPPORTED_OUTPUT_FORMATS = {"parquet", "csv", "json"}
DEFAULT_NUM_OUTPUT_FILES = 1

# -- validators/rules.py --------------------------------------------------------
DATASET_LEVEL_RULE_TYPES = {"unique", "custom_sql"}
DEFAULT_SEVERITY = "error"

# -- cli.py ---------------------------------------------------------------------
DEFAULT_LOG_LEVEL = "INFO"
