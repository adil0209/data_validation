"""SparkSession construction, including the jars needed to talk to ADLS Gen2 (abfss://)."""
from __future__ import annotations

import glob
import os
import re

from pyspark.sql import SparkSession

from data_validation.settings import AZURE_STORAGE_JAR, FALLBACK_HADOOP_VERSION, SPARK_APP_NAME_DEFAULT


def _detect_hadoop_version() -> str:
    env_override = os.environ.get("AZURE_HADOOP_VERSION")
    if env_override:
        return env_override
    try:
        import pyspark

        jars_dir = os.path.join(os.path.dirname(pyspark.__file__), "jars")
        matches = glob.glob(os.path.join(jars_dir, "hadoop-client-api-*.jar"))
        if matches:
            m = re.search(r"hadoop-client-api-([\d.]+)\.jar$", matches[0])
            if m:
                return m.group(1)
    except Exception:
        pass
    return FALLBACK_HADOOP_VERSION


def _azure_packages() -> str:
    hadoop_version = _detect_hadoop_version()
    return f"org.apache.hadoop:hadoop-azure:{hadoop_version},{AZURE_STORAGE_JAR}"


def get_spark_session(
    app_name: str = SPARK_APP_NAME_DEFAULT,
    enable_azure: bool = True,
    master: str | None = None,
) -> SparkSession:
    """Create (or fetch) the active SparkSession.

    ``master`` defaults to the ``SPARK_MASTER`` env var, falling back to
    ``local[*]`` for local development. Setting ``.master()`` here always
    overrides ``spark-submit --master``, so on Databricks/YARN/K8s runs
    just leave the ``SPARK_MASTER`` env var unset and pass ``master="yarn"``
    (or whatever applies) explicitly, or drop the ``.master()`` call in the
    Databricks case since a session is already provided there.

    Set ``enable_azure=False`` for pure local runs (unit tests, local-only I/O)
    to skip downloading the Azure connector jars.
    """
    resolved_master = master or os.environ.get("SPARK_MASTER", "local[*]")
    builder = SparkSession.builder.appName(app_name).master(resolved_master)
    if enable_azure:
        builder = builder.config("spark.jars.packages", _azure_packages())

    return builder.getOrCreate()
