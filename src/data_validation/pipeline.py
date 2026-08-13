"""Orchestrates the end-to-end pipeline: read -> validate -> write results."""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from data_validation.azure_auth import configure_adls_oauth
from data_validation.config import AzureAuthConfig, PipelineConfig, load_pipeline_config
from data_validation.io.readers import read_input
from data_validation.io.writers import write_results
from data_validation.validators.engine import ValidationEngine
from data_validation.validators.rules import ValidationRule

logger = logging.getLogger(__name__)

_ABFSS_PREFIX = "abfss://"


def configure_azure_if_needed(spark: SparkSession, config: PipelineConfig) -> None:
    """Registers service-principal OAuth credentials if either the input or output path is abfss://."""
    if config.output_path.startswith(_ABFSS_PREFIX) or config.input_path.startswith(_ABFSS_PREFIX):
        storage_account, container = _parse_abfss_account_and_container(
            config.output_path if config.output_path.startswith(_ABFSS_PREFIX) else config.input_path
        )
        auth = AzureAuthConfig.from_env(storage_account=storage_account, container=container)
        configure_adls_oauth(spark, auth)
        logger.info("Configured ADLS Gen2 OAuth for account '%s'", storage_account)


def run_pipeline(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Runs the full pipeline and returns the validation results DataFrame."""
    configure_azure_if_needed(spark, config)

    logger.info("Reading input dataset '%s' from %s (%s)", config.dataset_name, config.input_path, config.input_format)
    input_df = read_input(spark, config.input_path, config.input_format)

    rules = [ValidationRule.from_dict(r) for r in config.rules]
    logger.info("Loaded %d validation rule(s) for dataset '%s'", len(rules), config.dataset_name)

    engine = ValidationEngine(spark=spark, dataset_name=config.dataset_name, rules=rules)
    results_df = engine.run(input_df)

    logger.info(
        "Writing validation results to %s (%s, mode=%s)",
        config.output_path,
        config.output_format,
        config.write_mode,
    )
    write_results(
        results_df,
        path=config.output_path,
        fmt=config.output_format,
        mode=config.write_mode,
        partition_by=config.partition_by,
    )

    failed = results_df.filter("status = 'FAIL'").count()
    total = results_df.count()
    logger.info("Validation complete: %d/%d rule(s) failed", failed, total)

    return results_df


def read_results_history(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Reads back every persisted result (all runs, all partitions) for this dataset,
    with the most recently run rule checks on top."""
    history_df = read_input(spark, config.output_path, config.output_format)
    return history_df.orderBy(F.desc("run_timestamp"), "status", "rule_id")


def _parse_abfss_account_and_container(path: str) -> tuple[str, str]:
    # abfss://<container>@<account>.dfs.core.windows.net/...
    without_scheme = path[len(_ABFSS_PREFIX):]
    host_and_rest = without_scheme.split("/", 1)[0]
    container, account_fqdn = host_and_rest.split("@", 1)
    storage_account = account_fqdn.split(".", 1)[0]
    return storage_account, container


def spark_session_for_config(config: PipelineConfig, app_name: str) -> SparkSession:
    from data_validation.spark_session import get_spark_session

    uses_azure = config.output_path.startswith(_ABFSS_PREFIX) or config.input_path.startswith(_ABFSS_PREFIX)
    return get_spark_session(app_name=app_name, enable_azure=uses_azure)


def run_from_config_files(
    config_path: str, env_file: str | None = None, environment: str | None = None
) -> tuple[SparkSession, PipelineConfig, DataFrame]:
    config = load_pipeline_config(config_path, env_file=env_file, environment=environment)
    spark = spark_session_for_config(config, app_name=f"validate-{config.dataset_name}")
    results_df = run_pipeline(spark, config)
    return spark, config, results_df
