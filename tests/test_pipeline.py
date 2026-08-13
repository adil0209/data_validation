from dataclasses import replace
from pathlib import Path

from data_validation.config import load_pipeline_config
from data_validation.pipeline import run_pipeline

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_run_pipeline_local_writes_expected_results(spark, tmp_path):
    output_path = str(tmp_path / "results")
    base_config = load_pipeline_config(REPO_ROOT / "config" / "pipeline_config.yaml", environment="local")
    config = replace(base_config, output_path=output_path, write_mode="overwrite")

    results_df = run_pipeline(spark, config)

    # Every rule from pipeline_config.yaml produced exactly one result row.
    assert results_df.count() == 8
    assert set(results_df.columns) == {
        "run_id",
        "run_timestamp",
        "run_date",
        "dataset",
        "rule_id",
        "rule_type",
        "column_name",
        "severity",
        "description",
        "total_records",
        "failed_records",
        "passed_records",
        "pass_rate",
        "status",
    }

    # The sample data was constructed with known failures for each rule type.
    by_rule = {row["rule_id"]: row for row in results_df.collect()}
    assert by_rule["order_id_unique"]["status"] == "FAIL"
    assert by_rule["customer_email_format"]["status"] == "FAIL"
    assert by_rule["order_amount_range"]["status"] == "FAIL"
    assert by_rule["order_status_allowed"]["status"] == "FAIL"
    assert by_rule["discount_not_exceeding_amount"]["status"] == "FAIL"
    assert by_rule["cancellation_rate_under_threshold"]["status"] == "FAIL"
    assert by_rule["cancellation_rate_under_threshold"]["failed_records"] == 2

    # Results were actually persisted to the output path.
    written_df = spark.read.parquet(output_path)
    assert written_df.count() == 8
