"""Reads back persisted validation results without re-running the pipeline.

Usage:
  python scripts/inspect_results.py --config config/pipeline_config.yaml
  python scripts/inspect_results.py --config config/pipeline_config.yaml --env azure --env-file .env
  python scripts/inspect_results.py --config config/pipeline_config.yaml --latest-only
"""
from __future__ import annotations

import argparse
import sys


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Inspect persisted validation results (latest run on top).")
    parser.add_argument("--config", required=True, help="Path to pipeline_config.yaml")
    parser.add_argument("--env-file", default=None, help="Path to a .env file with Azure credentials")
    parser.add_argument(
        "--env",
        default=None,
        help="Environment profile to inspect (key under 'environments' in the config file). "
        "Defaults to the config's top-level 'environment', or the PIPELINE_ENV env var.",
    )
    parser.add_argument("--latest-only", action="store_true", help="Show only the most recent run instead of full history")
    args = parser.parse_args(argv)

    from data_validation.config import load_pipeline_config
    from data_validation.pipeline import configure_azure_if_needed, read_results_history, spark_session_for_config

    config = load_pipeline_config(args.config, env_file=args.env_file, environment=args.env)
    spark = spark_session_for_config(config, app_name=f"inspect-{config.dataset_name}")
    configure_azure_if_needed(spark, config)

    history_df = read_results_history(spark, config)

    if args.latest_only:
        latest_run_id = history_df.select("run_id").first()
        if latest_run_id is None:
            print(f"No results found yet at {config.output_path}")
            return 0
        history_df = history_df.filter(history_df.run_id == latest_run_id["run_id"])

    label = "latest run" if args.latest_only else "full history, latest run on top"
    print(f"Validation results for dataset '{config.dataset_name}' from {config.output_path} ({label}):")
    history_df.show(truncate=False, n=200)
    return 0


if __name__ == "__main__":
    sys.exit(main())
