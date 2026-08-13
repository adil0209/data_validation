"""Command-line entrypoint: run-validation-pipeline --config config/pipeline_config.yaml"""
from __future__ import annotations

import argparse
import logging
import sys

from data_validation.settings import DEFAULT_LOG_LEVEL


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the PySpark data validation pipeline.")
    parser.add_argument("--config", required=True, help="Path to pipeline_config.yaml")
    parser.add_argument("--env-file", default=None, help="Path to a .env file with Azure credentials")
    parser.add_argument(
        "--env",
        default=None,
        help="Environment profile to run (key under 'environments' in the config file). "
        "Defaults to the config's top-level 'environment', or the PIPELINE_ENV env var.",
    )
    parser.add_argument("--log-level", default=DEFAULT_LOG_LEVEL, help=f"Logging level (default: {DEFAULT_LOG_LEVEL})")
    args = parser.parse_args(argv)

    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

    from data_validation.pipeline import read_results_history, run_from_config_files

    spark, config, results_df = run_from_config_files(args.config, env_file=args.env_file, environment=args.env)

    history_df = read_results_history(spark, config)
    print(f"\nValidation history for dataset '{config.dataset_name}' (latest run on top):")
    history_df.show(truncate=False, n=200)

    failed = results_df.filter("status = 'FAIL' AND severity = 'error'").count()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
