"""Configuration loading for the validation pipeline.

Two config sources are combined:
  * ``pipeline_config.yaml``   -- non-secret settings (source, per-environment
    destinations, validation rules)
  * environment variables/.env -- secrets (tenant id, client id/secret)
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv


@dataclass(frozen=True)
class AzureAuthConfig:
    """Service principal (OAuth) credentials for ADLS Gen2 access."""

    storage_account: str
    container: str
    tenant_id: str
    client_id: str
    client_secret: str

    @classmethod
    def from_env(cls, storage_account: str, container: str) -> "AzureAuthConfig":
        missing = [
            name
            for name in ("AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET")
            if not os.environ.get(name)
        ]
        if missing:
            raise EnvironmentError(
                f"Missing required Azure credential env vars: {', '.join(missing)}. "
                "Copy .env.example to .env and fill in real values, or export them directly."
            )
        return cls(
            storage_account=storage_account,
            container=container,
            tenant_id=os.environ["AZURE_TENANT_ID"],
            client_id=os.environ["AZURE_CLIENT_ID"],
            client_secret=os.environ["AZURE_CLIENT_SECRET"],
        )


@dataclass(frozen=True)
class PipelineConfig:
    dataset_name: str
    environment: str
    input_path: str
    input_format: str
    output_path: str
    output_format: str
    rules: list[dict[str, Any]]
    write_mode: str = "append"
    partition_by: list[str] = field(default_factory=lambda: ["run_date"])
    raw: dict[str, Any] = field(default_factory=dict)


def load_pipeline_config(
    config_path: str | Path,
    env_file: str | Path | None = None,
    environment: str | None = None,
) -> PipelineConfig:
    """Load pipeline_config.yaml and the .env file (if present) into a PipelineConfig.

    ``environment`` selects which entry under the config's ``environments:``
    section supplies the destination (and any source overrides) for this
    run. It defaults to the ``PIPELINE_ENV`` env var, then the config's
    top-level ``environment`` key.
    """
    load_dotenv(dotenv_path=env_file) if env_file else load_dotenv()

    config_path = Path(config_path)
    with config_path.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}

    env_name = environment or os.environ.get("PIPELINE_ENV") or raw.get("environment")
    environments = raw.get("environments", {})
    if not env_name or env_name not in environments:
        raise ValueError(
            f"Unknown environment '{env_name}' in {config_path}. "
            f"Defined environments: {sorted(environments)}"
        )
    env_block = environments[env_name]

    source = {**raw.get("source", {}), **env_block.get("source", {})}
    destination = env_block.get("destination", {})
    if not destination:
        raise ValueError(f"environments.{env_name} in {config_path} must define 'destination'")

    rules = raw.get("rules")
    if not rules:
        raise ValueError(f"{config_path} must define at least one rule under 'rules'")

    return PipelineConfig(
        dataset_name=raw["dataset_name"],
        environment=env_name,
        input_path=source["path"],
        input_format=source.get("format", "csv"),
        output_path=destination["path"],
        output_format=destination.get("format", "parquet"),
        rules=rules,
        write_mode=destination.get("write_mode", "append"),
        partition_by=destination.get("partition_by", ["run_date"]),
        raw=raw,
    )
