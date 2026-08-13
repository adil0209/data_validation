"""Configures a SparkSession's Hadoop conf for OAuth (service principal) access to ADLS Gen2."""
from __future__ import annotations

from pyspark.sql import SparkSession

from data_validation.config import AzureAuthConfig
from data_validation.settings import OAUTH_PROVIDER


def configure_adls_oauth(spark: SparkSession, auth: AzureAuthConfig) -> None:
    """Register service-principal OAuth credentials for the given storage account.

    After this call, paths of the form
    ``abfss://<container>@<storage_account>.dfs.core.windows.net/...``
    can be read/written directly through the Spark/Hadoop filesystem API.
    """
    account_host = f"{auth.storage_account}.dfs.core.windows.net"
    hconf = spark._jsc.hadoopConfiguration()

    hconf.set(f"fs.azure.account.auth.type.{account_host}", "OAuth")
    hconf.set(f"fs.azure.account.oauth.provider.type.{account_host}", OAUTH_PROVIDER)
    hconf.set(f"fs.azure.account.oauth2.client.id.{account_host}", auth.client_id)
    hconf.set(f"fs.azure.account.oauth2.client.secret.{account_host}", auth.client_secret)
    hconf.set(
        f"fs.azure.account.oauth2.client.endpoint.{account_host}",
        f"https://login.microsoftonline.com/{auth.tenant_id}/oauth2/token",
    )


def abfss_path(auth: AzureAuthConfig, relative_path: str = "") -> str:
    """Build an ``abfss://`` URI for the configured container, e.g. abfss://container@account.dfs.core.windows.net/relative/path."""
    relative_path = relative_path.lstrip("/")
    base = f"abfss://{auth.container}@{auth.storage_account}.dfs.core.windows.net"
    return f"{base}/{relative_path}" if relative_path else base
