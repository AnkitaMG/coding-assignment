import contextlib
import datetime
import time
from collections.abc import Iterator
from typing import Any

from pyspark.sql import SparkSession

def spark_and_dbutils() -> tuple[SparkSession, Any]:
    """
    Returns `(spark, dbutils)` tuple when executed inside databricks notebook.

    This way, if you add `spark, dbutils = spark_and_dbutils()` at the top of your notebook,
    various tools won't complain about those variables being undefined.
    """
    try:
        import IPython

        user_ns = IPython.get_ipython().user_ns
        return user_ns["spark"], user_ns["dbutils"]
    except (ImportError, KeyError):
        raise NotImplementedError(
            "spark_and_dbutils() called outside of databricks notebook"
        ) from None


def initialize(spark: SparkSession, dbutils: Any) -> None:
    """
    In order to read the external delta table, we need to setup some credentials
       This code does the following:
       - Obtain the secrets from our databricks secret environment
       - Store those secrets in the spark configuration
    """
    spark.sparkContext.setCheckpointDir("/tmp/checkpoints")

    TENANT_ID = "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    OAUTH_ENDPOINT = ("https://login.microsoftonline.com/" + TENANT_ID + "/oauth2/token")
    OAUTH_PROVIDER_TYPE = ("org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")

    storage_account = "saaccountdetailstore"

    # Get secrets
    client_id = dbutils.secrets.get("client-id", "id")
    client_secret = dbutils.secrets.get("client-secret", "secret")

    # Spark conf
    spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net","OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",OAUTH_PROVIDER_TYPE)
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",OAUTH_ENDPOINT)
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net",client_id)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net",client_secret)

    spark.conf.set("spark.databricks.io.cache.enabled", "true")
    spark.conf.set("spark.databricks.delta.optimizeWrites.enabled", "auto")


def start_processing(spark: SparkSession, dbutils: Any) -> float:
    start = time.perf_counter()
    initialize(spark, dbutils)
    return start

def finish_processing(spark: SparkSession, start_time: float) -> None:
    end = datetime.timedelta(seconds=time.perf_counter() - start_time)
    print(f"Finished in {end}")
    spark.catalog.clearCache()

@contextlib.contextmanager
def context(spark: SparkSession, dbutils: Any) -> Iterator[None]:
    start = start_processing(spark, dbutils)
    try:
        yield
    finally:
        finish_processing(spark, start)
