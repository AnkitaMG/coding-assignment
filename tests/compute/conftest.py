import os
import pytest
from pyspark.sql import functions as F

@pytest.fixture
def test_transactions(spark, request):
    rootdir = request.config.rootdir
    # Constructing the path to the test data file
    transactions_path = os.path.join(rootdir, 'data', 'input', 'transactions.csv')
    return spark.read.csv(transactions_path, header=True, inferSchema=True)


@pytest.fixture
def test_account_balance(spark, request):
    rootdir = request.config.rootdir
    # Constructing the path to the test data file
    transactions_path = os.path.join(rootdir, 'data', 'output', 'account_balance.csv')
    account_balance_df = spark.read.csv(transactions_path, header=True, inferSchema=True)
    account_balance_df = account_balance_df.withColumn("CurrentBalance", F.col("CurrentBalance").cast("long"))
    return account_balance_df