# Databricks notebook source
import logging
import dbutils

dbutils.widgets.text("FILE_PATH", "abfss://accountbalancesheet@saaccountdetailstore.dfs.core.windows.net/input/")
file_path = dbutils.widgets.get("FILE_PATH")
print("file_path: ", file_path)

from assignment.src import initializer
from assignment.src.compute import (
    data_io,
    transform
)

logger = logging.getLogger(__name__)

# Initializing Spark session
spark, dbutils = initializer.spark_and_dbutils()

start_time = initializer.start_processing(spark, dbutils)

# Loading bank transactions csv data into a DataFrame from Azure Blob Path
transactions_df = data_io.load_data(spark, file_path)

# Cleaning and Transforming the data
clean_df = transform.clean_transactions_data(transactions_df)

# Adding new column for account balance calculation
transformed_transactions_df = transform.calculate_account_balance(clean_df)

initializer.finish_processing(spark, start_time)
print("Job Completed")