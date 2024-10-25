from pyspark.sql import functions as F


from assignment.src.compute.transform import (
    clean_transactions_data,
    calculate_account_balance
)


def test_clean_transactions_data(test_transactions):
    cleaned_df = clean_transactions_data(test_transactions)
    cleaned_df.show()

    # Assertions to check for expected transformations
    # 1. Checking if TransactionType has no leading/trailing spaces
    assert cleaned_df.filter(F.col("TransactionType").like("% %")).count() == 0, "TransactionType contains spaces"

    # 2. Checking if TransactionDate is in the correct format YYYY-MM-DD
    date_format_check = cleaned_df.filter(~F.col("TransactionDate").rlike(r"^\d{4}-\d{2}-\d{2}$"))
    assert date_format_check.count() == 0, "Not all TransactionDate entries are in the format yyyy-MM-dd"


def test_calculate_account_balance(test_transactions, test_account_balance):
    cleaned_df = clean_transactions_data(test_transactions)
    balance_df = calculate_account_balance(cleaned_df)

    # Asserting that 'CurrentBalance' column exists
    assert 'CurrentBalance' in balance_df.columns, "CurrentBalance column not found in balance_df"

    # Checking if schemas are the same
    assert balance_df.schema == test_account_balance.schema, "DataFrame schemas are different"

    # Sorting both DataFrames by all columns to ensure order consistency before comparison
    sorted_balance_df = balance_df.sort(balance_df.columns)
    sorted_test_account_balance = test_account_balance.sort(test_account_balance.columns)

    # Collecting and comparing rows
    assert sorted_balance_df.collect() == sorted_test_account_balance.collect(), "DataFrames rows are different"

