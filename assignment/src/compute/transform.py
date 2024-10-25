import logging
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F, Window

logger = logging.getLogger(__name__)

def clean_transactions_data(
        transactions_df: DataFrame
) -> DataFrame:
    """
    Cleans the transactions DataFrame by removing extra spaces from the TransactionType
    and converting the TransactionDate to 'YYYY-MM-DD' format.

    Args:
        transactions_df (DataFrame): Input DataFrame with raw transaction details.

    Returns:
        DataFrame: Cleaned DataFrame with corrected TransactionType and formatted TransactionDate.
    """
    try:
        # Removing spaces from the TransactionType and trim the values
        cleaned_df = transactions_df.withColumn(
        "TransactionType", F.trim(F.col("TransactionType"))
        )

        # Converting TransactionDate from YYYYMMDD to YYYY-MM-DD format
        cleaned_df = cleaned_df.withColumn(
            "TransactionDate", F.date_format(F.to_date(F.col("TransactionDate").cast("string"), "yyyyMMdd"), "yyyy-MM-dd")
        )

        return cleaned_df
    except Exception as ex:
        logger.error(f"An error occurred while cleaning transactions data: {ex}")
        raise


def calculate_account_balance(
        transactions_df: DataFrame
) -> DataFrame:
    """
    Calculates the balance of each account after each transaction.

    Args:
        transactions_df (DataFrame): Input DataFrame with transaction details (TransactionDate, AccountNumber, TransactionType, Amount).

    Returns:
        DataFrame: DataFrame with an additional 'Balance' column, showing the cumulative balance for each account.
    """
    try:
        # Adjusting the amount based on transaction type (Credit as positive, Debit as negative)
        transactions_with_balance_df = transactions_df.withColumn(
            "CurrentBalance",
            F.when(transactions_df.TransactionType == "Credit", transactions_df.Amount)
            .otherwise(-transactions_df.Amount)
        )

        # Defining a window specification to partition by AccountNumber and order by TransactionDate
        window_spec = Window.partitionBy("AccountNumber").orderBy("TransactionDate")

        # Calculating cumulative balance
        balance_df = transactions_with_balance_df.withColumn(
            "CurrentBalance", F.sum("CurrentBalance").over(window_spec)
        )

        return balance_df
    except Exception as ex:
        logger.error(f"An error occurred while calculating account balance: {ex}")
        raise