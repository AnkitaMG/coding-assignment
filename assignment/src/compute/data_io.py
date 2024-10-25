import logging
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

logger = logging.getLogger(__name__)

def load_data(
        spark: SparkSession,
        file_path: str
) -> DataFrame:
    """
    Loads CSV files from the given path into a DataFrame.

    :parameter:
        spark (SparkSession): The active Spark session.
        file_path (str): The path where CSV files are located.

    :return:
        DataFrame: The loaded DataFrame containing data from CSV files.
    """
    try:
        return spark.read.csv(file_path + "*.csv",header=True, inferSchema=True)
    except Exception as ex:
        logger.error(f"An error occurred while loading data: {ex}")
        raise