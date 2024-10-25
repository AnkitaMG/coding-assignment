## Description

* The main notebook/starting point is
```
account_balance_calculation.py
Path: assignment/notebooks/account_balance_calculation.py
```
* Test folder written in pytest : test
* Test data folders with csv file : data

## Used technologies

* [Python](https://www.python.org/ "Official Python website") - by Spark supported language
* [Poetry](https://python-poetry.org// "Official Poetry website") - build system for Python and dependency management
* [Apache Spark](https://spark.apache.org/ "Official Spark Spark website") - distributed multi-language runtime engine optimized for data
  processing
* [Azure Databricks](https://www.databricks.com/ "Official Azure Databricks website") - managed Spark service in Azure

### Prerequisites
All you need is the following configuration already installed:

* Oracle/Open JDK version = 8
* Python version >=3.12
* Poetry version >=1.5
* Pyspark version >=3.2.0
* Pytest version >=7.1.0

## How to build / poetry commands

* Install Dependencies
```shell
poetry install
```
* Run all Test
```shell
poetry run pytest
```
* Run specific Tests
```shell
poetry run pytest tests/compute/test_transform.py -s
```
* Run a particular test function within a file
```shell
poetry run pytest tests/compute/test_transform.py::test_clean_transactions_data -s
```
* Build python wheel
```shell
poetry build
```