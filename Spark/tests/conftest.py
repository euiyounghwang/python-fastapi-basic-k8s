import pytest
from pyspark.sql import SparkSession

# In order to share fixtures across multiple test files, pytest suggests defining fixtures in a conftest.py

# A pytest fixture to create a Spark session
@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder.appName("PytestPySparkExample").getOrCreate()
    yield spark
    spark.stop()