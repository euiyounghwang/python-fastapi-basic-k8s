# test_my_functions.py
''' pip install pytest '''
import pytest
from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual

''' Create a separate test file (e.g., test_my_functions.py) to test a function that uses Spark:  '''

# A simple function to test
def filter_adults(df):
    return df.filter(df['Age'] >= 30)

# # A pytest fixture to create a Spark session
# @pytest.fixture(scope="session")
# def spark_session():
#     spark = SparkSession.builder.appName("PytestPySparkExample").getOrCreate()
#     yield spark
#     spark.stop()

def test_filter_adults(spark_session):
    # Sample input data
    input_data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    input_df = spark_session.createDataFrame(input_data, ["Name", "Age"])

    # Expected output data
    expected_data = [("Bob", 30), ("Charlie", 35)]
    expected_df = spark_session.createDataFrame(expected_data, ["Name", "Age"])

    # Call the function being tested
    result_df = filter_adults(input_df)

    # Use the built-in utility to assert DataFrame equality
    assertDataFrameEqual(result_df, expected_df)
