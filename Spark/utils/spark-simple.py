
# simple_example.py
from pyspark.sql import SparkSession

''' pip install pyspark '''
''' To install and test PySpark, you can use pip to install the library, write a simple application to verify the installation, and use testing frameworks like pytest for more robust testing.  '''

# Create a Spark session
spark = SparkSession.builder.appName("SimplePySparkExample").getOrCreate()

# Create a DataFrame with sample data
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Show the DataFrame
print("DataFrame content:")
df.show()

# Stop the Spark session
spark.stop()