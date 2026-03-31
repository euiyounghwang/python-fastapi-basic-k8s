import json
from pyspark.sql import SparkSession

# Initialize Spark Session (if not already done)
spark = SparkSession.builder.appName("JsonPrint").getOrCreate()

# Create a sample DataFrame
df = spark.createDataFrame([(1, "Alice", 10), (2, "Bob", 20), (3, "Charlie", 30)], ["id", "name", "age"])

# Convert the DataFrame to an RDD of JSON strings, then collect into a Python list
json_strings = df.toJSON().collect()

# Pretty print the list of JSON objects using Python's json module
for json_str in json_strings:
    # Use json.loads to convert the string to a Python object (dictionary)
    data = json.loads(json_str)
    # Use json.dumps with indent for pretty printing
    pretty_json = json.dumps(data, indent=4)
    print(pretty_json)
