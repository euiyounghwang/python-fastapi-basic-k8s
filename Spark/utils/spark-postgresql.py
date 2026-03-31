from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def create_spark_session():
    """Initializes and returns a Spark session."""
    spark = SparkSession.builder \
        .getOrCreate()
    return spark


def work():
    spark = create_spark_session()

    jdbc_url = "jdbc:postgresql://<hostname>:<port>/<database>"
    connection_properties = {
        "user": "<username>",
        "password": "<password>",
        "driver": "org.postgresql.Driver"
    }

    df_jdbc = spark.read.jdbc(
        url=jdbc_url,
        table="large_table_name",
        # Enable parallel reads
        column="id",          # Partitioning column (must be numeric)
        lowerBound=1,         # Minimum value of the partitioning column
        upperBound=1000000,   # Maximum value of the partitioning column
        numPartitions=10,     # Number of parallel tasks
        properties=connection_properties
    )

    print(f"JDBC DataFrame partitions: {df_jdbc.rdd.getNumPartitions()}")

if __name__ == '__main__':
    work()