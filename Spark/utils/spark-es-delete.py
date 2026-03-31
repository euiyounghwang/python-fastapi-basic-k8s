from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def create_spark_session(desc):
    """Initializes and returns a Spark session."""
    spark = SparkSession.builder \
        .getOrCreate(desc)
    return spark


def work():
    spark = create_spark_session("EsDeleteById")

    # Create a list or RDD of IDs you want to delete.
    # The DataFrame needs columns that map to the Elasticsearch index and ID fields.
    # The 'es.mapping.id' property tells ES-Hadoop which DataFrame column contains the ES document ID.

    ids_to_delete = [
        ("id_1",),
        ("id_2",),
        ("id_3",)
    ]

    # Create a DataFrame with the IDs
    # The column name 'doc_id' should be specified in the write options later
    ids_df = spark.createDataFrame(ids_to_delete, ["doc_id"])

    # Write the DataFrame to Elasticsearch with the 'delete' operation
    # Make sure to configure your Elasticsearch connection details
    es_write_conf = {
        "es.nodes": "localhost",
        "es.port": "9200",
        "es.resource": "your_index_name/_doc", # Target index and doc type (use _doc for modern ES)
        "es.mapping.id": "doc_id", # Map the 'doc_id' column to the ES document ID
        "es.write.operation": "delete" # Specify the delete operation
    }

    ids_df.write.format("org.elasticsearch.spark.sql").options(**es_write_conf).mode("append").save()


if __name__ == '__main__':
    work()