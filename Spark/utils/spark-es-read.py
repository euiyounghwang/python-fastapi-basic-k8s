from pyspark.sql import SparkSession
import logging
import sys
import argparse
from threading import Thread
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

def create_spark_session():
    """Initializes and returns a Spark session."""
    spark = SparkSession.builder \
        .getOrCreate()
    return spark


def work(spark, es_index):
    ''' You need to include the elasticsearch-hadoop connector JAR in your Spark environment when running spark-submit or pyspark '''
    ''' 
    # Example command using spark-submit with the connector JAR
    spark-submit --jars /path/to/elasticsearch-hadoop-*.jar your_spark_script.py

    Download ES-Hadoop: https://www.elastic.co/downloads/past-releases?product=es-hadoop
    '''
    # Define your Elasticsearch bool query as a JSON string
    # This query finds documents where 'status' is 'publish' AND 'authors' is '104'
    # es_query = """
    # {
    #     "query": {
    #         "bool": {
    #         "filter": [
    #             {
    #             "match": {
    #                 "status": "publish"
    #             }
    #             },
    #             {
    #             "match": {
    #                 "authors": "104"
    #             }
    #             }
    #         ]
    #         }
    #     }
    # }
    # """
    es_query = """
    {
        "query" : {
            "match_all" : {}
        },
        "size": 1
    }
    """

    # Read data from Elasticsearch into a Spark DataFrame using the es.query option
    """
    df = spark.read \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "your_es_host") \
        .option("es.port", "9200") \
        .option("es.net.http.auth.user", "your_user") \
        .option("es.net.http.auth.pass", "your_password") \
        .option("es.query", es_query) \
        .load("your_index_name") # Specify the index to load from
    """
    # Define connection properties
    config = {
        # "es.nodes": "https://<your-elasticsearch-host>", 
        # "es.port": "443", # Use 443 for HTTPS, or 9200 if configured
        "es.nodes": "http://{}".format(es_source), 
        "es.port": "{}".format(es_port), # Use 443 for HTTPS, or 9200 if configured
        # "es.net.ssl": "true",
        # "es.net.http.auth.user": "your_username",
        # "es.net.http.auth.pass": "your_password",
        # "es.nodes.wan.only": "true", # Use if needed for cloud/remote clusters
        "es.resource": es_index,
        "es.query" : es_query
    }

    logger.info(f"es_source : {es_source}, es_port : {es_port}, es_index : {es_index}")
    logger.info(f"es_query : {es_query}")

    # Read data from Elasticsearch
    df = spark.read.format("org.elasticsearch.spark.sql").options(**config).load()

    # Show the resulting DataFrame
    df.show(10)
    #df.printSchema()

    logger.info("--- Printing DataFrame rows as JSON ---")
    logger.info(df.count())
    logger.info(f"ES total count : {df.count()}")

    return (es_index, df.count())


def work_parallel(spark, es_index):
    # https://velog.io/@thedev_junyoung/PythonThreadPoolExecutor%EC%99%80-%EA%B5%AC%EC%A1%B0OOP
    # ThreadPoolExecutor를 사용하여 병렬 처리
    # max_workers는 클러스터 환경에 맞게 조정 (예: Executor 수)
    processes = [es_index, es_index]
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_to_es_read = {executor.submit(work, spark, process): process for process in processes}
        results = []
        for future in as_completed(future_to_es_read):
            results.append(future.result())

    '''
    --- All Jobs are completed ---
    25/12/17 18:09:42 INFO __main__: es_index: sample, 레코드 수: 2
    25/12/17 18:09:42 INFO __main__: es_index: sample, 레코드 수: 2
    '''
    logger.info("\n--- All Jobs are completed ---")
    for es_index, count in results:
        logger.info(f"es_index: {es_index}, 레코드 수: {count}")

    spark.stop()


if __name__ == '__main__':
     # 1. Create the parser
    parser = argparse.ArgumentParser(description="A PySpark job with Elasticsearch cluster")
    
    # 2. Add arguments
    parser.add_argument('--es_source', type=str, required=True, help="es_source")
    parser.add_argument('--es_port', type=str, required=True, help="es_port")
    parser.add_argument('--es_index', type=str, required=True, help="es_index")

    # 3. Parse the arguments from sys.argv
    args = parser.parse_args()
    
    es_source = args.es_source
    es_index = args.es_index
    es_port = args.es_port

    try:
        T = []
        '''
        th1 = Thread(target=test)
        th1.daemon = True
        th1.start()
        T.append(th1)
        '''
        
         # Initialize Spark Session (ensure elasticsearch-spark connector is in classpath)
        spark = create_spark_session()

        # Get the LogManager object via the Py4j bridge
        log4j_logger = spark._jvm.org.apache.log4j
        logger = log4j_logger.LogManager.getLogger(__name__)

        ''' es/kafka/kibana/logstash prcess check thread'''
        ''' main process to collect metrics'''
        main_th = Thread(target=work, args=(spark, es_index,))
        # main_th = Thread(target=work_parallel, args=(spark, es_index, ))
        main_th.daemon = True
        main_th.start()
        T.append(main_th)

        # wait for all threads to terminate
        for t in T:
            while t.is_alive():
                t.join(0.5)
    
    except (KeyboardInterrupt, SystemExit):
        logger.info("# Interrupted..")

    finally:
        logger.info("PySpark has been completed..!")
    