from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark import SparkContext
import logging
from logging.handlers import RotatingFileHandler
import json
import argparse
from threading import Thread


def create_spark_session():
    """Initializes and returns a Spark session."""
    spark = SparkSession.builder \
        .getOrCreate()
    return spark


def get_logger(path_logger_file):
    # Get the LogManager object via the Py4j bridge
    # log4j_logger = spark._jvm.org.apache.log4j
    # logger = log4j_logger.LogManager.getLogger(__name__)
    # 1. Configure the Python logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # # Create a file handler and set the format
    handler = RotatingFileHandler(path_logger_file, maxBytes=1000000, backupCount=5)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


def multiple_cal(x):
    return x * x


def parse_and_transform(json_string):
    """
    Parses a JSON string and returns a transformed value (e.g., a dictionary, 
    or extracts specific fields).
    """
    try:
        data = json.loads(json_string)
        # Example transformation: return a new dictionary with uppercase name
        # return {"id": data["id"], "upper_name": data["name"].upper()}
        print('\n\n')
        print(f"parse_and_transform : {data}")
        print('\n\n')
        return data
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return None
    

def work():
    '''
    Docstring for work
    25/12/18 15:24:42 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0) (127.0.0.1, executor 0, partition 0, PROCESS_LOCAL, 7825 bytes)
    25/12/18 15:24:42 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 127.0.0.1:15716 (size: 3.8 KiB, free: 434.4 MiB)
    25/12/18 15:24:43 INFO TaskSetManager: Starting task 1.0 in stage 0.0 (TID 1) (127.0.0.1, executor 0, partition 1, PROCESS_LOCAL, 7825 bytes)
    25/12/18 15:24:44 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 1417 ms on 127.0.0.1 (executor 0) (1/10)
    25/12/18 15:24:44 INFO PythonAccumulatorV2: Connected to AccumulatorServer at host: 127.0.0.1 port: 12570
    25/12/18 15:24:44 INFO TaskSetManager: Starting task 2.0 in stage 0.0 (TID 2) (127.0.0.1, executor 0, partition 2, PROCESS_LOCAL, 7869 bytes)
    25/12/18 15:24:44 INFO TaskSetManager: Finished task 1.0 in stage 0.0 (TID 1) in 137 ms on 127.0.0.1 (executor 0) (2/10)
    25/12/18 15:24:44 INFO TaskSetManager: Starting task 3.0 in stage 0.0 (TID 3) (127.0.0.1, executor 0, partition 3, PROCESS_LOCAL, 7925 bytes)
    25/12/18 15:24:44 INFO TaskSetManager: Finished task 2.0 in stage 0.0 (TID 2) in 80 ms on 127.0.0.1 (executor 0) (3/10)
    25/12/18 15:24:44 INFO TaskSetManager: Starting task 4.0 in stage 0.0 (TID 4) (127.0.0.1, executor 0, partition 4, PROCESS_LOCAL, 7925 bytes)
    25/12/18 15:24:44 INFO TaskSetManager: Finished task 3.0 in stage 0.0 (TID 3) in 96 ms on 127.0.0.1 (executor 0) (4/10)
    25/12/18 15:24:44 INFO TaskSetManager: Starting task 5.0 in stage 0.0 (TID 5) (127.0.0.1, executor 0, partition 5, PROCESS_LOCAL, 7925 bytes)
    25/12/18 15:24:44 INFO TaskSetManager: Finished task 4.0 in stage 0.0 (TID 4) in 81 ms on 127.0.0.1 (executor 0) (5/10)
    25/12/18 15:24:44 INFO TaskSetManager: Starting task 6.0 in stage 0.0 (TID 6) (127.0.0.1, executor 0, partition 6, PROCESS_LOCAL, 7925 bytes)
    25/12/18 15:24:44 INFO TaskSetManager: Finished task 5.0 in stage 0.0 (TID 5) in 86 ms on 127.0.0.1 (executor 0) (6/10)
    25/12/18 15:24:44 INFO TaskSetManager: Starting task 7.0 in stage 0.0 (TID 7) (127.0.0.1, executor 0, partition 7, PROCESS_LOCAL, 7925 bytes)
    25/12/18 15:24:44 INFO TaskSetManager: Finished task 6.0 in stage 0.0 (TID 6) in 77 ms on 127.0.0.1 (executor 0) (7/10)
    25/12/18 15:24:44 INFO TaskSetManager: Starting task 8.0 in stage 0.0 (TID 8) (127.0.0.1, executor 0, partition 8, PROCESS_LOCAL, 7925 bytes)
    25/12/18 15:24:44 INFO TaskSetManager: Finished task 7.0 in stage 0.0 (TID 7) in 78 ms on 127.0.0.1 (executor 0) (8/10)
    25/12/18 15:24:44 INFO TaskSetManager: Starting task 9.0 in stage 0.0 (TID 9) (127.0.0.1, executor 0, partition 9, PROCESS_LOCAL, 7953 bytes)
    25/12/18 15:24:44 INFO TaskSetManager: Finished task 8.0 in stage 0.0 (TID 8) in 84 ms on 127.0.0.1 (executor 0) (9/10)
    25/12/18 15:24:44 INFO TaskSetManager: Finished task 9.0 in stage 0.0 (TID 9) in 77 ms on 127.0.0.1 (executor 0) (10/10)
    '''
    # spark = create_spark_session()
    # Create SparkContext (usually available as 'sc' in notebooks like Databricks/Jupyter)
    # Initialize a Spark Session (if not already done)
    spark = SparkSession.builder.appName("ParallelizeJsonList").getOrCreate()
    sc = SparkContext.getOrCreate()

    # data = range(1, 01) # A local Python collection
    data = [num for num in range(1001)]
    logger.info(f"data : {data}")
    # Distribute the data into 10 partitions
    parallel_rdd = sc.parallelize(data, 10)
    logger.info(f"parallel_rdd : {parallel_rdd}")

    # Perform a parallel operation (e.g., squaring each number)
    # The `map` operation runs in parallel across the 10 partitions
    # squared_rdd = parallel_rdd.map(lambda x: x * x)
    squared_rdd = parallel_rdd.map(lambda x: multiple_cal(x))

    # Collect results (brings data back to the driver, use cautiously with large data)
    results = squared_rdd.collect()
    logger.info(f"spark-paralllel-task : {results}")

    # 1. Define your list of JSON strings (must be newline-delimited JSON format)
    json_data_list = [
        '{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}',
        '{"name":"Justin","address":{"city":"Chicago","state":"Illinois"}}',
        '{"name":"Tania","address":{"city":"New York","state":"New York"}}'
    ]

    # 2. Parallelize the list to create an RDD with a specific number of partitions
    # We specify 4 partitions as an example. 
    # The optimal number is typically 2-4 times the number of CPU cores in your cluster.
    num_partitions = 3
    json_rdd = sc.parallelize(json_data_list, numSlices=num_partitions)
    
    # Verify the number of partitions (optional)
    logger.info(f"Number of partitions in RDD: {json_rdd.getNumPartitions()}")

    # Apply the map function in parallel
    # transformed_rdd  = json_rdd.map(lambda x: parse_and_transform(x, logger))
    # transformed_rdd  = json_rdd.map(lambda x: parse_and_transform(x))

    # 3. Convert the RDD of JSON strings into a DataFrame
    # Spark will automatically infer the schema and distribute the reading process across the partitions.
    # The RDD is a collection of JSON strings. We can create a DataFrame from it.
    json_df = spark.read.json(json_rdd)

    partition_data = json_df.rdd.glom().collect()
    print(f"Data distribution across partitions: {[p for p in partition_data]}")
    
    # 4. Work with the resulting DataFrame
    json_df.show()
    json_df.printSchema()

    # Convert the DataFrame to an RDD of JSON strings, then collect into a Python list
    json_strings = json_df.toJSON().collect()

    '''
    {
        "address": {
            "city": "Columbus",
            "state": "Ohio"
        },
        "name": "Yin"
    }
    {
        "address": {
            "city": "Chicago",
            "state": "Illinois"
        },
        "name": "Justin"
    }
    {
        "address": {
            "city": "New York",
            "state": "New York"
        },
        "name": "Tania"
    }
    '''
    # Pretty print the list of JSON objects using Python's json module
    for json_str in json_strings:
        # Use json.loads to convert the string to a Python object (dictionary)
        data = json.loads(json_str)
        # Use json.dumps with indent for pretty printing
        pretty_json = json.dumps(data, indent=4)
        print(pretty_json)

    # Collect results (brings data back to the driver, use cautiously with large data)
    # results = transformed_rdd.collect()
    # logger.info(f"transformed_rdd : {results}")


    # Stop the Spark session
    spark.stop()


if __name__ == '__main__':
# 1. Create the parser
    parser = argparse.ArgumentParser(description="A PySpark job with data parallel job")
    
    # 2. Add arguments
    parser.add_argument('--log_path', type=str, required=True, help="log_path")
    # 3. Parse the arguments from sys.argv
    args = parser.parse_args()
    
    log_path = args.log_path

    try:
        T = []
        '''
        th1 = Thread(target=test)
        th1.daemon = True
        th1.start()
        T.append(th1)
        '''

        logger = get_logger(log_path)
        
        ''' es/kafka/kibana/logstash prcess check thread'''
        ''' main process to collect metrics'''
        main_th = Thread(target=work, args=())
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