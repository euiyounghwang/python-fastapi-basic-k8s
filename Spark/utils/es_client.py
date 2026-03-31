from elasticsearch import Elasticsearch

# Define your Elasticsearch host(s)
# Replace with your cluster URL and port, e.g., 'http://localhost:9200'
# For cloud instances, you may need to use cloud_id and basic_auth
ES_URL = 'http://localhost:9200'

# Instantiate the client
try:
    # Note: For many modern ES instances, especially cloud or local default setups,
    # you might need basic authentication (username/password).
    # es = Elasticsearch(ES_URL, basic_auth=('elastic', 'your_password'))

    es = Elasticsearch(ES_URL)

except Exception as e:
    print(f"Error creating Elasticsearch client: {e}")
    exit()

# Check the connection using ping()
if es.ping():
    print("Connected to Elasticsearch successfully!")
else:
    # If ping returns False, a connection failure occurred or the server is down
    print("Failed to connect to Elasticsearch.")
    raise ValueError("Connection to Elasticsearch failed")

'''
# It does not work on the spark cluster but it just works with an application
[spark@localhost spark]$ ./sparkSubmit.sh start
Starting SparkSubmit Job
Connected to Elasticsearch successfully!
25/12/19 15:37:55 INFO ShutdownHookManager: Shutdown hook called
25/12/19 15:37:55 INFO ShutdownHookManager: Deleting directory /tmp/spark-63a8226d-b74d-46f2-99cf-c2b4f3235d8c
'''