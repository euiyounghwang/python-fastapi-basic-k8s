from pyspark.sql import SparkSession

# SparkSession 생성
spark = SparkSession.builder.appName("ForEachPartitionExample").getOrCreate()

# 예제 DataFrame 생성
data = [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "David")]
df = spark.createDataFrame(data, ["id", "name"])

# 각 파티션 처리 함수 정의 (예: 데이터베이스 연결/닫기, 벌크 저장)
def process_partition(partition_iterator):
    # 파티션별로 데이터베이스 연결 또는 파일 열기 (효율적)
    # print("새로운 파티션 처리 시작!")
    # conn = connect_to_database() # 실제 연결 로직
    
    for row in partition_iterator:
        # 파티션 내 각 행 처리 (예: 데이터베이스에 쓰기)
        print(f"Processing row: {row['id']}, {row['name']}")
        # conn.execute(f"INSERT INTO my_table VALUES ({row['id']}, '{row['name']}')")
    
    # conn.close() # 연결 닫기

# foreachPartition 호출
# repartitioned_df = df.repartition(10)
# print(df.rdd.getNumPartitions())
# repartitioned_df.foreachPartition(process_partition) 
# 실제 실행 시 주석 해제 후 사용

# 예제 실행을 위해 DataFrame을 RDD로 변환 후 mapPartitions 예시 (forEachPartition과 유사)
# foreachPartition은 DataFrame API의 일부지만 내부적으로 RDD API를 사용합니다.
df.rdd.mapPartitions(process_partition).collect() # collect는 예시를 위한 것으로, 실제는 액션 호출 필요 없음

spark.stop()