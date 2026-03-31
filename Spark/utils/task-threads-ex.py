from pyspark.sql import SparkSession
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# SparkSession 초기화
spark = SparkSession.builder \
    .appName("SparkThreadNum") \
    .getOrCreate()

def task(n):
    return (n, n * 2)

'''
with ThreadPoolExecutor(max_workers=3) as executor:
    future = executor.submit(task, 5)
    print(future.result())  # 10
'''

'''
with ThreadPoolExecutor(max_workers=3) as executor:
    results = executor.map(square, [1, 2, 3, 4, 5])
    print(list(results))  # [1, 4, 9, 16, 25]
'''

with ThreadPoolExecutor(max_workers=2) as executor:
    # future = executor.submit(task, 5)
    future = {executor.submit(task, num): num for num in [10,20,30]}
    results = []
    for future in as_completed(future):
        results.append(future.result())

print("\n--- 모든 작업 완료 ---")
for path, count in results:
    print(f"파일: {path}, 레코드 수: {count}")

spark.stop()