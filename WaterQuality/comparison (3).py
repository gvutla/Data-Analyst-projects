
import time
from pymongo import MongoClient
from kafka import KafkaConsumer
import json

def get_streaming_data_from_kafka():
    consumer = KafkaConsumer('CleanSensorData', bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest', consumer_timeout_ms=10000, max_poll_records=100)
    total_temp = 0
    count = 0
    start_time = time.time()
    for message in consumer:
        data = json.loads(message.value.decode('utf-8'))
        if 'WaterTemperature' in data:
            total_temp += data['WaterTemperature']
            count += 1
        if count >= 100:  # Limit to 100 messages for faster processing
            break
    end_time = time.time()
    consumer.close()
    if count > 0:
        average_temp = total_temp / count
    else:
        average_temp = None
    return average_temp, end_time - start_time

def run_batch_query():
    client = MongoClient('localhost', 27017)
    db = client['RealTimeDB']
    collection = db['RealTimeCollection']
    start_time = time.time()
    result = collection.aggregate([
        {"$group": {"_id": None, "AverageTemperature": {"$avg": "$WaterTemperature"}}},
        {"$limit": 100}  # Limit the documents processed
    ])
    end_time = time.time()
    batch_time = end_time - start_time
    for data in result:
        return data['AverageTemperature'], batch_time
    return None, batch_time

def compare_performance(stream_temp, stream_time, batch_temp, batch_time):
    print(f"Streaming Temperature: {stream_temp}, Streaming Execution Time: {stream_time} seconds")
    print(f"Batch Temperature: {batch_temp}, Batch Execution Time: {batch_time} seconds")

def main():
    stream_temp, stream_time = get_streaming_data_from_kafka()
    batch_temp, batch_time = run_batch_query()
    compare_performance(stream_temp, stream_time, batch_temp, batch_time)

if __name__ == "__main__":
    main()