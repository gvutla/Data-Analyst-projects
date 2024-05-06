from pymongo import MongoClient
import time

def run_batch_queries(batch_size=2000):
    client = MongoClient('localhost', 27017)
    db = client['RealTimeDB']
    collection = db['RealTimeCollection']

    start_time = time.time()

    # Get the total count of documents in the collection
    total_count = collection.count_documents({})

    # Initialize batch start index
    batch_start = 0

    # Process batches until all documents are processed
    while batch_start < total_count:
        # Fetch documents for the current batch
        batch_documents = collection.find({}).skip(batch_start).limit(batch_size)

        # Calculate average temperature for the current batch
        sum_temp = 0
        num_docs = 0
        for doc in batch_documents:
            sum_temp += doc['WaterTemperature']
            num_docs += 1

        # Calculate average temperature for the batch
        avg_temp = sum_temp / num_docs if num_docs > 0 else 0

        # Print average temperature for the batch
        print(f"Batch {batch_start // batch_size + 1}: Processed {num_docs} records. Average Water Temperature: {avg_temp:.2f}")

        # Move to the next batch
        batch_start += batch_size

    end_time = time.time()
    print(f"Total Batch Query Execution Time: {end_time - start_time:.7f} seconds")

if __name__ == "__main__":
    run_batch_queries()
