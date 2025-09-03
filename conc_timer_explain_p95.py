import os
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import time
import numpy as np

from dotenv import load_dotenv

load_dotenv() # take environment variables from .env.

# Connect to MongoDB
uri = os.getenv("MONGO_URI")
client = MongoClient(uri)
db = client["sample_airbnb"]  # Replace with your database
collection = db["listingsAndReviews"]  # Replace with your collection

# Generate 100/1000/10000/100000 randomized queries
def generate_queries():
    queries = []
    for _ in range(100000):
        query_type = random.choice(["price", "bedrooms", "amenities"])
        if query_type == "price":
            min_price = random.randint(50, 200)
            max_price = min_price + random.randint(50, 150)
            queries.append({"price": {"$gte": min_price, "$lte": max_price}})
        elif query_type == "bedrooms":
            queries.append({"bedrooms": random.randint(1, 5)})
        elif query_type == "amenities":
            amenity = random.choice(["Wifi", "Kitchen", "Heating", "Air conditioning", "Washer"])
            queries.append({"amenities": {"$in": [amenity]}})
    return queries

queries = generate_queries()

# Function to execute a query with explain() and measure its timing
def execute_query_with_explain(query):
    # Start wall clock timer for end-to-end execution timing
    start_time = time.perf_counter()

    # Run the query with explain() in "executionStats" mode
    explain_result = collection.find(query).explain()

    # End wall clock timer
    end_time = time.perf_counter()
    elapsed_time_wall_clock = end_time - start_time

    # Extract execution time from MongoDB explain results
    execution_time_mongo = explain_result["executionStats"]["executionTimeMillis"]

    # Count the number of documents returned
    documents_returned = explain_result["executionStats"]["nReturned"]

    return {
        "query": query,
        "documents_found": documents_returned,
        "time_taken_wall_clock": elapsed_time_wall_clock,
        "time_taken_mongo_execution": execution_time_mongo / 1000  # Convert ms to seconds
    }

# Run queries concurrently and collect results
results = []
with ThreadPoolExecutor(max_workers=1000) as executor:
    futures = [executor.submit(execute_query_with_explain, q) for q in queries]
    for future in as_completed(futures):
        results.append(future.result())

# Calculate average and p95 latency
mongo_execution_times = [result["time_taken_mongo_execution"] for result in results]

average_latency = np.mean(mongo_execution_times)
max_latency = np.max(mongo_execution_times)
p90_latency = np.percentile(mongo_execution_times, 90)
p95_latency = np.percentile(mongo_execution_times, 95)

# Output results
for result in results:
    print(f"Query: {result['query']}")
    print(f"Documents Found: {result['documents_found']}")
    print(f"Wall Clock Time Taken: {result['time_taken_wall_clock']:.6f} seconds")
    print(f"MongoDB Execution Time: {result['time_taken_mongo_execution']:.6f} seconds\n")

print("--- Summary ---")
print(f"Average MongoDB Execution Time: {average_latency:.6f} seconds")
print(f"Max MongoDB Execution Time: {max_latency:.6f} seconds")
print(f"P90 MongoDB Execution Time: {p90_latency:.6f} seconds")
print(f"P95 MongoDB Execution Time: {p95_latency:.6f} seconds")
