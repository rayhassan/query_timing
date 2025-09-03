import os
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

from dotenv import load_dotenv

load_dotenv() # take environment variables from .env.

# Connect to MongoDB
uri = os.getenv("MONGO_URI")
client = MongoClient(uri)
db = client["sample_airbnb"]  # Replace with your database
collection = db["listingsAndReviews"]  # Replace with your collection

# Define queries
queries = [
    {"price": {"$gte": 100, "$lte": 200}},  # Query 1
    {"bedrooms": 2},                       # Query 2
    {"amenities": {"$in": ["Wifi", "Kitchen"]}}  # Query 3
]

# Function to execute a query and measure its execution time
def execute_query(query):
    start_time = time.perf_counter()
    results = list(collection.find(query))  # Execute the query
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    return {"query": query, "result_count": len(results), "time_taken": elapsed_time}

# Run queries concurrently
with ThreadPoolExecutor(max_workers=3) as executor:
    futures = [executor.submit(execute_query, q) for q in queries]
    for future in as_completed(futures):
        result = future.result()
        print(f"Query: {result['query']}")
        print(f"Documents Found: {result['result_count']}")
        print(f"Time Taken: {result['time_taken']:.6f} seconds\n")

