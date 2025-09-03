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

# Function to execute a query with explain() and measure its timing
def execute_query_with_explain(query):
    # Start wall clock timer for end-to-end execution timing
    start_time = time.perf_counter()

    # Run the query with explain() in "executionStats" mode
    #explain_result = collection.find(query).explain("executionStats")
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

# Run queries concurrently
with ThreadPoolExecutor(max_workers=3) as executor:
    futures = [executor.submit(execute_query_with_explain, q) for q in queries]
    for future in as_completed(futures):
        result = future.result()
        print(f"Query: {result['query']}")
        print(f"Documents Found: {result['documents_found']}")
        print(f"Wall Clock Time Taken: {result['time_taken_wall_clock']:.6f} seconds")
        print(f"MongoDB Execution Time: {result['time_taken_mongo_execution']:.6f} seconds\n")

