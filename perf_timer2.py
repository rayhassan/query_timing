import time
from pymongo import MongoClient

# Connect to MongoDB
uri = "mongodb+srv://rayh:mongodb4u@sandbox.fgkzb.mongodb.net/?retryWrites=true&w=majority&appName=sandbox"
client = MongoClient(uri)
db = client["sample_airbnb"]  # Example database
collection = db["listingsAndReviews"]  # Example collection

# Define the query
query = {"price": {"$gte": 100, "$lte": 300}}  # Example query
agg_pipeline=[
    {
        '$group': {
            '_id': '$address.country', 
            'property_count': {
                '$sum': 1
            }
        }
    }, {
        '$sort': {
            'property_count': -1
        }
    }
]
projection = {"_id": 0, "name": 1, "price": 1}

# High-resolution timer: Start
start_time = time.perf_counter()  # More precise than time.time()

# Execute the query
# Use the explain() method with executionStats mode
explain_result = collection.find(query, projection).explain()

# Extract and display the execution time
execution_time = explain_result["executionStats"]["executionTimeMillis"]
print(f"Query execution time (as reported by MongoDB): {execution_time} ms")

#results = list(collection.aggregate(agg_pipeline))

# High-resolution timer: End
end_time = time.perf_counter()

# Calculate the elapsed time in seconds
elapsed_time = end_time - start_time

# Print the results
print(f"Query executed in {elapsed_time:.6f} seconds.")
