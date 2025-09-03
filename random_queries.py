import os
from dotenv import load_dotenv
from pymongo import MongoClient
import random

load_dotenv() # take environment variables from .env.

# Connect to MongoDB
uri = os.getenv("MONGO_URI")
client = MongoClient(uri)
db = client["sample_airbnb"]  # Replace with your database
collection = db["listingsAndReviews"]  # Replace with your collection

# Function to calculate the minimum document count for 10% coverage
def calculate_target_count():
    total_documents = collection.count_documents({})
    target_count = max(1, int(0.1 * total_documents))  # At least 10% of the dataset
    return target_count

target_count = calculate_target_count()
print(f"Target number of documents to cover: {target_count}")

# Generate random queries to cover at least 10% of the dataset
def generate_unique_queries(target_count):
    unique_queries = set()
    queries = []

    while len(queries) < target_count:
        query_type = random.choice(["price", "bedrooms", "amenities"])
        if query_type == "price":
            min_price = random.randint(50, 500)
            max_price = min_price + random.randint(50, 300)
            query = {"price": {"$gte": min_price, "$lte": max_price}}
        elif query_type == "bedrooms":
            query = {"bedrooms": random.randint(1, 5)}
        elif query_type == "amenities":
            amenity = random.choice(["Wifi", "Kitchen", "Heating", "Air conditioning", "Washer"])
            query = {"amenities": {"$in": [amenity]}}

        # Ensure the query is unique
        query_str = str(query)
        if query_str not in unique_queries:
            unique_queries.add(query_str)
            queries.append(query)

    return queries

unique_queries = generate_unique_queries(target_count)
print(f"Generated {len(unique_queries)} unique queries.")

# Output the generated queries
for i, query in enumerate(unique_queries):
    print(f"Query {i + 1}: {query}")

