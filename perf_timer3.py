import os
import time
import pprint
from pymongoexplain import ExplainableCollection
from pymongo import MongoClient

from dotenv import load_dotenv

load_dotenv() # take environment variables from .env.

# Connect to MongoDB
uri = os.getenv("MONGO_URI")
client = MongoClient(uri)
db = client["sample_airbnb"]  # Example database
collection_name = db["listingsAndReviews"]  # Example collection

# Define the qggregation query
agg_pipeline=[
   {
        "$search": {
            "index": "default",  # Specify the name of your search index
            "text": {
                "query": "spacious",  # Search keyword
                "path": "description"  # Field to search in
            }
        }
    },
    {
        "$project": {
            "name": 1,
            "description": 1,
            "score": {"$meta": "searchScore"}
        }
    },
    {
        "$limit": 10
    }
]


# High-resolution timer: Start
start_time = time.perf_counter()  # More precise than time.time()

# Execute the query
#explain_result = db.collection.explain("executionStats").aggregate(agg_pipeline)
#explain_result = db.collection.aggregate(agg_pipeline).explain("executionStats")

explain_command = {
        "aggregate": "listingsAndReviews",
        "pipeline": agg_pipeline,
        "explain": True,
    }

explain_result = db.command(explain_command)

#explain_result = db.command('aggregate', 'collection', pipeline=agg_pipeline, explain=True)
#explain_result = ExplainableCollection(collection).aggregate(agg_pipeline)
pprint.pprint(explain_result)

# High-resolution timer: End
end_time = time.perf_counter()

# Calculate the elapsed time in seconds
elapsed_time = end_time - start_time

# Print the results
#print(f"Query executed in {elapsed_time:.6f} seconds.")
