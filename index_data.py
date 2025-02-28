import pandas as pd
from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import BulkIndexError

# -------------------------------------------
# Step 1: Load the dataset (first 1000 documents)
# -------------------------------------------
df = pd.read_csv("sample_data.csv")
# Convert any NaN values to None (helps JSON serialization)
df = df.where(pd.notnull(df), None)
# Select the first 1000 rows (in case file has more)
df = df.head(1000)
print("Number of documents in sample:", len(df))

# -------------------------------------------
# Step 2: Connect to Elasticsearch using HTTPS
# -------------------------------------------
# Replace the credentials with your actual username and password.
es = Elasticsearch(
    "https://localhost:9200",
    basic_auth=("elastic", "U25pq33cD8FqObxn9UOT"),
    verify_certs=False
)

# Test connection
if not es.ping():
    raise ValueError("Connection failed: Ensure Elasticsearch is running on https://localhost:9200")
else:
    print("Connected to Elasticsearch.")

# -------------------------------------------
# Step 3: Create an index (delete if exists for a clean start)
# -------------------------------------------
index_name = "articles"  # Name of the index

if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)
    print(f"Deleted existing index: {index_name}")

# Create the index with default settings (or add custom mapping if needed)
es.indices.create(index=index_name)
print(f"Created index: {index_name}")

# -------------------------------------------
# Step 4: Prepare documents for bulk indexing
# -------------------------------------------
actions = []
for i, row in df.iterrows():
    doc = row.to_dict()
    action = {
        "_index": index_name,
        "_id": i,  # Alternatively, use a unique field from your document
        "_source": doc
    }
    actions.append(action)

# -------------------------------------------
# Step 5: Bulk index the documents with error debugging
# -------------------------------------------
try:
    helpers.bulk(es, actions)
    print(f"Indexed {len(actions)} documents into index '{index_name}'.")
except BulkIndexError as bulk_error:
    # bulk_error.errors is a list of error details for the documents that failed
    error_details = bulk_error.errors
    print(f"BulkIndexError: {len(error_details)} document(s) failed to index.")
    for error in error_details:
        print(error)