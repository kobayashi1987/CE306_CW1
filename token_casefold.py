import json
from elasticsearch import Elasticsearch

# -------------------------------------------
# Step 1: Connect to Elasticsearch using HTTPS
# -------------------------------------------
es = Elasticsearch(
    "https://localhost:9200",
    basic_auth=("elastic", "password"),
    verify_certs=False
)

if not es.ping():
    raise ValueError("Connection failed: Ensure Elasticsearch is running on https://localhost:9200")
else:
    print("Connected to Elasticsearch.")

# -------------------------------------------
# Step 2: Create an index with a custom analyzer for tokenization and case folding
# -------------------------------------------
# We define an analyzer that uses:
#   - "standard" tokenizer: splits text based on language-independent rules.
#   - "lowercase" filter: performs case folding (converts tokens to lower case).
index_name = "articles_token"
settings = {
    "settings": {
        "analysis": {
            "analyzer": {
                "my_lowercase_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase"]
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "content": {
                "type": "text",
                "analyzer": "my_lowercase_analyzer"
            }
        }
    }
}

# Delete index if it already exists (for a clean start)
if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)
    print(f"Deleted existing index: {index_name}")

# Create the index with the custom settings
es.indices.create(index=index_name, body=settings)
print(f"Created index: {index_name} with custom analyzer 'my_lowercase_analyzer'.")

# -------------------------------------------
# Step 3: Test tokenization and case folding using the Analyze API
# -------------------------------------------
# Define a sample text to analyze.
sample_text = "The Quick Brown Fox Jumps Over The Lazy Dog."

analyze_body = {
    "analyzer": "my_lowercase_analyzer",
    "text": sample_text
}

# Call the analyze API on the new index
response = es.indices.analyze(index=index_name, body=analyze_body)
print("\nAnalyzed tokens:")
for token in response["tokens"]:
    print(token["token"])

# Optionally, print the full response (uncomment if needed)
# print(json.dumps(response, indent=2))