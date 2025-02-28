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
# Step 2: Create an index with a custom stemming analyzer
# -------------------------------------------
# This analyzer, named "stem_analyzer", uses:
#   - "standard" tokenizer: splits text into tokens.
#   - "lowercase" filter: converts tokens to lower case.
#   - "porter_stem" filter: reduces tokens to their word stem.
index_name = "articles_stemming"
settings = {
    "settings": {
        "analysis": {
            "analyzer": {
                "stem_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "porter_stem"]
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "content": {
                "type": "text",
                "analyzer": "stem_analyzer"
            }
        }
    }
}

# Delete the index if it exists for a clean start
if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)
    print(f"Deleted existing index: {index_name}")

# Create the index with the custom stemming analyzer
es.indices.create(index=index_name, body=settings)
print(f"Created index: {index_name} with custom analyzer 'stem_analyzer'.")

# -------------------------------------------
# Step 3: Test the stemming analyzer using the Analyze API
# -------------------------------------------
# Sample text includes different inflections of "bus" and variations like "running"
sample_text = "The buses were running while the bus driver drove the busses."
analyze_body = {
    "analyzer": "stem_analyzer",
    "text": sample_text
}

response = es.indices.analyze(index=index_name, body=analyze_body)
print("\nAnalyzed tokens:")
for token in response["tokens"]:
    print(token["token"])

# Optionally, print the full response for detailed inspection:
# print(json.dumps(response, indent=2))