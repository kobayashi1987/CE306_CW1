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
# Step 2: Create an index with a custom analyzer for keyword selection
# -------------------------------------------
# The custom analyzer "keyword_selector" is defined to:
# - Use the standard tokenizer
# - Apply a lowercase filter (case folding)
# - Apply a stop filter (remove common English stopwords)
# - Apply a shingle filter to generate bigrams and trigrams (while outputting unigrams as well)
# We remove the similarity configuration so the default BM25 (a TF-IDF variant) is used.
index_name = "articles_keywords"
settings = {
    "settings": {
        "analysis": {
            "analyzer": {
                "keyword_selector": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "stop",       # removes common English stopwords
                        "my_shingle"  # custom shingle filter for n-grams
                    ]
                }
            },
            "filter": {
                "my_shingle": {
                    "type": "shingle",
                    "min_shingle_size": 2,
                    "max_shingle_size": 3,
                    "output_unigrams": True  # include original tokens too
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "content": {
                "type": "text",
                "analyzer": "keyword_selector"
            }
        }
    }
}

# Delete the index if it exists for a clean start
if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)
    print(f"Deleted existing index: {index_name}")

# Create the new index with the custom settings
es.indices.create(index=index_name, body=settings)
print(f"Created index: {index_name} with custom analyzer 'keyword_selector'.")

# -------------------------------------------
# Step 3: Test the custom analyzer using the Analyze API
# -------------------------------------------
sample_text = (
    "<p><b>Gerard Salton</b> (8 March 1927 in <a href=\"/wiki/Nuremberg\" title=\"Nuremberg\">Nuremberg</a> - 28 August 1995), "
    "also known as Gerry Salton, was a Professor of <a href=\"/wiki/Computer_Science\" title=\"Computer Science\" class=\"mw-redirect\">"
    "Computer Science</a> at <a href=\"/wiki/Cornell_University\" title=\"Cornell University\">Cornell University</a>. "
    "Salton was perhaps the leading computer scientist working in the field of <a href=\"/wiki/Information_retrieval\" "
    "title=\"Information retrieval\">information retrieval</a> during his time."
)

analyze_body = {
    "analyzer": "keyword_selector",
    "text": sample_text
}

response = es.indices.analyze(index=index_name, body=analyze_body)
print("\nAnalyzed tokens:")
for token in response["tokens"]:
    print(token["token"])