import pandas as pd
from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import BulkIndexError

def connect_es():
    """
    Connects to Elasticsearch running on HTTPS and returns the client.
    """
    es = Elasticsearch(
        "https://localhost:9200",
        basic_auth=("elastic", "password"),
        verify_certs=False
    )
    if not es.ping():
        raise ValueError("Connection failed: Ensure Elasticsearch is running on https://localhost:9200")
    print("Connected to Elasticsearch.")
    return es

def create_index(es, index_name, settings):
    """
    Creates an index with the given settings. If the index exists, it is deleted first.
    """
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        print(f"Deleted existing index: {index_name}")
    es.indices.create(index=index_name, body=settings)
    print(f"Created index: {index_name}")

def index_documents(es, index_name, csv_file):
    """
    Reads a CSV file, selects the first 1000 documents, and bulk indexes them into the specified index.
    To avoid mapping conflicts, this function extracts (or creates) a single "content" field.
    """
    df = pd.read_csv(csv_file)
    df = df.head(1000)
    # Replace NaN with None so that JSON serialization works correctly.
    df = df.where(pd.notnull(df), None)
    actions = []
    for i, row in df.iterrows():
        # If the CSV already has a "content" column, use it.
        # Otherwise, combine all column values into one string.
        if "content" in row:
            content_value = row["content"]
        else:
            content_value = " ".join(str(val) for val in row.values if val is not None)
        doc = {"content": content_value}
        action = {
            "_index": index_name,
            "_id": i,
            "_source": doc
        }
        actions.append(action)
    
    try:
        helpers.bulk(es, actions)
        print(f"Indexed {len(actions)} documents into index '{index_name}'.")
    except BulkIndexError as bulk_error:
        error_details = bulk_error.errors
        print(f"BulkIndexError: {len(error_details)} document(s) failed to index.")
        for error in error_details:
            print(error)
        raise

def analyze_text(es, index_name, analyzer, text):
    """
    Uses the Analyze API to process a sample text with a specified analyzer.
    """
    body = {
        "analyzer": analyzer,
        "text": text
    }
    response = es.indices.analyze(index=index_name, body=body)
    tokens = [token["token"] for token in response["tokens"]]
    print(f"\nAnalyzed tokens using '{analyzer}':")
    print(tokens)
    return tokens

def search_documents(es, index_name, query, description="Query"):
    """
    Executes a search query and prints the total hit count and each hit’s score and content.
    """
    print(f"\nExecuting {description}:")
    res = es.search(index=index_name, body=query)
    total = res["hits"]["total"]["value"]
    print("Total hits:", total)
    for hit in res["hits"]["hits"]:
        content = hit["_source"].get("content", "No content field")
        print("Score:", hit["_score"], "Content:", content)
    print("-" * 50)
    return res

def main():
    # Step 1: Connect to Elasticsearch using the API.
    es = connect_es()
    
    # Step 2: Create an index with a custom analyzer that performs:
    # - Standard tokenization
    # - Lowercase (case folding)
    # - Stopword removal
    # - Porter stemming (for morphological analysis)
    index_name = "articles_pipeline"
    settings = {
        "settings": {
            "analysis": {
                "analyzer": {
                    "custom_analyzer": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": ["lowercase", "stop", "porter_stem"]
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "content": {
                    "type": "text",
                    "analyzer": "custom_analyzer"
                }
            }
        }
    }
    create_index(es, index_name, settings)
    
    # Step 3: Index the first 1000 documents from your dataset (sample_data.csv).
    csv_file = "sample_data.csv"  # Ensure this file exists in your working directory.
    index_documents(es, index_name, csv_file)
    
    # Step 4: Demonstrate text analysis (tokenization, case folding, stopword removal, and stemming)
    sample_text = "The buses were running while the bus driver drove the busses."
    analyze_text(es, index_name, "custom_analyzer", sample_text)
    
    # Step 5: Execute three example search queries using the API.
    
    # Query 1: A simple match query searching for "Singapore".
    query1 = {
        "query": {
            "match": {
                "content": "Singapore"
            }
        }
    }
    search_documents(es, index_name, query1, "Query 1: Match 'Singapore'")
    
    # Query 2: Boolean query requiring both "Indonesian" and "tourist".
    query2 = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"content": "Indonesian"}},
                    {"match": {"content": "tourist"}}
                ]
            }
        }
    }
    search_documents(es, index_name, query2, "Query 2: Boolean 'Indonesian' AND 'tourist'")
    
    # Query 3: Phrase query searching for the exact phrase "Indonesian tourist".
    query3 = {
        "query": {
            "match_phrase": {
                "content": "Indonesian tourist"
            }
        }
    }
    search_documents(es, index_name, query3, "Query 3: Phrase 'Indonesian tourist'")

if __name__ == "__main__":
    main()