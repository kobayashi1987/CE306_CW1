# from elasticsearch import Elasticsearch

# # -------------------------------------------
# # Step 1: Connect to Elasticsearch using HTTPS
# # -------------------------------------------
# es = Elasticsearch(
#     "https://localhost:9200",
#     basic_auth=("elastic", "password"),
#     verify_certs=False
# )

# if not es.ping():
#     raise ValueError("Connection failed: Ensure Elasticsearch is running on https://localhost:9200")
# else:
#     print("Connected to Elasticsearch.")

# # -------------------------------------------
# # Step 2: Specify the index name
# # -------------------------------------------
# # This should be the index where your 1000 documents were indexed.
# index_name = "articles"

# # -------------------------------------------
# # Step 3: Define three example queries
# # -------------------------------------------

# # Query 1: Simple match query for "Singapore tourism 2014"
# query1 = {
#     "query": {
#         "match": {
#             "content": "Singapore"
#         }
#     }
# }

# # Query 2: Boolean query (documents must contain both "Indonesian" and "budget")
# query2 = {
#     "query": {
#         "bool": {
#             "must": [
#                 {"match": {"content": "Indonesian"}},
#                 {"match": {"content": "budget"}}
#             ]
#         }
#     }
# }

# # Query 3: Match phrase query for the exact phrase "cultural attractions Singapore"
# query3 = {
#     "query": {
#         "match_phrase": {
#             "content": "cultural attractions Singapore"
#         }
#     }
# }

# # -------------------------------------------
# # Step 4: Execute each query and print the results
# # -------------------------------------------

# def execute_query(query, description):
#     print(f"\nExecuting Query: {description}")
#     res = es.search(index=index_name, body=query)
#     total = res["hits"]["total"]["value"]
#     print("Total hits:", total)
#     for hit in res["hits"]["hits"]:
#         # Adjust the field name if needed; here we assume 'content' holds the text.
#         content = hit["_source"].get("content", "No content field")
#         print("Score:", hit["_score"], "Content:", content)
#     print("-" * 50)

# # Run Query 1
# execute_query(query1, "Match query for 'Singapore tourism 2014'")

# # Run Query 2
# execute_query(query2, "Boolean query for 'Indonesian' AND 'budget'")

# # Run Query 3
# execute_query(query3, "Phrase query for 'cultural attractions Singapore'")



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
# Step 2: Specify the index name
# -------------------------------------------
# This should be the index where your 1000 documents were indexed.
index_name = "articles"

# -------------------------------------------
# Step 3: Define three example queries that should produce positive hits
# -------------------------------------------

# Query 1: Simple match query for "Singapore"
query1 = {
    "query": {
        "match": {
            "content": "Singapore"
        }
    }
}

# Query 2: Boolean query requiring both "Indonesian" and "tourist"
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

# Query 3: Exact phrase query for "Indonesian tourist"
query3 = {
    "query": {
        "match_phrase": {
            "content": "Indonesian tourist"
        }
    }
}

# -------------------------------------------
# Step 4: Execute each query and print the results
# -------------------------------------------

def execute_query(query, description):
    print(f"\nExecuting Query: {description}")
    res = es.search(index=index_name, body=query)
    total = res["hits"]["total"]["value"]
    print("Total hits:", total)
    for hit in res["hits"]["hits"]:
        content = hit["_source"].get("content", "No content field")
        print("Score:", hit["_score"], "Content:", content)
    print("-" * 50)

# Run Query 1
execute_query(query1, "Match query for 'Singapore'")

# Run Query 2
execute_query(query2, "Boolean query for 'Indonesian' AND 'tourist'")

# Run Query 3
execute_query(query3, "Phrase query for 'Indonesian tourist'")

