from elasticsearch import Elasticsearch

es = Elasticsearch(["http://elasticsearch:9200"])
index_name = "memes"

mapping = {
    "mappings": {
        "properties": {
            # The text of the post (every social media allows users to add text even in image posts)
            " post_text": {"type": "text"},
            "image_text": {"type": "text"},
            "visual_description": {"type": "text"},
            "explainer": {"type": "text"},
            "remote_url": {"type": "keyword"},
            "local_url": {"type": "keyword"},
            # In Elasticsearch, there is no dedicated array data type. Any field can contain zero or more values by default, however, all values in the array must be of the same data type.
            "tags": {"type": "keyword"},
            # "text_embedding": {"type": "dense_vector", "dims": 1536},
            # If dims is not specified, it will be set to the length of the first vector added to the field.
            "text_embedding": {"type": "dense_vector"},
            "img_embedding": {"type": "dense_vector"},
            # TODO: Add template information
        }
    }
}


if es.indices.exists(index=index_name):
    print(f"L'indice '{index_name}' esiste già. Lo sto cancellando...")
    es.indices.delete(index=index_name)


response = es.indices.create(index=index_name, body=mapping)

print(f"Indice '{index_name}' creato con successo!")

""" # Esempio di documento da inserire nell'indice
doc = {
    "id": "1",
    "created_timestamp": "2023-09-18T12:34:56.789Z",
    "title": "Example Title for Fielddata",
    "selftext": "This text will be analyzed and tokenized for fielddata operations.",
    "caption_text": "A sample caption for fielddata example.",
    "ocr_text": "Recognized text from an image using OCR.",
    "score": 42,
    "upvote_ratio": 0.85,
    "subreddit": "example_subreddit",
    "img_url": "http://example.com/image.jpg",
    "img_filename": "image.jpg",
    "num_comments": 10,
    "predicted_category": "news",
    "ground_truth_category": "news",
}

# Inserire il documento nell'indice
insert_response = es.index(index=index_name, id=doc["id"], body=doc)
print(f"Documento con id {doc['id']} inserito correttamente.") """
