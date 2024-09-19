from elasticsearch import Elasticsearch

es = Elasticsearch(["http://elasticsearch:9200"])
index_name = "reddit-posts"

mapping = {
    "settings": {
        "analysis": {
            "analyzer": {
                "custom_analyzer": {
                    "type": "standard",
                    "stopwords": "_english_",  # Rimuove le stopwords in inglese
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "id": {"type": "keyword"},
            "created_timestamp": {
                "type": "date",
                "format": "yyyy-MM-dd'T'HH:mm:ss.SSSZ||epoch_millis",
            },
            "title": {
                "type": "text",
                "fielddata": True,
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256,  # Limita la lunghezza massima del campo indicizzato
                    }
                },
                "analyzer": "standard",
            },
            "selftext": {
                "type": "text",
                "fielddata": True,
                "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                "analyzer": "custom_analyzer",
            },
            "caption_text": {
                "type": "text",
                "fielddata": True,
                "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                "analyzer": "custom_analyzer",
            },
            "ocr_text": {
                "type": "text",
                "fielddata": True,
                "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                "analyzer": "custom_analyzer",
            },
            "score": {"type": "integer"},
            "upvote_ratio": {"type": "float"},
            "subreddit": {"type": "keyword"},
            "img_url": {"type": "keyword"},
            "img_filename": {"type": "keyword"},
            "num_comments": {"type": "integer"},
            "predicted_category": {"type": "keyword"},
            "ground_truth_category": {"type": "keyword"},
            "all_text": {
                "type": "text",
                "fielddata": True,
                "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                "analyzer": "custom_analyzer",
            },
        }
    },
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
