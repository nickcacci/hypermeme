import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import streamlit as st
import torch
from elasticsearch import Elasticsearch


from meme_analysis_pipeline.components.local_components import (
    ClipEmbedder,
)


# Connessione a Elasticsearch
@st.cache_resource
def get_es_connection():
    return Elasticsearch("http://localhost:9200")


es = get_es_connection()
# print es version and es client version
print(es.info())


# Carica il modello di embedding
@st.cache_resource
def get_embedding_model():
    return ClipEmbedder()


embedder = get_embedding_model()


def get_text_embedding(query: str):
    # Calcola l'embedding testuale
    with torch.no_grad():
        embedding = embedder.calculate_text_embedding(query)
    return embedding.squeeze(0).tolist()


def keyword_search(post_text, visual_description, image_text, explainer, k=10):
    # Esegue una ricerca per keyword sui campi testuali
    es_query = {
        "query": {
            "bool": {
                "should": [
                    {"match": {"post_text": post_text}},
                    {"match": {"visual_description": visual_description}},
                    {"match": {"image_text": image_text}},
                    {"match": {"explainer": explainer}},
                ],
                "minimum_should_match": 1,
            }
        },
        "size": k,
    }
    return es.search(index="memes", body=es_query)


def all_keyword_search(query_text, k=10):
    # Esegue una ricerca per keyword sui campi testuali
    es_query = {
        "query": {
            "bool": {
                "should": [
                    {"match": {"post_text": query_text}},
                    {"match": {"visual_description": query_text}},
                    {"match": {"image_text": query_text}},
                    {"match": {"explainer": query_text}},
                ],
                "minimum_should_match": 1,
            }
        },
        "size": k,
    }
    return es.search(index="memes", body=es_query)


#  def semantic_search_rrf(query_embedding, k=10):
#     # Costruisce la query Elasticsearch usando rrf per combinare i risultati
#     resp = es.search(
#         index="example-index",
#         body={
#             "retriever": {
#                 "rrf": {
#                     "retrievers": [
#                         {
#                             "knn": {
#                                 "field": "text_embedding",
#                                 "query_vector": [1.25, 2, 3.5],
#                                 "k": 50,
#                                 "num_candidates": 100,
#                             }
#                         },
#                         {
#                             "knn": {
#                                 "field": "img_embedding",
#                                 "query_vector": [1.25, 2, 3.5],
#                                 "k": 50,
#                                 "num_candidates": 100,
#                             }
#                         },
#                     ],
#                     "rank_window_size": 50,
#                     "rank_constant": 20,
#                 }
#             }
#         },
#     )
#     return resp


def semantic_search(query_embedding, field="text_embedding", k=10):
    # Esegue una ricerca semantica su un singolo campo specificato.
    es_query = {
        "knn": {
            "field": field,
            "query_vector": query_embedding,
            "k": k,
        }
    }
    return es.search(index="memes", body=es_query)


def display_results(results):
    displayed_urls = set()
    cols = st.columns(5, vertical_alignment="bottom")
    col_index = 0  # Initialize column index

    for i, hit in enumerate(results["hits"]["hits"], start=1):
        if hit["_source"]["remote_url"] not in displayed_urls:
            with cols[col_index]:

                st.image(
                    hit["_source"]["remote_url"],
                    caption=f"#{i} \nScore: {hit['_score']}",
                )
                with st.popover("More details"):
                    st.markdown(f"**Local URL:** {hit['_source']['local_url']}")
                    st.markdown(f"**Remote URL:** {hit['_source']['remote_url']}")
                    st.markdown(f"**Tags:** {hit['_source']['tags']}")

            displayed_urls.add(hit["_source"]["remote_url"])

            # col_index = (col_index + 1) % 5  # Move to the next column, reset after 5
            col_index = col_index + 1
            if col_index == 5:
                break


st.title("🔎 Meme Search")

# Form per ricerca per keyword
st.write("### Ricerca per Keyword")

with st.form(key="all_keyword_search_form"):
    query_text = st.text_input("Cerca su tutti i campi")
    all_keyword_submit = st.form_submit_button("Cerca per Keyword")

if all_keyword_submit:
    if query_text:
        response = all_keyword_search(query_text)
        st.write("Risultati ricerca per keyword:")
        display_results(response)
    else:
        st.error("Inserisci almeno una parola chiave.")


with st.form(key="keyword_search_form"):
    post_text = st.text_input("Titolo del post")
    visual_description = st.text_input("Visual Description")
    image_text = st.text_input("Image Text")
    explainer = st.text_input("Explainer")
    keyword_submit = st.form_submit_button("Cerca per Keyword")

if keyword_submit:
    query_text = " ".join(
        [visual_description, image_text, explainer, post_text]
    ).strip()
    if query_text:
        response = keyword_search(post_text, visual_description, image_text, explainer)
        st.write("Risultati ricerca per keyword:")
        display_results(response)
    else:
        st.error("Inserisci almeno una parola chiave.")

# Form per ricerca semantica
st.write("### Ricerca Semantica")
with st.form(key="semantic_search_form"):
    semantic_query = st.text_input("Query per ricerca semantica")
    semantic_submit = st.form_submit_button("Cerca")

if semantic_submit:
    if semantic_query:
        query_embedding = get_text_embedding(semantic_query)
        st.write("#### Risultati cercando tra i text_embeddings:")
        response = semantic_search(query_embedding, field="text_embedding")
        display_results(response)
        st.write("#### Risultati cercando tra gli img_embeddings:")
        response1 = semantic_search(query_embedding, field="img_embedding")

        display_results(response1)
    else:
        st.error("Inserisci il testo per la ricerca semantica.")
