import shutil
import sys
import os
import tempfile
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import streamlit as st
from PIL import Image
from elasticsearch import Elasticsearch
from meme_analysis_pipeline.components.local_components import ClipEmbedder

logging.basicConfig(level=logging.INFO)

@st.cache_resource
def get_embedding_model():
    logging.info("Loading embedding model...")
    embedder = ClipEmbedder()
    return embedder


embedder = get_embedding_model()


@st.cache_resource
def get_connection():
    logging.info("Connecting to Elasticsearch...")
    es = Elasticsearch("http://localhost:9200")
    return es


es = get_connection()


def get_image_embedding(image_path):
    logging.info(f"Calculating image embedding for {image_path}...")
    embedding = embedder.calculate_image_embedding(image_path)
    return embedding


def search_image_embedding(image_path):
    logging.info(f"Searching for image embedding for {image_path}...")
    embedding = get_image_embedding(image_path)
    #log type of embedding
    logging.debug(f"Embedding type: {type(embedding.tolist())}")

    query = {
            "knn": {
                "field": "img_embedding",
                "query_vector": embedding.squeeze(0).tolist(),
                "k": 10,
            }
        }

    response = es.search(index="memes", body=query)
    logging.info("Search completed.")
    return response


def copy_to_tempfile(src_path):
    logging.info(f"Copying uploaded file to temporary file...")
    temp_file = tempfile.NamedTemporaryFile(delete=False)

    with open(temp_file.name, "wb") as f:
        f.write(uploaded_file.getbuffer())

    return temp_file.name


st.title("🖼️ Search By Image")

st.write("Upload an image and the app will search for similar images.")

uploaded_file = st.file_uploader("Choose an image...", type=["jpg", "jpeg", "png"])

if uploaded_file is not None:
    uploaded_file_path = copy_to_tempfile(uploaded_file)
    
    results = search_image_embedding(uploaded_file_path)
    logging.info("Got %d Hits:" % results['hits']['total']['value'])
    
    displayed_urls = set()
    cols = st.columns(5)  # Create 5 columns for the grid
    col_index = 0  # Initialize column index
    
    for i, hit in enumerate(results['hits']['hits'], start=1):
        if hit["_source"]["remote_url"] not in displayed_urls:
            with cols[col_index]:
                st.image(hit["_source"]["remote_url"], use_container_width=True, caption=f"#{i} \nScore: {hit['_score']}")
                
                with st.expander("More details"):
                    st.markdown(f"**Local URL:** {hit['_source']['local_url']}")
                    st.markdown(f"**Remote URL:** {hit['_source']['remote_url']}")
                    st.markdown(f"**Tags:** {hit['_source']['tags']}")
                
                # Log the information
                logging.info(f"Hit {i}: Relevance Score: {hit['_score']}, Local URL: {hit['_source']['local_url']}, Remote URL: {hit['_source']['remote_url']}, Tags: {hit['_source']['tags']}")
                
                displayed_urls.add(hit["_source"]["remote_url"])
            
            col_index = (col_index + 1) % 5  # Move to the next column, reset after 5

