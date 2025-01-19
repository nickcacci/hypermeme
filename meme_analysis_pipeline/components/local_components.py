import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from io import BytesIO
import os
from tempfile import NamedTemporaryFile
from urllib.parse import urlparse
from ollama import chat
import requests
import meme_analysis_pipeline.components.interfaces as interfaces
import meme_analysis_pipeline.utils.models as models
import torch
from PIL import Image
import open_clip
from elasticsearch import Elasticsearch
import shutil
from urllib.parse import urlparse
from tempfile import NamedTemporaryFile
import logging


class LocalLlmDescriber(interfaces.MemeDescriber):
    def __init__(self):
        self.MODEL_NAME = "llava-phi3"

    def describe(self, image_path: str) -> dict:
        response = chat(
            model=self.MODEL_NAME,
            format=models.Meme.model_json_schema(),  # Pass in the schema for the response
            messages=[
                {
                    "role": "user",
                    "content": "Analyze this image and return a detailed JSON description. If you cannot determine certain details, leave those fields empty.",
                    "images": [image_path],
                },
            ],
            options={
                "temperature": 0
            },  # Set temperature to 0 for more deterministic output
        )
        image_analysis = models.Meme.model_validate_json(response.message.content)
        return image_analysis


class ClipEmbedder(interfaces.EmbeddingCalculator):
    def __init__(self):
        self.model, _, self.preprocess = open_clip.create_model_and_transforms(
            "ViT-B-32", pretrained="laion2b_s34b_b79k"
        )
        self.model.eval()  # model in train mode by default, impacts some models with BatchNorm or stochastic depth active
        self.tokenizer = open_clip.get_tokenizer("ViT-B-32")

    def calculate_image_embedding(self, image_path):
        #convert the image to rgb
        image= Image.open(image_path).convert("RGB")
        image = self.preprocess(image).unsqueeze(0)
        image_features = self.model.encode_image(image)
        return image_features

    def calculate_text_embedding(self, text):
        text = self.tokenizer(text)
        return self.model.encode_text(text)


class ElasticSearchManager(interfaces.DatabaseManager):
    def __init__(self):
        self.es = Elasticsearch(["http://localhost:9200"])
        self.index_name = "memes"

    def save_data(self, data: models.EnrichedMeme):
        document = {
            "post_text": data.post_text,
            "image_text": data.enrichment.image_text,
            "visual_description": data.enrichment.visual_description,
            "explainer": data.enrichment.explainer,
            "remote_url": data.img_url,
            "local_url": data.local_url,
            "tags": data.enrichment.tags,
            "text_embedding": (data.text_embedding.squeeze(0).tolist()),
            "img_embedding": (data.image_embedding.squeeze(0).tolist()),
        }
        try:
            insert_response = self.es.index(index=self.index_name, body=document)
            logging.debug(
                f"Documento {document} inserito correttamente. Risposta: {insert_response}"
            )
        except Exception as e:
            print(f"Errore durante l'inserimento del documento: {e}")


class DummyDBManager(interfaces.DatabaseManager):
    def __init__(self):
        print("INIT DUMMY DB MANAGER")

    def save_data(self, data: models.EnrichedMeme):
        print(f"Documento {data} ricevuto dal dummy db.")


class TmpImgDownloader(interfaces.ImageDownloader):
    # download the image in a tmp file, returns the url
    def is_remote_url(self, url: str) -> bool:
        parsed_url = urlparse(url)
        return parsed_url.scheme in ["http", "https"]

    def save_img(self, img_url: str) -> str:
        if self.is_remote_url(img_url):
            response = requests.get(img_url)
            img = NamedTemporaryFile(delete=False, suffix=".jpg")
            img.write(response.content)
            img.close()
            return img.name
        else:
            if os.path.exists(img_url):
                img = NamedTemporaryFile(delete=False, suffix=".jpg")
                shutil.copy(img_url, img.name)
                return img.name
            else:
                raise FileNotFoundError(f"Il file locale {img_url} non esiste.")


class LocalImgDownloader(interfaces.ImageDownloader):
    def __init__(self, folder_path: str = "./memes_dataset/images"):
        self.folder_path = folder_path
        os.makedirs(self.folder_path, exist_ok=True)

    def is_remote_url(self, url: str) -> bool:
        parsed_url = urlparse(url)
        return parsed_url.scheme in ["http", "https"]

    def save_img(self, img_url: str) -> str:
        if self.is_remote_url(img_url):
            response = requests.get(img_url)
            img = Image.open(BytesIO(response.content))
            img_path = os.path.join(self.folder_path, os.path.basename(img_url))
            img.save(img_path)
            return img_path
        else:
            if os.path.exists(img_url):
                img_path = os.path.join(self.folder_path, os.path.basename(img_url))
                shutil.copy(img_url, img_path)
                return img_path
            else:
                raise FileNotFoundError(f"Il file locale {img_url} non esiste.")
