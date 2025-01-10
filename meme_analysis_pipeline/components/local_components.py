from ollama import chat
import components.interfaces as interfaces
import utils.models as models
import torch
from PIL import Image
import open_clip
from elasticsearch import Elasticsearch


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
        image = self.preprocess(Image.open(image_path)).unsqueeze(0)
        image_features = self.model.encode_image(image)
        return image_features

    def calculate_text_embedding(self, text):
        text = self.tokenizer(text)
        return self.model.encode_text(text)


class ElasticSearchManager(interfaces.DatabaseManager):
    def __init__(self):
        self.es = Elasticsearch(["http://elasticsearch:9200"])
        self.index_name = "memes"

    def save_data(self, data):
        insert_response = self.es.index(index=self.index_name, body=data)
        # print (f"Documento {data.} inserito correttamente.") """


class DummyDBManager(interfaces.DatabaseManager):
    def __init__(self):
        print("INIT DUMMY DB MANAGER")

    def save_data(self, data):
        print(f"Documento {data} ricevuto dal dummy db.")
