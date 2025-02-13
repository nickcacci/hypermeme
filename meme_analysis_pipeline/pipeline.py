# Pipeline per l'analisi e l'arricchimento di meme, composta da:
# - un Language Model
# - un Text Embedder
# - un Image Embedder
# Tutti i componenti della pipeline sono modulari e possono essere sostituiti con altri modelli.
from PIL import Image
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), ".")))

from components.local_components import *
from components.remote_components import *
from components import interfaces
from utils.models import EnrichedMeme


class MemeAnalysisPipeline:

    def __init__(
        self,
        describer: interfaces.MemeDescriber = LocalLlmDescriber(),
        embedding_calculator: interfaces.EmbeddingCalculator = ClipEmbedder(),
        database_manager: interfaces.DatabaseManager = DummyDBManager(),
        image_downloader: interfaces.ImageDownloader = TmpImgDownloader(),
    ):
        self.describer = describer
        self.embedding_calculator = embedding_calculator
        self.database_manager = database_manager
        self.image_downloader = image_downloader

    def pipeline_info(self):
        print("Pipeline components:")
        print(f"Describer: {self.describer.__class__.__name__}")
        print(f"Embedding calculator: {self.embedding_calculator.__class__.__name__}")
        print(f"Database manager: {self.database_manager.__class__.__name__}")
        print(f"Image downloader: {self.image_downloader.__class__.__name__}")

    def process_meme(self, image_url: str, post_text: str = "", local_url: str = None):
        # 0. TODO: download image
        if local_url is None:
            self.local_url = self.image_downloader.save_img(image_url)
        else:
            self.local_url = local_url
        # 1. Descrivi il meme
        llm_enrichment = self.describer.describe(self.local_url)
        # text = description["text"]

        # 2. Calcola gli embeddings
        text_embedding = self.embedding_calculator.calculate_text_embedding(
            llm_enrichment.explainer
        )
        image_embedding = self.embedding_calculator.calculate_image_embedding(
            self.local_url
        )

        # TODO: template finding

        # 3. Crea l'oggetto EnrichedMeme
        enriched_meme = EnrichedMeme(
            enrichment=llm_enrichment,
            image_embedding=image_embedding,
            text_embedding=text_embedding,
            img_url=image_url,
            local_url=self.local_url,
            post_text=post_text,
        )

        # 4. Salva i dati nel database
        self.database_manager.save_data(enriched_meme)
        return enriched_meme


def createLocalPipeline(use_elastic_search: bool = False):
    if use_elastic_search:
        local_describer = LocalLlmDescriber()
        local_embedding_calculator = ClipEmbedder()
        local_database_manager = ElasticSearchManager()
        return MemeAnalysisPipeline(
            local_describer, local_embedding_calculator, local_database_manager
        )
    else:
        return MemeAnalysisPipeline()


def createGooglePipeline(use_dummy_db: bool = True):
    describer = GoogleLlmDescriber()
    local_embedding_calculator = ClipEmbedder()
    if use_dummy_db:
        database_manager = DummyDBManager()
    else:
        database_manager = ElasticSearchManager()

    return MemeAnalysisPipeline(describer, local_embedding_calculator, database_manager)


if __name__ == "__main__":
    # Just some test code
    # pipeline = createLocalPipeline()
    pipeline = createGooglePipeline()
    pipeline.process_meme("tests/test.jpeg")
    # print(pipeline.pipeline_info())
