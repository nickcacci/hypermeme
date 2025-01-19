from abc import ABC, abstractmethod

import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from meme_analysis_pipeline.utils.models import EnrichedMeme


class MemeDescriber(ABC):
    @abstractmethod
    def describe(self, image_path: str) -> dict:
        """Genera una descrizione strutturata del meme."""
        pass


class EmbeddingCalculator(ABC):
    @abstractmethod
    def calculate_text_embedding(self, text: str) -> list:
        """Calcola l'embedding per un testo."""
        pass

    @abstractmethod
    def calculate_image_embedding(self, image_path: str) -> list:
        """Calcola l'embedding per un'immagine."""
        pass


class DatabaseManager(ABC):
    @abstractmethod
    def save_data(self, data: EnrichedMeme):
        """Salva i dati nel database."""
        pass


class ImageDownloader(ABC):
    @abstractmethod
    def save_img(self, img_url: str) -> str:
        """Salva i dati nel database. Ritorna l'url locale"""
        pass
