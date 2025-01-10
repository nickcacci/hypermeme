from abc import ABC, abstractmethod


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
    def save_data(self, data: dict):
        """Salva i dati nel database."""
        pass
