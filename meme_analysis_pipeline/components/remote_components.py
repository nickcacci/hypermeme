import components.interfaces as interfaces
import utils.models as models
from google import genai
from google.genai import types
from dotenv import load_dotenv
from PIL import Image


class GoogleLlmDescriber(interfaces.MemeDescriber):
    def __init__(self):
        load_dotenv()
        self.client = genai.Client()
        self.MODEL_ID = "gemini-2.0-flash-exp"

    def describe(self, image_path: str) -> dict:
        image = Image.open(image_path)

        image.thumbnail([512, 512])

        # Fix for images with transparency
        if image.mode == "RGBA":
            image = image.convert("RGB")

        response = self.client.models.generate_content(
            model=self.MODEL_ID,
            contents=[image, "Using schema given, analyze this meme."],
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=models.Meme,
            ),
        )
        meme_analysis = models.Meme.model_validate_json(response.text)
        return meme_analysis


# TODO: add openAI
