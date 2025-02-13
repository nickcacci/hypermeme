from pydantic import BaseModel
from typing import List


""" class Meme(BaseModel):
    # Internet url of the image
    img_url: str = ""
    # after you downloaded the image, its local url
    # local_url: str = ""

    # The text of the post (every social media allows users to add text even in image posts)
    post_text: str = ""

    # A description of the visual contents of the meme
    visual_description: str = ""

    # The text in the image
    image_text: str = ""

    # Meme description
    description: str = ""

    # Meme explainer
    explainer: str = ""

    # A list of 5 useful tags for retrieval
    tags: List[str] = []

    # clip_embedding
    # text_embedding

    # The template the Meme belongs to
    # template: str = ""
 """


from pydantic import BaseModel, Field


""" class Meme(BaseModel):
    visual_description: str = Field(
        default="", description="A description of the visual content of the image"
    )
    image_text: str = Field(default="", description="The text in the image")
    explainer: str = Field(default="", description="Meme explainer")
    tags: List[str] = Field(
        default_factory=list, description="A list of 5 useful tags for retrieval"
    ) """


class Meme(BaseModel):
    visual_description: str = Field(
        description="A description of the visual content of the image, do not include the main text in the meme, or caption. Include text in the background"
    )
    image_text: str = Field(description="The text in the image")
    explainer: str = Field(
        description="Meme explainer, you should use everything you grasped to produce a good explanation of the meme to someone who can't understand it. Try to be as comprehensive and complete as possible. Explain the joke, why it is funny? Also include any info that can help the user understand better the meme, try to don't exceed 100 characters"
    )
    tags: List[str] = Field(description="A list of 5 useful tags for retrieval")


class EnrichedMeme:

    def __init__(
        self,
        enrichment: Meme,
        image_embedding,
        text_embedding,
        img_url: str = "",
        local_url: str = "",
        post_text: str = "",
        template: str = "",
    ):
        self.img_url = img_url
        self.local_url = local_url
        self.post_text = post_text
        self.enrichment = enrichment
        self.image_embedding = image_embedding if image_embedding is not None else []
        self.text_embedding = text_embedding if text_embedding is not None else []
        self.template = template

    def __repr__(self):
        return f"EnrichedMeme(img_url={self.img_url}, local_url={self.local_url}, post_text={self.post_text}, enrichment={self.enrichment}, image_embedding={self.image_embedding.size()}, text_embedding={self.text_embedding.size()}, template={self.template})"


if __name__ == "__main__":
    print(Meme.model_json_schema())
