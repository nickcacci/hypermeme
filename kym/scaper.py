import os
import json
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from PIL import Image
from io import BytesIO
import open_clip
import torch
from elasticsearch import Elasticsearch
import logging

# Configuration
BASE_URL = "https://knowyourmeme.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}
OUTPUT_DIR = "./memes_dataset"
SCRAPED_DATA_FILE = "./scraped_data.json"

# Elasticsearch setup
es = Elasticsearch(["http://localhost:9200"])

# Logging setup
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Ensure output directory exists
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

# Load OpenCLIP model
device = "cuda" if torch.cuda.is_available() else "cpu"
model, preprocess, _ = open_clip.create_model_and_transforms(
    "ViT-B-32", pretrained="laion2b_s34b_b79k"
)
model = model.to(device)


def load_scraped_data():
    """Loads already scraped data from a JSON file."""
    if os.path.exists(SCRAPED_DATA_FILE):
        with open(SCRAPED_DATA_FILE, "r") as file:
            return json.load(file)
    return {}


def save_scraped_data(data):
    """Saves the scraped data to a JSON file."""
    with open(SCRAPED_DATA_FILE, "w") as file:
        json.dump(data, file, indent=4)


def fetch_page(url):
    """Fetches a webpage and returns its content"""
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        return response.content
    except Exception as e:
        logger.error(f"Failed to fetch page {url}: {e}")
        return None


def parse_meme_list(page_content):
    """Parses the meme list page and returns meme links."""
    soup = BeautifulSoup(page_content, "html.parser")
    meme_links = []
    for meme in soup.select(".entry-grid-body a"):
        href = meme.get("href")
        if href:
            meme_links.append(BASE_URL + href)
    return meme_links


def parse_meme_page(url):
    """Parses a single meme page to extract metadata and template info."""
    content = fetch_page(url)
    if not content:
        return None

    soup = BeautifulSoup(content, "html.parser")

    # Extract "About" section
    about_section = soup.find("section", {"id": "about"})
    about_text = about_section.get_text(strip=True) if about_section else ""

    # Extract year and tags
    year = None
    tags = []
    for detail in soup.select(".entry-info dl dt"):
        if "Year" in detail.text:
            year = detail.find_next("dd").text.strip()
        if "Tags" in detail.text:
            tags = [tag.text for tag in detail.find_next("dd").select("a")]

    # Extract template images
    templates = []
    for img in soup.select(".photo-gallery img"):
        img_url = img.get("src")
        if img_url and "template" in img_url.lower():
            templates.append(img_url)

    return {
        "about": about_text,
        "year": year,
        "tags": tags,
        "templates": templates,
    }


def download_images(image_urls, output_folder):
    """Downloads images from a list of URLs and saves them locally."""
    downloaded_images = []
    Path(output_folder).mkdir(parents=True, exist_ok=True)

    for idx, url in enumerate(image_urls, start=1):
        try:
            response = requests.get(url, headers=HEADERS)
            response.raise_for_status()
            img = Image.open(BytesIO(response.content))

            # Construct filename
            file_name = f"template-{idx}-{Path(url).name}"
            file_path = os.path.join(output_folder, file_name)

            # Save image
            img.save(file_path)
            downloaded_images.append(file_path)
        except Exception as e:
            logger.error(f"Failed to download {url}: {e}")

    return downloaded_images


def index_to_elasticsearch(data, index_name):
    """Indexes a document into Elasticsearch."""
    try:
        es.index(index=index_name, body=data)
        logger.info(
            f"Document indexed in {index_name}: {data.get('meme_name', 'unknown')}"
        )
    except Exception as e:
        logger.error(f"Failed to index document: {e}")


# Example Workflow
def main():
    # Load already scraped data
    scraped_data = load_scraped_data()

    # Step 1: Fetch meme list
    meme_list_url = f"{BASE_URL}/memes?sort=views"
    meme_list_page = fetch_page(meme_list_url)
    if not meme_list_page:
        logger.error("Failed to fetch meme list page.")
        return

    meme_links = parse_meme_list(meme_list_page)

    for meme_url in meme_links:
        meme_name = meme_url.split("/")[-1]

        if meme_name in scraped_data:
            logger.info(f"Skipping already scraped meme: {meme_name}")
            continue

        try:
            logger.info(f"Processing meme: {meme_url}")

            # Step 2: Parse meme page
            meme_data = parse_meme_page(meme_url)
            if not meme_data:
                logger.warning(f"Skipping meme due to failed parsing: {meme_url}")
                continue

            # Save meme data to Elasticsearch
            meme_data["meme_name"] = meme_name
            index_to_elasticsearch(meme_data, index_name="meme_metadata")

            # Step 3: Download template images
            output_folder = os.path.join(OUTPUT_DIR, meme_name, "templates")
            templates = download_images(meme_data["templates"], output_folder)

            # Step 4: Compute embeddings and index templates
            for template_path in templates:
                image = preprocess(Image.open(template_path)).unsqueeze(0).to(device)
                with torch.no_grad():
                    embedding = model.encode_image(image).cpu().numpy().tolist()

                # Index template
                index_to_elasticsearch(
                    {
                        "type": "template",
                        "meme_name": meme_name,
                        "template_path": template_path,
                        "clip_embedding": embedding,
                    },
                    index_name="meme_templates",
                )

            # Mark as scraped
            scraped_data[meme_name] = meme_data
            save_scraped_data(scraped_data)

        except Exception as e:
            logger.error(f"Error processing meme {meme_url}: {e}")


if __name__ == "__main__":
    main()
