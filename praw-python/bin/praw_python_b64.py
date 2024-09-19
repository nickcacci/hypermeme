""" L'interfacciamento con reddit sarà svolto da uno script in python che fa uso della libreria PRAW (Python Reddit API Wrapper) il cui compito sarà quello di fetchare i post in streaming dai subreddits prescelti e inoltrarli a logstash mediante interfaccia HTTP.
 Lo script avrà tre modalità d'esecuzione:
1. Streaming: quanto appena descritto, scaricherà i post non appena escono e li inoltrerà periodicamente.
2. SendN: potrà inviare N posts precedentemente archiviati, funzione utile per debug e per demo, si potrà scegliere l'intervallo di tempo tra l'invio di un post e il successivo.
3. DownloadN: scarica N posts e le relative immagini e li archivia
 """

DEFAULT_DATASET_PATH = "./dataset/dataset.csv"
DEFAULT_IMAGES_PATH = "./dataset/images/"
LOGSTASH_HTTP_ADDRESS = "http://localhost:8081"

import praw
import json
import time
import requests
import argparse
import os
import logging
from datetime import datetime
import categories
from dotenv import load_dotenv
from dotenv import dotenv_values
from urllib.request import urlopen
from urllib.parse import urlparse
import shutil
import pandas as pd
import math
import signal
import easyocr
import cv2
import base64


# https://medium.com/@khaerulumam42/gracefully-stopping-python-processes-inside-a-docker-container-0692bb5f860f
class GracefulKiller:
    def __init__(self):
        self.kill_now = False
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        self.kill_now = True
        logging.warning("gracefully exiting")


class meme:

    reader = easyocr.Reader(["en"])

    if not os.path.exists(DEFAULT_IMAGES_PATH):
        logging.info("Creating Images Dir...")
        os.mkdir(DEFAULT_IMAGES_PATH)

    def __init__(self, submission: praw.models.Submission):

        self.id = submission.id if hasattr(submission, "id") else None
        self.url = submission.url if hasattr(submission, "url") else None
        self.title = submission.title if hasattr(submission, "title") else None
        self.selftext = submission.selftext if hasattr(submission, "selftext") else None
        self.subreddit = (
            submission.subreddit.display_name
            if hasattr(submission, "subreddit")
            and hasattr(submission.subreddit, "display_name")
            else None
        )
        self.score = submission.score if hasattr(submission, "score") else None
        self.num_comments = (
            submission.num_comments if hasattr(submission, "num_comments") else None
        )
        self.created_utc = (
            submission.created_utc if hasattr(submission, "created_utc") else None
        )
        self.author = (
            submission.author.name
            if hasattr(submission, "author") and hasattr(submission.author, "name")
            else None
        )
        self.upvote_ratio = (
            submission.upvote_ratio if hasattr(submission, "upvote_ratio") else None
        )

    """     def to_json(self):
        return json.dumps(vars(self)) """

    # Funzione per ridimensionare l'immagine
    def _resize_image(self, max_width=800, max_height=800):
        height, width = self.image.shape[:2]

        # Se l'immagine supera le dimensioni massime, la ridimensioniamo
        if width > max_width or height > max_height:
            scaling_factor = min(max_width / width, max_height / height)
            new_size = (int(width * scaling_factor), int(height * scaling_factor))
            resized_image = cv2.resize(
                self.image, new_size, interpolation=cv2.INTER_AREA
            )
            self.image = resized_image

    # Funzione per comprimere l'immagine e verificare la dimensione
    def _compress_image(self, max_size_in_bytes=1 * 1024 * 1024, initial_quality=90):
        quality = initial_quality
        while (
            quality > 10
        ):  # Prova diverse qualità fino a raggiungere la dimensione desiderata
            # Codifica l'immagine in JPEG
            encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), quality]
            _, buffer = cv2.imencode(".jpg", self.image, encode_param)

            # Verifica la dimensione
            if len(buffer) <= max_size_in_bytes:
                self.image = buffer
                return

            # Se l'immagine è ancora troppo grande, riduci la qualità
            quality -= 10

        # Se non riesci a ottenere la dimensione desiderata, restituisci l'immagine con la qualità minima
        _, buffer = cv2.imencode(".jpg", image, [int(cv2.IMWRITE_JPEG_QUALITY), 10])
        self.image = buffer

    # Funzione per codificare l'immagine in Base64
    def _encode_image_to_base64(self):
        # Codifica in Base64
        self.image_base64 = base64.b64encode(self.image).decode("utf-8")

    def _generate_ocr(self, verbose=False):
        if isinstance(self.image, type(None)):
            print("ERROR: ", self.img_filename)
            return ""
        result = self.reader.readtext(self.img_url, detail=0, paragraph=True)
        result = " ".join(result)
        if verbose:
            print("Processed ", self.img_filename, " got: '", result, "'")
        return result

    # TODO: output_dir taken from args
    def _download(self, post_id, post_url, output_dir=DEFAULT_IMAGES_PATH):
        """Given an image url, save it to output_dir"""
        *_, extension = os.path.basename(urlparse(post_url).path).split(".")
        output_filename = os.path.join(output_dir, f"{post_id}.{extension}")

        _data = {"id": post_id, "filename": output_filename}
        downloaded = True

        # Download the file from `url` and save it locally under `output_filename`
        if not os.path.isfile(output_filename):
            try:
                with (
                    urlopen(post_url) as response,
                    open(output_filename, "wb") as out_file,
                ):

                    shutil.copyfileobj(response, out_file)
            except Exception as e:
                # print current working directory of script where it was launched, and exception
                print(f"Current working directory: {os.getcwd()}")
                print(f"Exception: {e}")
                downloaded = False

        if downloaded:
            logging.info(f"    Downloaded image {output_filename}")
            self.img_filename = os.path.basename(output_filename)
            self.img_url = output_filename
        else:
            logging.error(f"    Error downloading image {output_filename}")
            self.img_filename = os.path.basename(output_filename)
        return downloaded

    def download_image(self, storing=False):

        if self._download(self.id, self.url):
            if not cv2.haveImageReader(self.img_url):
                os.remove(self.img_url)
                return False
            self.image = cv2.imread(self.img_url)
            self.ocr_text = self._generate_ocr()
            if not storing:
                self._resize_image()
                self._compress_image()
                self._encode_image_to_base64()
            return True
        else:
            return False

    def to_series(self, storing=False):
        d = vars(self).copy()
        # print (d.keys())
        d.pop("image")
        if storing and "image_base64" in d.keys():
            d.pop("image_base64")
        return pd.Series(d)


# https://docs.python.org/3/howto/argparse.html#argparse-tutorial
def get_args():
    parser = argparse.ArgumentParser(description="PRAW Python")
    parser.add_argument(
        "-m",
        "--mode",
        type=str,
        required=True,
        help="Mode of execution: streaming, sendn, downloadn",
        choices=["streaming", "sendn", "downloadn"],
    )
    parser.add_argument(
        "-n",
        "--number",
        type=int,
        required=False,
        default=10,
        help="Number of posts to send/download",
    )
    parser.add_argument(
        "-a",
        "--address",
        type=str,
        required=False,
        default=LOGSTASH_HTTP_ADDRESS,
        help="Logstash HTTP address",
    )
    parser.add_argument(
        "-t",
        "--time",
        type=int,
        required=False,
        default=1,
        help="Time interval between posts",
    )
    parser.add_argument(
        "-d",
        "--dataset",
        type=str,
        required=False,
        default=DEFAULT_DATASET_PATH,
        help="Path to the dataset",
    )
    parser.add_argument(
        "--do-not-save",
        action="store_true",
        required=False,
        help="Do not save posts when streaming",
    )
    parser.add_argument(
        "-i",
        "--images",
        type=str,
        required=False,
        default=DEFAULT_IMAGES_PATH,
        help="Path to the images",
    )
    return parser.parse_args()


def get_reddit_instance():
    user_agent = "hyper-meme/0.1 by nc"
    load_dotenv()
    logging.info(f"dotenv values: {dotenv_values()}")

    CLIENT_ID = os.getenv("CLIENT_ID")
    CLIENT_SECRET = os.getenv("CLIENT_SECRET")

    reddit = praw.Reddit(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        user_agent=user_agent,
    )
    return reddit


def get_posts(reddit, subreddits, t):
    for submission in reddit.subreddit(subreddits).stream.submissions():
        yield submission
        time.sleep(t)


def sanitize_dict(d):
    """Rimuove o sostituisce i valori float non validi nel dizionario."""
    for key, value in d.items():
        if isinstance(value, float):
            if math.isnan(value) or math.isinf(value):
                d[key] = None  # Sostituisci con None o un altro valore appropriato
        elif isinstance(value, dict):
            sanitize_dict(value)
    d["img_url"] = d.pop("url")
    return d


def send_post(meme_dict, address):
    meme_dict = sanitize_dict(meme_dict)
    r = requests.post(
        address,
        json=meme_dict,
        headers={"Content-Type": "application/json"},
    )
    # show the post request and the response
    # logging.info(f"Post request: {meme_dict}")
    logging.info(f"Response: {r.text}")
    # time.sleep(1)


def download_posts(reddit, subreddits, n):

    n_c = n / len(categories.categories_id)

    # posts= []
    for c in categories.categories_id.values():
        print(f"Downloading {n_c} posts for class {categories.categories_names[c]}")
        yield [
            post
            for post in reddit.subreddit(
                "+".join(categories.categories_subreddits[c])
            ).new(limit=n_c)
        ]
        # https://www.reddit.com/r/redditdev/comments/gryuqv/how_do_i_get_a_multireddit_with_praw/


"""         posts+=[
                vars(post)
                for post in reddit.subreddit("+".join(categories_subreddits[c])).hot(limit=n_c)
            ] """


def load_or_create_dataset(dataset_path):
    if os.path.exists(dataset_path):
        dataset = pd.read_csv(dataset_path)
    else:
        # Create new dataset with columns from meme
        dataset = pd.DataFrame(
            columns=[
                "id",
                "title",
                "subreddit",
                "score",
                "num_comments",
                "created_utc",
                "author",
                "upvote_ratio",
                "img_filename",
                "ocr_text",
                "url",
                "selftext",
                "img_url",
            ]
        )
    return dataset


def resize_image(image, max_width=800, max_height=800):
    height, width = image.shape[:2]

    # Se l'immagine supera le dimensioni massime, la ridimensioniamo
    if width > max_width or height > max_height:
        scaling_factor = min(max_width / width, max_height / height)
        new_size = (int(width * scaling_factor), int(height * scaling_factor))
        resized_image = cv2.resize(image, new_size, interpolation=cv2.INTER_AREA)
        return resized_image
    return image


# Funzione per comprimere l'immagine e verificare la dimensione
def compress_image(image, max_size_in_bytes=1 * 1024 * 1024, initial_quality=90):
    quality = initial_quality
    while (
        quality > 10
    ):  # Prova diverse qualità fino a raggiungere la dimensione desiderata
        # Codifica l'immagine in JPEG
        encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), quality]
        _, buffer = cv2.imencode(".jpg", image, encode_param)

        # Verifica la dimensione
        if len(buffer) <= max_size_in_bytes:
            return buffer

        # Se l'immagine è ancora troppo grande, riduci la qualità
        quality -= 10

    # Se non riesci a ottenere la dimensione desiderata, restituisci l'immagine con la qualità minima
    _, buffer = cv2.imencode(".jpg", image, [int(cv2.IMWRITE_JPEG_QUALITY), 10])
    return buffer


# Funzione per codificare l'immagine in Base64
def encode_image_to_base64(image_buffer):
    # Codifica in Base64
    image_base64 = base64.b64encode(image_buffer).decode("utf-8")
    return image_base64


if __name__ == "__main__":
    logging.basicConfig(
        level=getattr(logging, os.getenv("LOGLEVEL", "").upper(), "INFO"),
        format="[%(asctime)s] %(levelname)s:%(name)s:%(message)s",
    )
    g = GracefulKiller()
    args = get_args()

    # print working directory
    logging.info(f"Current working directory: {os.getcwd()}")

    """    

    if args.time != None:
        print(f"Time: {args.time}")

    if args.number != None:
        print(f"Number: {args.number}")

    if args.dataset != None:
        print(f"Dataset: {args.dataset}")

    if args.images != None:
        print(f"Images: {args.images}")
    """
    match args.mode:
        case "streaming":
            if not args.do_not_save:
                dataset = load_or_create_dataset(args.dataset)
                existing_ids = set(dataset["id"])
                memes = []
            reddit = get_reddit_instance()
            logging.info("streaming")
            for post in get_posts(
                reddit=reddit,
                subreddits="+".join(categories.all_subreddits),
                t=args.time,
            ):
                if g.kill_now:
                    break
                if not type(post) == praw.models.Submission:
                    logging.error(
                        f"    type {type(post)} is not praw.models.Submission"
                    )
                    exit(1)
                # if post has attribute post_hint and its not an image, skip
                if post.is_self == True:
                    logging.info("   Self post, skipping : " + post.url)
                    continue
                if post.is_video == True:
                    logging.info("   Video post, skipping : " + post.url)
                    continue
                if hasattr(post, "post_hint") and post.post_hint != "image":
                    logging.info("   Not an image post, skipping : " + post.url)
                    continue
                m = meme(post)
                if m.download_image():
                    # log image path
                    logging.info("   Downloaded image successfully: " + m.img_filename)
                else:
                    logging.error("   Error downloading image: " + m.img_filename)
                    continue
                if not args.do_not_save:
                    if m.id in existing_ids:
                        logging.info(
                            f"   Post already in dataset, not saving it : {m.id}"
                        )
                    else:
                        memes.append(m.to_series(storing=True))
                        logging.info(f"   Post saved in dataset: {m.id}")

                send_post(m.to_series().to_dict(), args.address)
                logging.info(f"Post sent: {post.id}")
            if not args.do_not_save:
                logging.info("Saving dataset")
                ndf = pd.DataFrame(memes)
                dataset = pd.concat([dataset, ndf], ignore_index=True)
                dataset.to_csv(args.dataset, index=False)
                logging.info("Dataset saved")

        case "sendn":
            logging.info(f"Exec mode: sendn; n: {args.number}; time: {args.time}")
            # Check if dataset exists
            if not os.path.exists(args.dataset):
                logging.error(f"Dataset {args.dataset} not found")
                exit(1)
            # Load dataset
            dataset = pd.read_csv(args.dataset)
            # Pick n random posts
            posts = dataset.sample(n=args.number)
            # Send posts
            for _, post in posts.iterrows():
                pts = post.to_dict().copy()
                image = cv2.imread(post.img_url)
                image = resize_image(image)
                image = compress_image(image)
                image_b64 = encode_image_to_base64(image)
                pts["image_base64"] = image_b64

                send_post(pts, args.address)
                logging.info(f"Post sent: {post.id}")
                time.sleep(args.time)

        case "downloadn":
            logging.info(f"Exec mode: downloadn; n: {args.number}; time: {args.time}")
            assert args.number >= len(
                categories.categories_id
            ), "Number of posts must be at least the number of categories"
            reddit = get_reddit_instance()
            dataset = load_or_create_dataset(args.dataset)
            existing_ids = set(dataset["id"])
            memes = []
            for posts in download_posts(
                reddit, "+".join(categories.all_subreddits), args.number
            ):
                for post in posts:
                    logging.info("   Downloaded id : " + str(post.id))
                    if post.is_self == True:
                        logging.info("   Self post, skipping : " + post.url)
                        continue
                    if post.is_video == True:
                        logging.info("   Video post, skipping : " + post.url)
                        continue
                    if hasattr(post, "post_hint") and post.post_hint != "image":
                        logging.info("   Not an image post, skipping : " + post.url)
                        continue

                    if post.id in existing_ids:
                        logging.info(
                            "   Post already in dataset, skipping : " + post.id
                        )
                        continue
                    m = meme(post)
                    if m.download_image(storing=True):
                        # log image path
                        logging.info(
                            "   Downloaded image successfully: " + m.img_filename
                        )
                    else:
                        logging.error("   Error downloading image: " + m.img_filename)
                        continue

                    # Append to dataset
                    memes.append(m.to_series(storing=True))

            ndf = pd.DataFrame(memes)
            dataset = pd.concat([dataset, ndf], ignore_index=True)
            dataset.to_csv(args.dataset, index=False)
