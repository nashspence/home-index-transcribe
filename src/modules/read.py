import easyocr
import io
import json
import logging
import numpy as np
import rawpy
import re
import torch

from PIL import Image
from pdf2image import convert_from_path
from wand.image import Image as WandImage

from read_meta import (
    NAME,
    VERSION,
    PDF_MIME_TYPES,
    RAW_MIME_TYPES,
    VECTOR_MIME_TYPES,
    STANDARD_IMAGE_MIME_TYPES,
)

logger = logging

MAX_WIDTH, MAX_HEIGHT = 2000, 2000


def preprocess_image(image):
    width, height = image.size
    if width > MAX_WIDTH or height > MAX_HEIGHT:
        scaling_factor = min(MAX_WIDTH / width, MAX_HEIGHT / height)
        image = image.resize(
            (int(width * scaling_factor), int(height * scaling_factor)), Image.ANTIALIAS
        )
    return image


reader = easyocr.Reader(
    ["en"],
    gpu=torch.cuda.is_available(),
    verbose=False,
    model_storage_directory="/app/data/pytorch",
    download_enabled=True,
)


def extract_text_from_image(image):
    np_image = np.array(image)
    text = reader.readtext(np_image, detail=0, rotation_info=[0, 90, 180, 270])
    return " ".join(text).strip()


def main(file_path, document, doc_db_path, mtime, _logger):
    global logger
    logger = _logger
    logger.info(f"{NAME} start {file_path}")

    version_path = doc_db_path / f"{NAME}.json"
    read_text_path = doc_db_path / "read_text.json"

    read_text = ""

    mime_type = document.get("type", "")
    if mime_type in STANDARD_IMAGE_MIME_TYPES:
        image = Image.open(file_path).convert("RGB")
        read_text = extract_text_from_image(preprocess_image(image))
    elif mime_type in RAW_MIME_TYPES:
        with rawpy.imread(file_path) as raw:
            rgb_image = raw.postprocess()
            image_pil = Image.fromarray(rgb_image)
            read_text = extract_text_from_image(preprocess_image(image_pil))
    elif mime_type in VECTOR_MIME_TYPES:
        with WandImage(filename=file_path, resolution=300) as img:
            img.format = "png"
            image_blobs = [
                img.sequence[i].make_blob() for i in range(len(img.sequence))
            ]
            images = [
                np.array(preprocess_image(Image.open(io.BytesIO(blob))))
                for blob in image_blobs
            ]
            texts_list = reader.readtext_batched(images, detail=0)
            read_text = "\n".join([" ".join(texts) for texts in texts_list])
    elif mime_type in PDF_MIME_TYPES:
        pages = convert_from_path(file_path, dpi=300)
        images = [np.array(preprocess_image(page)) for page in pages]
        texts_list = reader.readtext_batched(images, detail=0)
        read_text = "\n".join([" ".join(texts) for texts in texts_list])

    document["read_text"] = (
        re.sub(r"\s{2,}", " ", re.sub(r"[^\w\s]+|\s{2,}", " ", str(read_text)))
        .strip()
        .lower()
    )
    with open(read_text_path, "w") as file:
        json.dump(read_text, file, indent=4)

    if version_path.exists():
        with open(version_path, "r") as file:
            version_info = json.load(file)
        logger.debug(f"nulling old fields")
        if "added_fields" in version_info:
            for field in version_info["added_fields"]:
                document[field] = None

    with open(version_path, "w") as file:
        json.dump({"version": VERSION, "added_fields": ["read_text"]}, file, indent=4)

    logger.info(f"{NAME} done")
    return document
