import easyocr
import numpy as np
from threading import Lock
from PIL import Image
import rawpy
import io
from wand.image import Image as WandImage
import os

NAME = "easyocr"
FIELD_NAME = "easyocrVersion"
MAX_WORKERS = 1
VERSION = 1

# MIME types categorized by processing method
PDF_MIME_TYPES = {'application/pdf'}

RAW_MIME_TYPES = {
    'image/x-adobe-dng',
    'image/x-canon-cr2',
    'image/x-canon-crw',
    'image/x-nikon-nef',
    'image/x-sony-arw',
    'image/x-panasonic-raw',
    'image/x-olympus-orf',
    'image/x-fuji-raf',
    'image/x-sigma-x3f',
    'image/x-pentax-pef',
    'image/x-samsung-srw',
    'image/x-raw',
}

VECTOR_MIME_TYPES = {
    'image/svg+xml',
    'image/x-eps',
    'application/postscript',
    'application/eps',
    'image/vnd.adobe.photoshop',       # PSD files
    'application/vnd.adobe.photoshop',
    'application/x-photoshop',
    'application/photoshop',
    'image/vnd.adobe.illustrator',     # AI files
    'application/vnd.adobe.illustrator',
    'application/illustrator',
    'application/x-illustrator',
}

STANDARD_IMAGE_MIME_TYPES = {
    'image/jpeg',
    'image/png',
    'image/gif',
    'image/bmp',
    'image/tiff',
    'image/webp',
}

# Combine all supported MIME types
SUPPORTED_MIME_TYPES = (
    PDF_MIME_TYPES |
    RAW_MIME_TYPES |
    VECTOR_MIME_TYPES |
    STANDARD_IMAGE_MIME_TYPES
)

DATA_DIR = "./data/easyocr_module"

def ensure_data_directory():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        
def does_support_mime(mime):
    return mime in SUPPORTED_MIME_TYPES

# Initialize EasyOCR reader with desired languages
ensure_data_directory()
reader = easyocr.Reader(['en'], model_storage_directory=f'{DATA_DIR}', download_enabled=True, gpu=False)
reader_lock = Lock()

async def init():
    return

async def cleanup():
    return

async def get_fields(file_path, mime):
    text = ""
    
    with reader_lock:
        if mime in PDF_MIME_TYPES:
            # Convert PDF to images
            from pdf2image import convert_from_path
            images = convert_from_path(file_path)
            for image in images:
                image_np = np.array(image)
                results = reader.readtext(image_np, detail=0)
                text += ' '.join(results) + ' '
            text = text.strip()
        elif mime in RAW_MIME_TYPES:
            # Process RAW image
            with rawpy.imread(file_path) as raw:
                rgb_image = raw.postprocess()
                image_pil = Image.fromarray(rgb_image)
                image_np = np.array(image_pil)
            # Perform OCR
            results = reader.readtext(image_np, detail=0)
            text = ' '.join(results)
        elif mime in VECTOR_MIME_TYPES:
            # Use wand to convert vector images and Adobe formats to raster images
            with WandImage(filename=file_path, resolution=300) as img:
                img.format = 'png'
                image_blob = img.make_blob()
                image_pil = Image.open(io.BytesIO(image_blob))
                image_np = np.array(image_pil)
            # Perform OCR
            results = reader.readtext(image_np, detail=0)
            text = ' '.join(results)
        elif mime in STANDARD_IMAGE_MIME_TYPES:
            # Process standard image formats
            image_pil = Image.open(file_path)
            image_np = np.array(image_pil)
            # Perform OCR
            results = reader.readtext(image_np, detail=0)
            text = ' '.join(results)
        else:
            # Unsupported MIME type
            print(f"Unsupported MIME type: {mime}")
            
    return {"ocr": text}
