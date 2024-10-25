import torch
import torchvision.transforms as T
import numpy as np
from PIL import Image
import rawpy
import io
from wand.image import Image as WandImage
from torchvision.models.detection import fasterrcnn_resnet50_fpn
import pytesseract
import logging
from pdf2image import convert_from_path

NAME = "visually identify entities" 
FIELD_NAME = "visually_identified_entities_version"    
DATA_FIELD_NAMES = ["visually_identified_entities", "visually_identified_text"]  # Add new field for text
MAX_WORKERS = 1                          
VERSION = 1                     

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
    'image/x-raw'
}

VECTOR_MIME_TYPES = {
    'image/svg+xml',
    'image/x-eps',
    'application/postscript',
    'application/eps',
    'image/vnd.adobe.photoshop',
    'application/vnd.adobe.photoshop',
    'application/x-photoshop',
    'application/photoshop',
    'image/vnd.adobe.illustrator',
    'application/vnd.adobe.illustrator',
    'application/illustrator',
    'application/x-illustrator'
}

STANDARD_IMAGE_MIME_TYPES = {
    'image/jpeg',
    'image/png',
    'image/gif',
    'image/tiff',
    'image/bmp',
    'image/webp'
}

SUPPORTED_MIME_TYPES = (
    PDF_MIME_TYPES |
    RAW_MIME_TYPES |
    VECTOR_MIME_TYPES |
    STANDARD_IMAGE_MIME_TYPES
)

model = None

def does_support_mime(mime):
    return mime in SUPPORTED_MIME_TYPES

async def init():
    global model
    model = fasterrcnn_resnet50_fpn(pretrained=True)
    model.eval()
    return

async def cleanup():
    global model
    model = None
    return

def process_image(image):
    transform = T.Compose([T.ToTensor()])
    
    if isinstance(image, Image.Image):
        image_tensor = transform(image).unsqueeze(0)
    else:
        image_tensor = image

    with torch.no_grad():
        predictions = model(image_tensor)[0]

    labels = predictions['labels']
    scores = predictions['scores']
    threshold = 0.5
    detected_objects = []

    for label, score in zip(labels, scores):
        if score >= threshold:
            detected_objects.append({
                "label": label.item(),
                "score": round(score.item(), 2)
            })
            
    return detected_objects

def extract_text_from_image(image):
    try:
        text = pytesseract.image_to_string(image)
        return text.strip()
    except Exception as e:
        print(f"Failed to extract text from image: {e}")
        return ""

async def get_fields(file_path, mime, info, doc):
    mime_type = doc.get("mime", "")
    detected_objects = []
    extracted_text = ""

    try:
        if mime_type in STANDARD_IMAGE_MIME_TYPES:
            image = Image.open(file_path).convert("RGB")
            detected_objects = process_image(image)
            extracted_text = extract_text_from_image(image)

        elif mime_type in RAW_MIME_TYPES:
            with rawpy.imread(file_path) as raw:
                rgb_image = raw.postprocess()
                image_pil = Image.fromarray(rgb_image)
                detected_objects = process_image(image_pil)
                extracted_text = extract_text_from_image(image_pil)

        elif mime_type in VECTOR_MIME_TYPES:
            with WandImage(filename=file_path, resolution=300) as img:
                img.format = 'png'
                image_blob = img.make_blob()
                image_pil = Image.open(io.BytesIO(image_blob))
                detected_objects = process_image(image_pil)
                extracted_text = extract_text_from_image(image_pil)
                
        elif mime_type in PDF_MIME_TYPES:
            pages = convert_from_path(file_path, dpi=300)
            for page in pages:
                detected_objects += process_image(page)
                extracted_text += extract_text_from_image(page) + "\n"

        result = {
            "visually_identified_entities": detected_objects,
            "visually_identified_text": extracted_text
        }
        
        yield result
        
    except Exception as e:
        logging.exception(f"Failed to process image {file_path}")
        raise e