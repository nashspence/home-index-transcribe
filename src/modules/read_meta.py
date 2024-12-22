import json

NAME = "read"
VERSION = 1
PATH = "/app/modules/read.py"
MAX_WORKERS = 1
FILTERABLE_FIELD_NAMES = ["read_text"]
SORTABLE_FIELD_NAMES = []

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

def handle_document_changed(pdoc, cdoc, fp, dir):
    if not cdoc["type"] in SUPPORTED_MIME_TYPES:
        return False
    version_path = dir / f"{NAME}.json"
    version = None
    if version_path.exists():
        with open(version_path, 'r') as file:
            version = json.load(file)  
    if version and version.get("version") == VERSION:
        return False
    return True
