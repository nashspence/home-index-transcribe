import json

NAME = "transcribe"
VERSION = 6
PATH = "/app/modules/transcribe.py"
MAX_WORKERS = 1
FILTERABLE_FIELD_NAMES = ["transcribed_audio"]
SORTABLE_FIELD_NAMES = []

SUPPORTED_MIME_TYPES = {
    "audio/aac",
    "audio/flac",
    "audio/mpeg",
    "audio/mp4",
    "audio/ogg",
    "audio/wav",
    "audio/webm",
    "audio/x-wav",
    "audio/x-m4a",
    "audio/x-ms-wma",
    "audio/x-ms-wax",
    "audio/x-flac",
    "audio/x-musepack",
    "audio/x-opus",
    "audio/x-vorbis",
    "audio/x-alac",
    "video/x-msvideo",
    "video/x-matroska",
    "video/x-flv",
    "video/x-m4v",
    "video/x-mjpeg",
    "video/quicktime",
    "video/mp4",
    "video/mpeg",
    "video/webm",
    "video/ogg",
    "video/x-nut",
    "video/x-matroska",
    "application/x-mpegURL",
    "application/ogg",
    "application/vnd.apple.mpegurl",
    "application/vnd.rn-realmedia",
    "application/vnd.rn-realmedia-vbr",
    "application/x-pn-realaudio",
    "video/x-ms-asf",
    "video/x-ms-wmv",
    "video/3gpp",
    "video/3gpp2",
}


def handle_document_changed(pdoc, cdoc, fp, dir):
    if not cdoc["type"] in SUPPORTED_MIME_TYPES:
        return False
    if not "creation_date" in cdoc:
        return False
    version_path = dir / f"{NAME}.json"
    version = None
    if version_path.exists():
        with open(version_path, "r") as file:
            version = json.load(file)
    if version and version.get("version") == VERSION:
        return False
    return True
