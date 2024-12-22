import time
import json
import tika

tika.TikaClientOnly = True
from tika import config

NAME = "scrape"
VERSION = 1
PATH = "/app/modules/scrape.py"
MAX_WORKERS = 1
FILTERABLE_FIELD_NAMES = [
    "_geo",
    "altitude",
    "audio_bit_depth",
    "audio_bit_rate",
    "audio_channels",
    "audio_codec",
    "audio_sample_rate",
    "camera_lens_make",
    "camera_lens_model",
    "creation_date",
    "creation_date_precision" "creation_date_is_inferred",
    "creation_date_offset_seconds",
    "creation_date_offset_is_inferred",
    "creation_date_special_filepath_type",
    "device_make",
    "device_model",
    "duration",
    "height",
    "video_bit_rate",
    "video_codec",
    "video_frame_rate",
    "width",
]
SORTABLE_FIELD_NAMES = ["_geo", "duration", "creation_date"]

TIKA_MIMES = {}
for attempt in range(30):
    try:
        TIKA_MIMES = set(json.loads(config.getMimeTypes()))
        break
    except:
        time.sleep(1 * attempt)


def handle_document_changed(pdoc, cdoc, fp, dir):
    version = None
    version_path = dir / f"{NAME}.json"
    if version_path.exists():
        with open(version_path, "r") as file:
            version = json.load(file)
    if version and version.get("file_path") == fp and version.get("version") == VERSION:
        return False
    return True
