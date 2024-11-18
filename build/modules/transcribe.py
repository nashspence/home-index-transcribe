import json
import logging
import warnings

from shared import setup_logger, is_version_out_of_date, process_files_sequential

logger = logging

logger1 = logging.getLogger('pytorch_user_warnings')
logger1.setLevel(logging.WARNING)
file_handler = logging.FileHandler(f"./data/logs/pytorch_user_warnings.log")
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger1.addHandler(file_handler)
logger1.propagate = False

def warning_to_log(message, category, filename, lineno, file=None, line=None):
    logger1.warning('%s:%s: %s: %s', filename, lineno, category.__name__, message)

warnings.showwarning = warning_to_log
warnings.filterwarnings("ignore", category=UserWarning)

logger2 = logging.getLogger('matplotlib.font_manager')
logger2.setLevel(logging.WARNING)
file_handler = logging.FileHandler(f"./data/logs/matplotlib.font_manager.log")
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger2.addHandler(file_handler)
logger2.propagate = False

logger3 = logging.getLogger('speechbrain.utils.quirks')
logger3.setLevel(logging.WARNING)
file_handler = logging.FileHandler(f"./data/logs/speechbrain.utils.quirks.log")
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger3.addHandler(file_handler)
logger3.propagate = False

logger4 = logging.getLogger('pytorch_lightning.utilities.migration.utils')
logger4.setLevel(logging.WARNING)
file_handler = logging.FileHandler(f"./data/logs/pytorch_lightning.utilities.migration.utils.log")
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger4.addHandler(file_handler)
logger4.propagate = False

NAME = "transcribe"
VERSION = 1
FILTERABLE_FIELD_NAMES = []
SORTABLE_FIELD_NAMES = []

SUPPORTED_MIME_TYPES = {
    "audio/aac", "audio/flac", "audio/mpeg", "audio/mp4", "audio/ogg", 
    "audio/wav", "audio/webm", "audio/x-wav", "audio/x-m4a", "audio/x-ms-wma",
    "audio/x-ms-wax", "audio/x-flac", "audio/x-musepack", "audio/x-opus",
    "audio/x-vorbis", "audio/x-alac", "video/x-msvideo", "video/x-matroska", 
    "video/x-flv", "video/x-m4v", "video/x-mjpeg", "video/quicktime",
    "video/mp4", "video/mpeg", "video/webm", "video/ogg", "video/x-nut",
    "video/x-matroska", "application/x-mpegURL", "application/ogg", 
    "application/vnd.apple.mpegurl", "application/vnd.rn-realmedia",
    "application/vnd.rn-realmedia-vbr", "application/x-pn-realaudio", 
    "video/x-ms-asf", "video/x-ms-wmv", "video/3gpp", "video/3gpp2"
}

whisperx = None
async def init():
    global whisperx
    import whisperx as _whisperx
    whisperx = _whisperx
    return

def is_processing_needed(file_path, document, doc_db_path):
    return is_version_out_of_date(document, doc_db_path, NAME, VERSION, SUPPORTED_MIME_TYPES, ["transcription.json"])

async def cleanup():
    whisperx = None
    return

async def process_file(file):
    fp, doc, spath, mtime = file
    try:
        result = await get_document(fp, doc, spath, mtime)
        return result, {"success": 1}
    except Exception as e:
        logger.error(f'{NAME} {fp} failed: {e}')
        return None, {"failure": 1}

async def process_files(file_list, cancel_event):
    async for result in process_files_sequential(file_list, process_file, cancel_event, NAME):
        yield result

async def get_document(file_path, document, doc_db_path, mtime):
    global logger
    with setup_logger(document["id"], file_path, doc_db_path / "log.txt") as _logger:
        logger = _logger
        
        logger.info(f'{NAME} start {file_path}')

        audio = whisperx.load_audio(file_path)
        model = whisperx.load_model("medium", device="cpu", compute_type="int8")
        result = model.transcribe(audio, batch_size=16)
        import gc; gc.collect(); del model  # Clean up

        model_a, metadata = whisperx.load_align_model(language_code=result["language"], device="cpu")
        result = whisperx.align(result["segments"], model_a, metadata, audio, "cpu", return_char_alignments=False) 
        import gc; gc.collect(); del model_a  # Clean up

        diarize_model = whisperx.DiarizationPipeline(use_auth_token="REMOVED", device="cpu")
        diarize_segments = diarize_model(audio)
        import gc; gc.collect(); del diarize_model  # Clean up

        result = whisperx.assign_word_speakers(diarize_segments, result)
        
        plain_text = " ".join([segment["text"].strip() for segment in result["segments"]]).strip()
        plain_text = " ".join(plain_text.split())
        document["transcribed_audio"] = plain_text
        
        with open(doc_db_path / "transcription.json", 'w') as file:
            json.dump(result, file, indent=4)
        
        with open(doc_db_path / "version.json", 'w') as file:
            json.dump({"version": VERSION, "added_fields": ["transcribed_audio"]}, file, indent=4)

        logger.info(f'{NAME} done')
        return document