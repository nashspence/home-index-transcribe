from faster_whisper import WhisperModel
from threading import Lock
import re
import os
import subprocess
import logging
import glob

MAX_PROCESSES = 14
MODEL = None
MODEL_LOCK = Lock()

NAME = "whisper"
FIELD_NAME = "whisperVersion"
DATA_FIELD_NAMES = ["audio_transcript", "audio_transcript_segments"]
MAX_WORKERS = 1
VERSION = 1

DATA_DIR = "./data/whisper_module"
TEMP_FILE_PATTERN = f"{DATA_DIR}/whisper_temp*"

def ensure_data_directory():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

def remove_temp_files():
    files = glob.glob(TEMP_FILE_PATTERN)
    for file in files:
        os.remove(file)

def load_existing_segments(doc):
    if "audio_transcript_segments" in doc and doc["audio_transcript_segments"][-1]:
        return doc["audio_transcript"], doc["audio_transcript_segments"]
    return "", []

def generate_temp_file_name(file_path):
    extension = os.path.splitext(file_path)[1]
    return f"{DATA_DIR}/whisper_temp{extension}"

def extract_untranscribed_part(file_path, last_end_time, temp_file):
    ffmpeg_command = ['ffmpeg', '-y', '-ss', str(last_end_time), '-i', file_path, temp_file]
    subprocess.run(ffmpeg_command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def transcribe(temp_file):
    segments, _ = MODEL.transcribe(temp_file)
    return segments

def does_support_mime(mime):
    return mime.startswith("audio/") or mime.startswith("video/")

async def init():
    global MODEL
    MODEL = WhisperModel("medium", device="cpu", num_workers=MAX_PROCESSES, cpu_threads=4, compute_type="int8")
    ensure_data_directory()
    remove_temp_files()
    return

async def cleanup():
    MODEL = None
    remove_temp_files()
    return

async def get_fields(file_path, doc):
    with MODEL_LOCK:
        audio_transcript, audio_transcript_segments = load_existing_segments(doc)
        last_end_time = 0
        
        if len(audio_transcript_segments) > 0:
            logging.debug(f'whisper from {last_end_time} "{file_path}"')
            last_end_time = audio_transcript_segments[-1]["end"]

        temp_file = generate_temp_file_name(file_path)
        extract_untranscribed_part(file_path, last_end_time, temp_file)

        segments = transcribe(temp_file)

        logging.debug(f'whisper {last_end_time} "{file_path}"')
        for segment in segments:
            adjusted_start = segment.start + last_end_time
            adjusted_end = segment.end + last_end_time
            audio_transcript += segment.text + " "
            audio_transcript_segments.append({'start': adjusted_start, 'end': adjusted_end, 'text': segment.text})
            logging.debug(f'whisper {adjusted_end} "{file_path}"')
            yield {
                "audio_transcript": audio_transcript,
                "audio_transcript_segments": audio_transcript_segments
            }

        remove_temp_files()
        audio_transcript = re.sub(r'\s+', ' ', audio_transcript).strip()

        yield {
            "audio_transcript": audio_transcript,
            "audio_transcript_segments": audio_transcript_segments
        }
