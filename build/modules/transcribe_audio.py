from faster_whisper import WhisperModel
from threading import Lock
import os
import subprocess
import logging
import glob

MAX_PROCESSES = 14
MODEL = None
MODEL_LOCK = Lock()

NAME = "transcribe audio"
FIELD_NAME = "transcribed_audio_version"
DATA_FIELD_NAMES = ["transcribed_audio"]
MAX_WORKERS = 1
VERSION = 1

DATA_DIR = "./data/whisper_module"
TEMP_FILE_PATTERN = f"{DATA_DIR}/transcribedAudio_temp*"

def ensure_data_directory():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

def remove_temp_files():
    files = glob.glob(TEMP_FILE_PATTERN)
    for file in files:
        os.remove(file)

def load_existing_segments(doc):
    if "transcribed_audio" in doc and doc["transcribed_audio"][-1]:
        return doc["transcribed_audio"]
    return []

def generate_temp_file_name(file_path):
    extension = os.path.splitext(file_path)[1]
    return f"{DATA_DIR}/whisper_temp{extension}"

def extract_untranscribed_part(file_path, last_end_time, temp_file):
    ffmpeg_command = ['ffmpeg', '-y', '-ss', str(last_end_time), '-i', file_path, temp_file]
    subprocess.run(ffmpeg_command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

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
    try:
        with MODEL_LOCK:
            transcribedAudio = load_existing_segments(doc)
            last_end_time = 0
            
            if len(transcribedAudio) > 0:
                last_end_time = transcribedAudio[-1]["end"]
                
            temp_file = generate_temp_file_name(file_path)
            extract_untranscribed_part(file_path, last_end_time, temp_file)

            vad_parameters = {
                'threshold': 0.20,
                'min_speech_duration_ms': 100,
                'max_speech_duration_s': float("inf"),
                'min_silence_duration_ms': 5000,
                'speech_pad_ms': 100
            }

            logging.debug(f'transcribe audio {last_end_time} "{file_path}"')
            segments, _ = MODEL.transcribe(temp_file, language="en", vad_filter=True, vad_parameters=vad_parameters)
            
            for segment in segments:
                adjusted_start = segment.start + last_end_time
                adjusted_end = segment.end + last_end_time
                transcribedAudio.append({'start': adjusted_start, 'end': adjusted_end, 'text': segment.text})
                logging.debug(f'transcribe audio {adjusted_start} {adjusted_end} "{segment.text}" "{file_path}"')
                yield {
                    "transcribed_audio": transcribedAudio
                }

            remove_temp_files()
    except Exception as e:
        logging.exception(f"transcribe audio exception")
        raise e
