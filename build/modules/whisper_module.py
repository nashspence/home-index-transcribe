from faster_whisper import WhisperModel
from threading import Lock
import re

max_processes = 14
model = WhisperModel("medium", device="cpu", num_workers=max_processes, cpu_threads=4, compute_type="int8")
model_lock = Lock()

NAME = "whisper"
FIELD_NAME = "whisperVersion"
MAX_WORKERS = 1
VERSION = 1

def does_support_mime(mime):
    if (mime.startswith("audio/") or mime.startswith("video/")):
        return True
    return False

def get_fields(file_path):
    with model_lock:
        audio_transcript = ""
        audio_transcript_segments = []
        
        segments, _ = model.transcribe(file_path)
        
        for segment in segments:
            audio_transcript += segment.text + " "
            audio_transcript_segments.append(segment)
            
        audio_transcript = re.sub(r'\s+', ' ', audio_transcript).strip()
        audio_transcript = audio_transcript.strip()
            
        return { "audio_transcript": audio_transcript, "audio_transcript_segments": audio_transcript_segments }