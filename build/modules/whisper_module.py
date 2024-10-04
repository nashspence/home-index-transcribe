from faster_whisper import WhisperModel
from threading import Lock
import re
import sqlite3
import os
import subprocess
import logging
import glob

MAX_PROCESSES = 14
MODEL = None
MODEL_LOCK = Lock()

NAME = "whisper"
FIELD_NAME = "whisperVersion"
MAX_WORKERS = 1
VERSION = 1

DB_PATH = "./data/whisper_module/db"
DATA_DIR = "./data/whisper_module"
TEMP_FILE_PATTERN = f"{DATA_DIR}/whisper_temp*"

def ensure_data_directory():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

def remove_temp_files():
    files = glob.glob(TEMP_FILE_PATTERN)
    for file in files:
        os.remove(file)

def setup_database():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS segments (
            file_path TEXT,
            segment_id INTEGER,
            start_time REAL,
            end_time REAL,
            text TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS file_mtimes (
            file_path TEXT PRIMARY KEY,
            mtime REAL
        )
    ''')
    conn.commit()
    conn.close()

def get_db_connection():
    return sqlite3.connect(DB_PATH)

def get_previous_mtime(cursor, file_path):
    cursor.execute('SELECT mtime FROM file_mtimes WHERE file_path = ?', (file_path,))
    result = cursor.fetchone()
    return result[0] if result else None

def delete_segments(cursor, file_path):
    cursor.execute('DELETE FROM segments WHERE file_path = ?', (file_path,))

def insert_or_replace_mtime(cursor, file_path, mtime):
    cursor.execute('INSERT OR REPLACE INTO file_mtimes (file_path, mtime) VALUES (?, ?)', (file_path, mtime))

def get_existing_segment_info(cursor, file_path):
    cursor.execute('SELECT MAX(segment_id), MAX(end_time) FROM segments WHERE file_path = ?', (file_path,))
    result = cursor.fetchone()
    existing_segment_id = result[0] if result[0] is not None else -1
    last_end_time = result[1] if result[1] is not None else 0.0
    return existing_segment_id, last_end_time

def load_existing_segments(cursor, file_path):
    audio_transcript = ""
    audio_transcript_segments = []
    cursor.execute('SELECT start_time, end_time, text FROM segments WHERE file_path = ? ORDER BY segment_id', (file_path,))
    rows = cursor.fetchall()
    for start_time, end_time, text in rows:
        audio_transcript += text + " "
        audio_transcript_segments.append({'start': start_time, 'end': end_time, 'text': text})
    return audio_transcript, audio_transcript_segments

def generate_temp_file_name(file_path):
    extension = os.path.splitext(file_path)[1]
    return f"{DATA_DIR}/whisper_temp{extension}"

def extract_untranscribed_part(file_path, last_end_time, temp_file):
    ffmpeg_command = ['ffmpeg', '-y', '-ss', str(last_end_time), '-i', file_path, temp_file]
    subprocess.run(ffmpeg_command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def transcribe(temp_file):
    segments, _ = MODEL.transcribe(temp_file)
    return segments

def insert_segment(cursor, file_path, segment_id, adjusted_start, adjusted_end, text):
    cursor.execute('''
        INSERT INTO segments (file_path, segment_id, start_time, end_time, text)
        VALUES (?, ?, ?, ?, ?)
    ''', (file_path, segment_id, adjusted_start, adjusted_end, text))

def delete_mtime(cursor, file_path):
    cursor.execute('DELETE FROM file_mtimes WHERE file_path = ?', (file_path,))

def does_support_mime(mime):
    return mime.startswith("audio/") or mime.startswith("video/")

async def init():
    global MODEL
    MODEL = WhisperModel("medium", device="cpu", num_workers=MAX_PROCESSES, cpu_threads=4, compute_type="int8")
    ensure_data_directory()
    remove_temp_files()
    setup_database()
    return

async def cleanup():
    MODEL = None
    return

async def get_fields(file_path, mime):
    conn = get_db_connection()
    cursor = conn.cursor()

    with MODEL_LOCK:
        audio_transcript = ""
        audio_transcript_segments = []

        current_mtime = os.path.getmtime(file_path)
        previous_mtime = get_previous_mtime(cursor, file_path)

        if previous_mtime is not None and previous_mtime != current_mtime:
            delete_segments(cursor, file_path)
            conn.commit()

        insert_or_replace_mtime(cursor, file_path, current_mtime)
        conn.commit()

        existing_segment_id, last_end_time = get_existing_segment_info(cursor, file_path)

        if existing_segment_id >= 0:
            logging.info(f'whisper resume @{last_end_time} "{file_path}"')
            audio_transcript, audio_transcript_segments = load_existing_segments(cursor, file_path)

        temp_file = generate_temp_file_name(file_path)
        extract_untranscribed_part(file_path, last_end_time, temp_file)

        segments = transcribe(temp_file)
        segment_id = existing_segment_id + 1

        for segment in segments:
            adjusted_start = segment.start + last_end_time
            adjusted_end = segment.end + last_end_time
            insert_segment(cursor, file_path, segment_id, adjusted_start, adjusted_end, segment.text)
            conn.commit()
            segment_id += 1
            audio_transcript += segment.text + " "
            audio_transcript_segments.append({'start': adjusted_start, 'end': adjusted_end, 'text': segment.text})

        remove_temp_files()
        audio_transcript = re.sub(r'\s+', ' ', audio_transcript).strip()
        delete_segments(cursor, file_path)
        delete_mtime(cursor, file_path)
        conn.commit()
        conn.close()

        return {
            "audio_transcript": audio_transcript,
            "audio_transcript_segments": audio_transcript_segments
        }
