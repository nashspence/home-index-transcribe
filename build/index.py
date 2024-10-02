import asyncio
import hashlib
import inspect
import json
import logging
import magic
import os
import re
import sqlite3

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, time as datetime_time
from faster_whisper import WhisperModel
from logging.handlers import TimedRotatingFileHandler
from meilisearch_python_sdk import AsyncClient
from multiprocessing import Process
from pathlib import Path
from tika import parser, config as tika_config
from time import time
from threading import Lock
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        TimedRotatingFileHandler(
            "./data/indexer.log",
            when="midnight",
            interval=1,
            backupCount=7,
            atTime=datetime_time(2, 30)
        ),
        logging.StreamHandler(),
    ],
)

other_loggers = ['httpx', 'tika.tika', 'faster_whisper'] 
for logger_name in other_loggers:
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler(f"./data/{logger_name}.log")
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(file_handler)
    logger.propagate = False 

VERSION = 1
DIRECTORY_TO_INDEX = os.environ.get("DIRECTORY_TO_INDEX", "/data")
MEILISEARCH_HOST = os.environ.get("MEILISEARCH_HOST", "http://meilisearch:7700")
INDEX_NAME = os.environ.get("INDEX_NAME", "files")
DOMAIN = os.environ.get("DOMAIN", "private.0819870.xyz")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "1000"))
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "32"))
DB_FILE = os.environ.get('DB_FILE', './data/sqlite3.db')

sqlite3_connection = sqlite3.connect(DB_FILE)
sqlite3_cursor = sqlite3_connection.cursor()
meili_index = None

async def sleep_until_3am():
    now = datetime.now()
    next_run = now.replace(hour=3, minute=0, second=0, microsecond=0)
    if now >= next_run:
        next_run += timedelta(days=1)
    await asyncio.sleep((next_run - now).total_seconds())
    
deadline = datetime.now()  
def reset_deadline():
    global deadline
    start_time = datetime.now()
    deadline = start_time.replace(hour=2, minute=0, second=0, microsecond=0)
    if start_time >= deadline:
        deadline += timedelta(days=1)

def is_deadline_passed():
    return datetime.now() > deadline

def remaining_time_until_deadline():
    now = datetime.now()
    remaining_time = (deadline - now).total_seconds()
    if remaining_time < 0:
        return 0
    return remaining_time

def init_db(): 
    sqlite3_cursor.execute('PRAGMA foreign_keys = ON;')
    
    sqlite3_cursor.execute('''
        CREATE TABLE IF NOT EXISTS settings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mtime REAL
        )
    ''')
    
    sqlite3_cursor.execute('''
        CREATE TABLE IF NOT EXISTS file_paths (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_path TEXT UNIQUE,
            mtime REAL
        );
    ''')

    sqlite3_cursor.execute('''
        CREATE TABLE IF NOT EXISTS job_file_paths (
            job_name TEXT,
            file_path_id INTEGER,
            FOREIGN KEY (file_path_id) REFERENCES file_paths (id) ON DELETE CASCADE,
            PRIMARY KEY (job_name, file_path_id)
        );
    ''')
    
    try:
        load_mtime_db()
    except Exception as e:
        sqlite3_cursor.execute('INSERT INTO settings (mtime) VALUES (?)', (0,))
        
    sqlite3_connection.commit()
    
def update_mtime_db(mtime):
    sqlite3_cursor.execute('UPDATE settings SET mtime = ? WHERE id = 1', (mtime,))
    sqlite3_connection.commit()

def load_mtime_db():
    sqlite3_cursor.execute('SELECT mtime FROM settings WHERE id = 1')
    mtime = sqlite3_cursor.fetchone()[0]
    return mtime

def add_or_replace_file_paths(file_path_rows):
    sqlite3_cursor.executemany("""
        INSERT INTO file_paths (file_path, mtime) 
        VALUES (?, ?)
        ON CONFLICT(file_path) 
        DO UPDATE SET mtime = excluded.mtime
    """, [(row['file_path'], row['mtime']) for row in file_path_rows])
    sqlite3_connection.commit()
    
def update_file_path_db(old_file_path, new_file_path):
    sqlite3_cursor.execute('''
        UPDATE file_paths SET file_path = ? WHERE file_path = ?
    ''', (new_file_path, old_file_path))
    sqlite3_connection.commit()
    
def get_file_path_mtime_db(file_path):
    result = sqlite3_cursor.execute("SELECT mtime FROM file_paths WHERE file_path = ?", (file_path,)).fetchone()
    
    if result is None:
        return None
    
    return result
    
def add_job_file_paths_db(job_name, file_paths):
    for file_path in file_paths:
        sqlite3_cursor.execute('''
            SELECT id FROM file_paths WHERE file_path = ?
        ''', (file_path,))
        
        file_path_id = sqlite3_cursor.fetchone()[0]

        sqlite3_cursor.execute('''
            INSERT OR IGNORE INTO job_file_paths (job_name, file_path_id)
            VALUES (?, ?)
        ''', (job_name, file_path_id))
    
    sqlite3_connection.commit()
    
def get_file_paths_db():
    sqlite3_cursor.execute("SELECT file_path FROM file_paths")
    db_file_paths = {row[0] for row in sqlite3_cursor.fetchall()}
    return db_file_paths
    
def delete_file_paths_db(file_paths_to_delete):
    if not file_paths_to_delete:
        return

    file_paths_list = list(file_paths_to_delete)

    query = "DELETE FROM file_paths WHERE file_path IN ({})".format(
        ",".join("?" * len(file_paths_list))
    )
    
    sqlite3_cursor.execute(query, file_paths_list)
    sqlite3_connection.commit()

def get_job_file_paths_db(job_name):
    sqlite3_cursor.execute('''
        SELECT fp.file_path
        FROM job_file_paths jfp
        JOIN file_paths fp ON jfp.file_path_id = fp.id
        WHERE jfp.job_name = ?
        ORDER BY fp.mtime DESC
    ''', (job_name,))

    return [row[0] for row in sqlite3_cursor.fetchall()]
    
def remove_job_name_file_path_db(job_name, file_path):
    sqlite3_cursor.execute('''
        SELECT id FROM file_paths WHERE file_path = ?
    ''', (file_path,))
    
    result = sqlite3_cursor.fetchone()

    if result is None:
        return

    file_path_id = result[0]

    sqlite3_cursor.execute('''
        DELETE FROM job_file_paths WHERE job_name = ? AND file_path_id = ?
    ''', (job_name, file_path_id))
    
    sqlite3_connection.commit()

def clear_job_name_file_paths_db(job_name):
    sqlite3_cursor.execute("DELETE FROM job_file_paths WHERE job_name = ?", (job_name,))
    sqlite3_connection.commit()

async def init_meili():
    global meili_index
    client = AsyncClient(MEILISEARCH_HOST)
        
    try:
        meili_index = await client.get_index(INDEX_NAME)
    except Exception as e:
        if e.code == "index_not_found":
            try:
                await client.create_index(INDEX_NAME, "id")
                logging.info(f"Creating meili index '{INDEX_NAME}'.")
                meili_index = await client.get_index(INDEX_NAME)
            except Exception as e:
                logging.error(f"Init meili error: {e.message}")
                raise
        else:
            logging.error(f"Init meili error: {e.message}")
            raise

async def get_doc_count_meili():
    try:
        stats = await meili_index.get_stats()
        return stats.number_of_documents
    except Exception as e:
        logging.error(f"Error getting meili index stats.: {e}")
        raise

async def add_or_update_doc_meili(doc):
    if doc:
        try:
            await meili_index.update_documents([doc])
        except Exception as e:
            logging.error(f"Error adding or updating meili doc.: {e}")
            raise

async def add_or_update_docs_meili(docs):
    if docs:
        try:
            for i in range(0, len(docs), BATCH_SIZE):
                batch = docs[i:i+BATCH_SIZE]
                await meili_index.update_documents(batch)
        except Exception as e:
            logging.error(f"Error adding and updating meili docs.: {e}")
            raise

async def delete_docs_by_id_meili(ids):
    try:
        if ids:
            for i in range(0, len(ids), BATCH_SIZE):
                batch = ids[i:i+BATCH_SIZE]
                await meili_index.delete_documents(ids=batch)
    except Exception as e:
        logging.error(f"Error deleting docs by id from meilisearch: {e}")
        raise
    
async def get_doc_meili(doc_id):
    try:
        doc = await meili_index.get_document(doc_id)
        return doc
    except Exception as e:
        logging.error(f"Error getting document with id {doc_id}: {e}")
        return None

def get_mime_magic(file_path):
    mime = magic.Magic(mime=True)
    mime_type = mime.from_file(file_path)
    return mime_type

def get_meili_id_from_relative_path(relative_path):
    return hashlib.sha256(relative_path.encode()).hexdigest()

async def sync_meili_docs(job_does_support_mime_func_map):
    logging.info("Syncing meili docs with directory files.")

    previous_mtime = load_mtime_db()
    current_time = time()

    exists_lock = Lock()
    exists = []
    exists_rows = []

    updated_lock = Lock()
    updated = []

    file_paths_by_job_lock = Lock()
    file_paths_by_job_name = {job_name: [] for job_name in job_does_support_mime_func_map}

    def handle_file_path(entry):
        try:
            if entry.is_file(follow_symlinks=False):
                path = Path(entry.path)
                relative_path = path.relative_to(DIRECTORY_TO_INDEX).as_posix()
                stat = path.stat()
                
                logging.info(f'found "{relative_path}"')
                current_mtime = entry.stat().st_mtime
                
                with exists_lock:
                    exists.append(entry.path)
                    exists_rows.append({"file_path": entry.path, "mtime": current_mtime})
                
                if current_mtime > previous_mtime:
                    mime = get_mime_magic(path)

                    document = {
                        "id": get_meili_id_from_relative_path(relative_path),
                        "name": path.name,
                        "size": stat.st_size,
                        "mtime": current_mtime,
                        "ctime": stat.st_ctime,
                        "url": f"https://{DOMAIN}/{relative_path}",
                        "type": mime,
                    }

                    with updated_lock:
                        updated.append(document)

                    for job_name, does_support_mime in job_does_support_mime_func_map.items():
                        if does_support_mime(mime):
                            with file_paths_by_job_lock:
                                file_paths_by_job_name[job_name].append(entry.path)
        except Exception as e:
            logging.exception(f'failed "{relative_path}": {e}')
    
    directories_to_scan = [DIRECTORY_TO_INDEX]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        while directories_to_scan:
            dir_path = directories_to_scan.pop()
            with os.scandir(dir_path) as it:
                for entry in it:
                    if entry.is_dir(follow_symlinks=False):
                        directories_to_scan.append(entry.path)
                    else:
                        executor.submit(handle_file_path, entry)

        executor.shutdown(wait=True)

    add_or_replace_file_paths(exists_rows)

    for job_name, file_paths in file_paths_by_job_name.items():
        if file_paths:
            add_job_file_paths_db(job_name, file_paths)

    await add_or_update_docs_meili(updated)
    
    deleted = list(set(get_file_paths_db()) - set(exists))
    
    deleted_meili_ids = [
        get_meili_id_from_relative_path(Path(file_path).relative_to(DIRECTORY_TO_INDEX).as_posix())
        for file_path in deleted
    ]
    
    await delete_docs_by_id_meili(deleted_meili_ids)
    delete_file_paths_db(deleted)
    
    update_mtime_db(current_time)
    count = await get_doc_count_meili()

    logging.info(f"Done syncing {count} meili docs with directory files.")

class MoveEventHandler(FileSystemEventHandler):
    def __init__(self, loop):
        self.loop = loop
    def on_moved(self, event):
        if not event.is_directory:
            old_path = event.src_path
            new_path = event.dest_path
            self.loop.call_soon_threadsafe(asyncio.create_task, handle_file_move(old_path, new_path)) 
    
async def handle_file_move(old_file_path, new_file_path):
    try:
        update_file_path_db(old_file_path, new_file_path)
        
        old_path = Path(old_file_path)
        new_path = Path(new_file_path)
        
        old_relative_path = old_path.relative_to(DIRECTORY_TO_INDEX).as_posix()
        new_relative_path = new_path.relative_to(DIRECTORY_TO_INDEX).as_posix()
        
        old_doc_id = get_meili_id_from_relative_path(old_relative_path)
        new_doc_id = get_meili_id_from_relative_path(new_relative_path)
        
        old_doc = await get_doc_meili(old_doc_id)
        
        if not old_doc:
            logging.warning(f"Document with id {old_doc_id} not found in Meilisearch")
            return
        
        await delete_docs_by_id_meili([old_doc_id])
        
        old_doc['id'] = new_doc_id
        old_doc['name'] = new_path.name
        old_doc['url'] = f"https://{DOMAIN}/{new_relative_path}"
        
        await add_or_update_doc_meili(old_doc)
        logging.info(f'update meili doc "{old_relative_path}" is now "{new_relative_path}"')
    except Exception as e:
        logging.error(f'failed to update meili doc "{old_relative_path}" to "{new_relative_path}": {e}')
        
def start_observer():
    init_db()
    asyncio.run(init_meili())
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    observer = Observer()
    handler = MoveEventHandler(loop)
    observer.schedule(handler, DIRECTORY_TO_INDEX, recursive=True)
    observer.start()
    try:
        loop.run_forever()
    finally:
        observer.stop()
        observer.join()
    
async def augment_meili_docs(tool_name, get_additional_fields_with_tool, max_workers=MAX_WORKERS):
    if is_deadline_passed():
        return
    
    try:
        file_paths = get_job_file_paths_db(tool_name)
    except Exception as e:
        logging.exception(f"Failed to get job file paths for {tool_name}: {e}")
        
    if len(file_paths) > 0:
        logging.info(f"Augmenting meili docs with {tool_name}.")

        semaphore = asyncio.Semaphore(max_workers)
        counter = 0
        counter_lock = asyncio.Lock()
    
        async def handle_file_path(file_path):
            nonlocal counter
            
            async with semaphore:
                path = Path(file_path)
                relative_path = path.relative_to(DIRECTORY_TO_INDEX).as_posix()
                
                logging.info(f'{tool_name} start {counter}/{len(file_paths)} "{relative_path}"')

                if not path.exists():
                    async with counter_lock:
                        counter += 1
                        logging.error(f'{tool_name} error {counter}/{len(file_paths)} "{relative_path}": not found')
                        
                    return

                try:                    
                    if inspect.iscoroutinefunction(get_additional_fields_with_tool):
                        additional_fields = await get_additional_fields_with_tool(file_path)
                    else:
                        additional_fields = await asyncio.to_thread(get_additional_fields_with_tool, file_path)

                    doc = {
                        "id": get_meili_id_from_relative_path(relative_path),
                        **additional_fields,
                    }

                    await add_or_update_doc_meili(doc)
                    remove_job_name_file_path_db(tool_name, file_path)
                    
                    async with counter_lock:
                        counter += 1
                        logging.info(f'{tool_name} done {counter}/{len(file_paths)} "{relative_path}"')
                except Exception as e:
                    async with counter_lock:
                        counter += 1
                        logging.exception(f'{tool_name} error {counter}/{len(file_paths)} "{relative_path}": {e}')
                        
                return
            
        try:
            tasks = [asyncio.create_task(handle_file_path(file_path)) for file_path in file_paths]
            try:
                await asyncio.wait_for(asyncio.gather(*tasks), timeout=remaining_time_until_deadline())
            except asyncio.TimeoutError:
                logging.info(f"{tool_name} stopping. Deadline reached.")
        except Exception as e:
            logging.exception(f'Error reading with {tool_name}: {e}')
            
        logging.info(f"Done augmenting meili docs with {tool_name}.")
    
tika_mimes = json.loads(tika_config.getMimeTypes())
def does_support_mime_tika(mime):
    archive_mimes = {
        'application/zip', 'application/x-tar', 'application/gzip',
        'application/x-bzip2', 'application/x-7z-compressed', 'application/x-rar-compressed'
    }
    
    if mime in tika_mimes:
        if (mime.startswith("text/") or mime.startswith("application/")) and mime not in archive_mimes:
            return True
    return False    
    
def get_tika_fields(file_path):
    parsed = parser.from_file(file_path)
    return { "text": parsed.get("content", "") }

def does_support_mime_whisper(mime):
    if (mime.startswith("audio/") or mime.startswith("video/")):
        return True
    return False

max_processes = 14
model = WhisperModel("medium", device="cpu", num_workers=max_processes, cpu_threads=4, compute_type="int8")
model_lock = Lock()
def get_whisper_fields(file_path):
    try:
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
    except Exception as e:
        logging.exception(f'Error transcribing with whisper: {e}')
        raise
    
async def update_meili_docs():
    logging.info("Update meilisearch docs.")
    
    reset_deadline()
    
    await sync_meili_docs({
        'tika': does_support_mime_tika,
        'whisper': does_support_mime_whisper
    })
    
    await augment_meili_docs("tika", get_tika_fields)
    await augment_meili_docs("whisper", get_whisper_fields, 1)
    
    logging.info("Done updating meilisearch docs.")
    
async def main():
    init_db()
    await init_meili()
    
    observer_process = Process(target=start_observer)
    observer_process.start()
    
    await update_meili_docs()
    while True:
        await sleep_until_3am()
        await update_meili_docs()

if __name__ == "__main__":
    asyncio.run(main())