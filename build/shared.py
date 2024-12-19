import json
import logging
import magic
import mimetypes
import os
import sqlite3
import time
import xxhash

from modules import scrape_meta, transcribe_meta, read_meta
from multiprocessing import Pool, cpu_count
from pathlib import Path

modules = [scrape_meta, transcribe_meta]

VERSION = 1
ALLOWED_TIME_PER_MODULE = int(os.environ.get("ALLOWED_TIME_PER_MODULE", "3600"))
ARCHIVE_DIRECTORY = os.environ.get("ARCHIVE_DIRECTORY", "/data/archive")
CACHE_FILE_PATH = os.environ.get("CACHE_FILE_PATH", "/data/metadata/cache")
INDEX_DIRECTORY = os.environ.get("INDEX_DIRECTORY", "/data")
MEILISEARCH_BATCH_SIZE = int(os.environ.get("MEILISEARCH_BATCH_SIZE", "10000"))
MEILISEARCH_HOST = os.environ.get("MEILISEARCH_HOST", "http://localhost:7700")
MEILISEARCH_INDEX_NAME = os.environ.get("MEILISEARCH_INDEX_NAME", "files")
METADATA_DIRECTORY = os.environ.get("METADATA_DIRECTORY", "/data/metadata")
RECHECK_TIME_AFTER_COMPLETE = int(os.environ.get("RECHECK_TIME_AFTER_COMPLETE", "1800"))

if not os.path.exists("./data/logs"):
    os.makedirs("./data/logs")
if not os.path.exists(METADATA_DIRECTORY):
    os.makedirs(METADATA_DIRECTORY)

conn = sqlite3.connect(CACHE_FILE_PATH, check_same_thread=False)
conn.execute("PRAGMA journal_mode=WAL")  # Enables WAL mode

def execute_with_retry(cursor, query, params=(), retries=3, delay=0.1):
    for attempt in range(retries):
        try:
            cursor.execute(query, params)
            return
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                time.sleep(delay * (2**attempt))  # Exponential backoff
            else:
                raise
    raise sqlite3.OperationalError("Database is locked after retries.")

def commit_with_retry(conn, retries=3, delay=0.1):
    for attempt in range(retries):
        try:
            conn.commit()
            return
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                time.sleep(delay * (2**attempt))  # Exponential backoff
            else:
                raise
    raise sqlite3.OperationalError("Database is locked after retries.")

cursor = conn.cursor()
execute_with_retry(
    cursor,
    """
    CREATE TABLE IF NOT EXISTS hash_cache (
        file_path TEXT PRIMARY KEY,
        mtime INTEGER,
        hash TEXT
    )
""",
)
commit_with_retry(conn)

def upsert_into_cache(file_path, mtime, file_hash):
    cursor = conn.cursor()
    execute_with_retry(
        cursor,
        """
        INSERT INTO hash_cache (file_path, mtime, hash)
        VALUES (?, ?, ?)
        ON CONFLICT(file_path) DO UPDATE SET mtime=excluded.mtime, hash=excluded.hash
    """,
        (file_path, mtime, file_hash),
    )
    commit_with_retry(conn)

def get_from_cache(file_path):
    cursor = conn.cursor()
    execute_with_retry(
        cursor, "SELECT mtime, hash FROM hash_cache WHERE file_path = ?", (file_path,)
    )
    result = cursor.fetchone()
    if result:
        mtime, file_hash = result
        return {"mtime": mtime, "hash": file_hash}
    else:
        return None

def delete_from_cache(file_path):
    cursor = conn.cursor()
    execute_with_retry(
        cursor, "DELETE FROM hash_cache WHERE file_path = ?", (file_path,)
    )
    commit_with_retry(conn)
    
def move_in_cache(src_file_path, dest_file_path):
    src_data = get_from_cache(src_file_path)
    if not src_data:
        return
    upsert_into_cache(dest_file_path, src_data['mtime'], src_data['hash'])
    delete_from_cache(src_file_path)

def compute_file_hash(file_path, mtime):
    cache_entry = get_from_cache(file_path)
    if cache_entry and cache_entry["mtime"] == mtime:
        # Cache hit
        file_hash = cache_entry["hash"]
    else:
        # Cache miss or mtime mismatch; recompute hash
        logging.debug(f'hashing "{file_path}"')
        hasher = xxhash.xxh64()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hasher.update(chunk)
        file_hash = hasher.hexdigest()
        # Update the cache
        upsert_into_cache(file_path, mtime, file_hash)
    return file_hash

def write_document_json(document_dict):
    doc_id = document_dict["id"]
    doc_path = os.path.join(METADATA_DIRECTORY, doc_id, "document.json")
    metadata_directory_path = os.path.join(METADATA_DIRECTORY, doc_id)
    if not os.path.exists(metadata_directory_path):
        os.makedirs(metadata_directory_path)
    with open(doc_path, "w") as file:
        json.dump(document_dict, file, indent=4, separators=(", ", ": "))
    version_path = os.path.join(metadata_directory_path, "version.json")
    version_data = {"version": VERSION}
    with open(version_path, "w") as version_file:
        json.dump(version_data, version_file, indent=4, separators=(", ", ": "))

def read_document_json(id):
    doc_path = os.path.join(METADATA_DIRECTORY, id, "document.json")
    if not os.path.exists(doc_path):
        return None, False
    with open(doc_path, "r") as file:
        document = json.load(file)
    version_path = os.path.join(METADATA_DIRECTORY, id, "version.json")
    with open(version_path, "r") as version_file:
        version_data = json.load(version_file)
        saved_version = version_data.get("version")
    version_changed = saved_version != VERSION
    return document, version_changed

def gather_file_info(file_path):
    try:
        logging.debug(f'gathering file info for "{file_path}"')
        stat = os.stat(file_path)
        mtime = stat.st_mtime
        file_hash = compute_file_hash(file_path, mtime)
        document, is_version_updated = read_document_json(file_hash)
        file_info = {
            "path": file_path,
            "mtime": mtime,
            "stat": stat,
            "id": file_hash,
            "document": document,
            "is_version_updated": is_version_updated,
        }
        logging.debug(f'file info for "{file_path}": id="{file_hash}", mtime="{mtime}"')
        return file_info
    except FileNotFoundError:
        logging.warning(f'"{file_path}" is now deleted, skipping')
        return None
    except Exception as e:
        logging.exception(f'failed to gather file info for "{file_path}": {e}')
        return None
    
def gather_file_infos(file_paths):
    all_file_infos = []
    with Pool(processes=cpu_count()) as pool:
        all_file_infos = [fi for fi in pool.map(gather_file_info, file_paths) if fi]
    return all_file_infos

def get_mime_type(file_path):
    mime = magic.Magic(mime=True)
    mime_type = mime.from_file(file_path)

    if mime_type == "application/octet-stream":
        mime_type, _ = mimetypes.guess_type(file_path)

        if mime_type is None:
            mime_type = "application/octet-stream"

    return mime_type

def path_from_relative_path(relative_path):
    absolute_path = os.path.join(INDEX_DIRECTORY, relative_path)
    absolute_path = os.path.normpath(absolute_path)
    return absolute_path

def path_from_meili_doc(doc):
    return path_from_relative_path(doc["paths"][0])

def metadata_dir_path_from_doc(doc):
    return Path(METADATA_DIRECTORY) / doc["id"]

def is_in_archive_directory(path):
    try:
        return os.path.commonpath([path, ARCHIVE_DIRECTORY]) == ARCHIVE_DIRECTORY
    except ValueError:
        return False

def handle_document_changed(pdoc, cdoc):
    is_archived = False
    for relative_path in cdoc['paths']:
        if relative_path.startswith(ARCHIVE_DIRECTORY):
            is_archived = True
    cdoc["is_archived"] = is_archived
    
    status = "idle"
    for module in modules:
        if module.handle_document_changed(pdoc, cdoc, path_from_meili_doc(cdoc), metadata_dir_path_from_doc(cdoc)):
            status = module.NAME
            break
    cdoc["status"] = status
    
    write_document_json(cdoc)
    return cdoc

def validate_and_update_document_paths(document):
    valid_paths = set()
    for path in document["paths"]:
        absolute_path = path_from_relative_path(path)
        if is_in_archive_directory(absolute_path) or os.path.exists(absolute_path):
            valid_paths.add(path)
        else:
            delete_from_cache(path)
    document["paths"] = list(valid_paths)
    document["copies"] = len(valid_paths)
    return len(valid_paths) == 0

def get_document_for_hash(doc_id, file_infos):
    try:
        logging.debug(f'processing files with id "{doc_id}"')
        paths = set()
        mtimes = []
        for file_info in file_infos:
            relative_path = os.path.relpath(file_info["path"], INDEX_DIRECTORY)
            paths.add(relative_path)
            mtimes.append(file_info["mtime"])
            logging.debug(
                f'file "{file_info["path"]}" contributes path "{relative_path}" and mtime "{file_info["mtime"]}"'
            )

        max_mtime = max(mtimes)
        sample_file_info = file_infos[0]
        mime_type = get_mime_type(sample_file_info["path"])
        existing_document = sample_file_info.get("document")

        # Construct the updated document
        document = {
            "id": doc_id,
            "mtime": max(
                max_mtime,
                existing_document.get("mtime", 0) if existing_document else 0,
            ),
            "paths": (
                list((paths.union(set(existing_document.get("paths", [])))))
                if existing_document
                else list(paths)
            ),
            "size": sample_file_info["stat"].st_size,
            "type": mime_type,
        }
        
        valid_paths = set()
        for path in document["paths"]:
            absolute_path = path_from_relative_path(path)
            if is_in_archive_directory(absolute_path) or os.path.exists(absolute_path):
                valid_paths.add(path)
        
        if not valid_paths:
            return document, True, True
        
        valid_paths_list = list(valid_paths)
        document["paths"] = valid_paths_list
        document["copies"] = len(valid_paths_list)

        # Check if the document has changed by comparing key fields
        document_changed = (
            existing_document is None
            or document["mtime"] != existing_document.get("mtime")
            or document["size"] != existing_document.get("size")
            or document["type"] != existing_document.get("type")
            or set(document["paths"]) != set(existing_document.get("paths", []))
            or document["copies"] != existing_document.get("copies")
            or sample_file_info["is_version_updated"] == True
        )

        if document_changed:
            document = handle_document_changed(existing_document, document)

        logging.debug(
            f'processed document for id "{doc_id}" with paths "{document["paths"]}"'
        )
        return document, document_changed, False
    except Exception as e:
        logging.warning(f'failed to process documents for id "{doc_id}, file likely deleted or moved"')
        return None, False, True
    