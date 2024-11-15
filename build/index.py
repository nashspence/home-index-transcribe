import asyncio
import datetime
import json
import logging
import magic
import mimetypes
import os
import shutil
import sqlite3
import time
import xxhash

from collections import defaultdict
from itertools import chain
from logging.handlers import TimedRotatingFileHandler
from meilisearch_python_sdk import AsyncClient, Client
from multiprocessing import Process, Pool, cpu_count
from pathlib import Path
from queue import Queue, Empty
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

VERSION = 1

ALLOWED_TIME_PER_MODULE = int(os.environ.get("ALLOWED_TIME_PER_MODULE", "86400"))
ARCHIVE_DIRECTORY = os.environ.get("ARCHIVE_DIRECTORY", "/data/archive")
CACHE_FILE_PATH = os.environ.get("CACHE_FILE_PATH", "/data/metadata/cache")
DOMAIN = os.environ.get("DOMAIN", "localhost")
INDEX_DIRECTORY = os.environ.get("INDEX_DIRECTORY", "/data")
MEILISEARCH_BATCH_SIZE = int(os.environ.get("MEILISEARCH_BATCH_SIZE", "10000"))
MEILISEARCH_HOST = os.environ.get("MEILISEARCH_HOST", "http://localhost:7700")
MEILISEARCH_INDEX_NAME = os.environ.get("MEILISEARCH_INDEX_NAME", "files")
METADATA_DIRECTORY = os.environ.get("METADATA_DIRECTORY", "/data/metadata")
RECHECK_TIME_AFTER_COMPLETE = int(os.environ.get("RECHECK_TIME_AFTER_COMPLETE", "3600"))

if not os.path.exists("./data/logs"):
    os.makedirs("./data/logs")
if not os.path.exists(METADATA_DIRECTORY):
    os.makedirs(METADATA_DIRECTORY)

import scrape_file_bytes

modules = [scrape_file_bytes]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    handlers=[
        TimedRotatingFileHandler(
            "./data/logs/indexer.log",
            when="midnight",
            interval=1,
            backupCount=7,
            atTime=datetime.time(2, 30)
        ),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger('asyncio')
logger.setLevel(logging.WARNING)
file_handler = logging.FileHandler(f"./data/logs/asyncio.log")
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(file_handler)
logger.propagate = False

logger = logging.getLogger('httpcore.connection')
logger.setLevel(logging.WARNING)
file_handler = logging.FileHandler(f"./data/logs/httpcore.connection.log")
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(file_handler)
logger.propagate = False

logger = logging.getLogger('httpcore.http11')
logger.setLevel(logging.WARNING)
file_handler = logging.FileHandler(f"./data/logs/httpcore.http11.log")
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(file_handler)
logger.propagate = False

logger = logging.getLogger('httpx')
logger.setLevel(logging.WARNING)
file_handler = logging.FileHandler(f"./data/logs/httpx.log")
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(file_handler)
logger.propagate = False
    
logger = logging.getLogger('watchdog')
logger.setLevel(logging.WARNING)
file_handler = logging.FileHandler(f"./data/logs/watchdog.log")
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(file_handler)
logger.propagate = False

logger = logging.getLogger('watchdog-process')
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(f"./data/logs/watchdog-process.log")
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(file_handler)
logger.propagate = False

client = None
index = None

async def init_meili():
    global client, index
    logging.debug(f"meili init")
    client = AsyncClient(MEILISEARCH_HOST)
    try:
        index = await client.get_index(MEILISEARCH_INDEX_NAME)
    except Exception as e:
        if getattr(e, 'code', None) == "index_not_found":
            try:
                logging.info(f'meili create index "{MEILISEARCH_INDEX_NAME}"')
                index = await client.create_index(MEILISEARCH_INDEX_NAME, primary_key="id")
            except Exception:
                logging.exception(f'meile create index failed "{MEILISEARCH_INDEX_NAME}"')
                raise
        else:
            logging.exception(f"meili init failed")
            raise

    filterable_attributes = [
        "id", 
        "type", 
        "modified_date", 
        "size",
        "urls",
    ] + list(chain(*[module.FILTERABLE_FIELD_NAMES for module in modules]))
    
    try:
        logging.debug(f"meili update index attrs")
        await index.update_filterable_attributes(filterable_attributes)
        await index.update_sortable_attributes(["modified_date", "size"] + list(chain(*[module.SORTABLE_FIELD_NAMES for module in modules])))
    except Exception:
        logging.exception(f"meili update index attrs failed")
        raise

async def get_document_count():
    if not index:
        raise Exception("meili index did not init")
    
    try:
        stats = await index.get_stats()
        return stats.number_of_documents
    except Exception:
        logging.exception(f"meili get stats failed")
        raise

async def add_or_update_document(doc):
    if not index:
        raise Exception("meili index did not init")
    
    if doc:
        try:
            await index.update_documents([doc])
        except Exception:
            logging.exception(f'meili index.update_documents failed for arg "{[doc]}"')
            raise

async def add_or_update_documents(docs):
    if not index:
        raise Exception("meili index did not init")
    
    if docs:
        try:
            for i in range(0, len(docs), MEILISEARCH_BATCH_SIZE):
                batch = docs[i:i + MEILISEARCH_BATCH_SIZE]
                await index.update_documents(batch)
        except Exception:
            logging.exception(f"meili update documents failed")
            raise

async def delete_documents_by_id(ids):
    if not index:
        raise Exception("meili index did not init")
    
    try:
        if ids:
            for i in range(0, len(ids), MEILISEARCH_BATCH_SIZE):
                batch = ids[i:i + MEILISEARCH_BATCH_SIZE]
                await index.delete_documents(ids=batch)
    except Exception:
        logging.exception(f"meili delete documents failed")
        raise

async def get_document(doc_id):
    if not index:
        raise Exception("meili index did not init")
    
    try:
        doc = await index.get_document(doc_id)
        return doc
    except Exception:
        logging.exception(f"meili get document failed")
        raise

async def get_all_documents():
    if not index:
        raise Exception("meili index did not init")
    
    docs = []
    offset = 0
    limit = MEILISEARCH_BATCH_SIZE
    try:
        while True:
            result = await index.get_documents(offset=offset, limit=limit)
            docs.extend(result.results)
            if len(result.results) < limit:
                break
            offset += limit
        return docs
    except Exception:
        logging.exception(f"meili get documents failed")
        raise

async def wait_for_meili_idle():
    if not client:
        raise Exception("meili index did not init")
    
    try:
        while True:
            tasks = await client.get_tasks()
            active_tasks = [task for task in tasks.results if task.status in ['enqueued', 'processing']]
            if len(active_tasks) == 0:
                break
            await asyncio.sleep(1)
    except Exception:
        logging.exception(f"meili wait for idle failed")
        raise

def get_mime_type(file_path):
    mime = magic.Magic(mime=True)
    mime_type = mime.from_file(file_path)
    
    if mime_type == 'application/octet-stream':
        mime_type, _ = mimetypes.guess_type(file_path)
        
        if mime_type is None:
            mime_type = 'application/octet-stream'
    
    return mime_type

def write_document_json(document_dict):
    doc_id = document_dict['id']
    doc_path = os.path.join(METADATA_DIRECTORY, doc_id, 'document.json')
    metadata_directory_path = os.path.join(METADATA_DIRECTORY, doc_id)
    if not os.path.exists(metadata_directory_path):
        os.makedirs(metadata_directory_path)
    with open(doc_path, 'w') as file:
        json.dump(document_dict, file, indent=4, separators=(", ", ": "))
    version_path = os.path.join(metadata_directory_path, 'version.json')
    version_data = {
        "version": VERSION
    }
    with open(version_path, 'w') as version_file:
        json.dump(version_data, version_file, indent=4, separators=(", ", ": "))

def read_document_json(id):
    doc_path = os.path.join(METADATA_DIRECTORY, id, 'document.json')
    try:
        with open(doc_path, 'r') as file:
            document = json.load(file)
    except Exception as e:
        logging.warning(e)
        return None, False
    version_path = os.path.join(METADATA_DIRECTORY, id, 'version.json')
    try:
        with open(version_path, 'r') as version_file:
            version_data = json.load(version_file)
            saved_version = version_data.get("version")
    except Exception as e:
        saved_version = None
    version_changed = saved_version != VERSION
    return document, version_changed

def path_from_meili_doc(doc):
    return Path(doc['urls'][0].replace(f"https://{DOMAIN}/", f"{INDEX_DIRECTORY}/")).as_posix()

def relative_path_from_url(url):
    return Path(url.replace(f"https://{DOMAIN}/", "")).as_posix()

def url_from_relative_path(relative_path):
    return f"https://{DOMAIN}/{relative_path}"

connection_pool = Queue(1)
for _ in range(1):
    conn = sqlite3.connect(CACHE_FILE_PATH, check_same_thread=False)
    conn.execute('PRAGMA journal_mode=WAL')  # Enables WAL mode
    connection_pool.put(conn)

def get_connection():
    try:
        return connection_pool.get(timeout=10)
    except Empty:
        raise Exception("Connection pool exhausted")

def return_connection(conn):
    connection_pool.put(conn)

def execute_with_retry(cursor, query, params=(), retries=3, delay=0.1):
    for attempt in range(retries):
        try:
            cursor.execute(query, params)
            return
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                time.sleep(delay * (2 ** attempt))  # Exponential backoff
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
                time.sleep(delay * (2 ** attempt))  # Exponential backoff
            else:
                raise
    raise sqlite3.OperationalError("Database is locked after retries.")

def init_hash_cache_sqlite3():
    conn = get_connection()
    try:
        cursor = conn.cursor()
        execute_with_retry(cursor, '''
            CREATE TABLE IF NOT EXISTS hash_cache (
                file_path TEXT PRIMARY KEY,
                mtime INTEGER,
                hash TEXT
            )
        ''')
        commit_with_retry(conn)
    finally:
        return_connection(conn)

def upsert_into_cache(file_path, mtime, file_hash):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        execute_with_retry(cursor, '''
            INSERT INTO hash_cache (file_path, mtime, hash)
            VALUES (?, ?, ?)
            ON CONFLICT(file_path) DO UPDATE SET mtime=excluded.mtime, hash=excluded.hash
        ''', (file_path, mtime, file_hash))
        commit_with_retry(conn)
    finally:
        return_connection(conn)

def get_from_cache(file_path):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        execute_with_retry(cursor, 'SELECT mtime, hash FROM hash_cache WHERE file_path = ?', (file_path,))
        result = cursor.fetchone()
        if result:
            mtime, file_hash = result
            return {'mtime': mtime, 'hash': file_hash}
        else:
            return None
    finally:
        return_connection(conn)

def delete_from_cache(file_path):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        execute_with_retry(cursor, 'DELETE FROM hash_cache WHERE file_path = ?', (file_path,))
        commit_with_retry(conn)
    finally:
        return_connection(conn)
    logging.debug(f'Deleted cache entry for file "{file_path}"')

def compute_file_hash(file_path, mtime):
    cache_entry = get_from_cache(file_path)
    if cache_entry and cache_entry['mtime'] == mtime:
        # Cache hit
        file_hash = cache_entry['hash']
    else:
        # Cache miss or mtime mismatch; recompute hash
        logging.debug(f'hashing "{file_path}"')
        hasher = xxhash.xxh64()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                hasher.update(chunk)
        file_hash = hasher.hexdigest()
        # Update the cache
        upsert_into_cache(file_path, mtime, file_hash)
    return file_hash

def gather_file_info(file_path):
    try:
        logging.debug(f'gathering file info for "{file_path}"')
        stat = os.stat(file_path)
        mtime = stat.st_mtime
        file_hash = compute_file_hash(file_path, mtime)
        document, is_version_updated = read_document_json(file_hash)
        file_info = {
            'path': file_path,
            'mtime': mtime,
            'stat': stat,
            'id': file_hash,
            'document': document,
            'is_version_updated': is_version_updated
        }
        logging.debug(f'file info for "{file_path}": id="{file_hash}", mtime="{mtime}"')
        return file_info
    except Exception as e:
        logging.exception(f'failed to gather file info for "{file_path}": {e}')
        return None

def get_document_for_hash(doc_id, file_infos):
    try:
        logging.debug(f'processing files with id "{doc_id}"')
        urls = set()
        mtimes = []
        for file_info in file_infos:
            relative_path = os.path.relpath(file_info['path'], INDEX_DIRECTORY)
            url = url_from_relative_path(relative_path)
            urls.add(url)
            mtimes.append(file_info['mtime'])
            logging.debug(f'file "{file_info["path"]}" contributes url "{url}" and mtime "{file_info["mtime"]}"')

        max_mtime = max(mtimes)
        sample_file_info = file_infos[0]
        mime_type = get_mime_type(sample_file_info['path'])
        existing_document = sample_file_info.get('document')

        # Construct the updated document
        document = {
            'id': doc_id,
            'modified_date': max(max_mtime, existing_document.get('modified_date', 0) if existing_document else 0),
            'size': sample_file_info['stat'].st_size,
            'type': mime_type,
            'urls': list(urls.union(set(existing_document.get('urls', [])))) if existing_document else list(urls)
        }

        # Check if the document has changed by comparing key fields
        document_changed = (
            existing_document is None or
            document['modified_date'] != existing_document.get('modified_date') or
            document['size'] != existing_document.get('size') or
            document['type'] != existing_document.get('type') or
            set(document['urls']) != set(existing_document.get('urls', [])) or
            sample_file_info['is_version_updated'] == True
        )
        
        if document_changed:
            write_document_json(document)

        logging.debug(f'processed document for id "{doc_id}" with urls "{document["urls"]}"')
        return document, document_changed
    except Exception as e:
        logging.exception(f'failed to process documents for id "{doc_id}": {e}')
        return None, False

def process_metadata_directory(current_doc_ids, existing_docs_by_id, documents_to_add_or_update, documents_to_delete_ids):
    restored_doc_ids = set()
    logging.debug('processing metadata directory')

    if not os.path.exists(METADATA_DIRECTORY):
        logging.debug(f'metadata directory "{METADATA_DIRECTORY}" does not exist')
        return restored_doc_ids

    for entry in os.scandir(METADATA_DIRECTORY):
        if entry.is_dir(follow_symlinks=False):
            doc_id = entry.name
            document_json_path = os.path.join(entry.path, 'document.json')
            if os.path.isfile(document_json_path):
                with open(document_json_path, 'r') as f:
                    document = json.load(f)
                if doc_id in current_doc_ids:
                    continue
                if doc_id not in existing_docs_by_id:
                    restored = restore_document_from_metadata(doc_id, document, documents_to_add_or_update, documents_to_delete_ids)
                    if restored:
                        restored_doc_ids.add(doc_id)
    return restored_doc_ids

def restore_document_from_metadata(doc_id, document, documents_to_add_or_update, documents_to_delete_ids):
    urls = document.get('urls', [])
    restored = False
    logging.debug(f'restoring document "{doc_id}" from metadata with urls "{urls}"')

    for url in urls:
        relative_path = relative_path_from_url(url)
        file_path = os.path.join(INDEX_DIRECTORY, relative_path)
        if os.path.exists(file_path):
            file_info = gather_file_info(file_path)
            if file_info:
                # Update document with current file information
                updated_document, _ = get_document_for_hash(doc_id, [file_info])
                if updated_document:
                    documents_to_add_or_update.append(updated_document)
                    restored = True
                    logging.debug(f'updated document "{doc_id}" with file "{file_path}"')
        else:
            if relative_path.startswith(ARCHIVE_DIRECTORY):
                # Restore the document
                documents_to_add_or_update.append(document)
                restored = True
                logging.debug(f'document "{doc_id}" already exists, restored from metadata')
            else:
                # Delete the metadata entry
                metadata_entry_path = os.path.join(METADATA_DIRECTORY, doc_id)
                if os.path.exists(metadata_entry_path):
                    shutil.rmtree(metadata_entry_path)
                    logging.debug(f'marked "{doc_id}" for deletion as file "{file_path}" does not exist')
                documents_to_delete_ids.add(doc_id)
    return restored

def filter_documents_to_delete(documents_to_delete_ids, existing_docs_by_id):
    filtered_ids = set()
    for doc_id in documents_to_delete_ids:
        document = existing_docs_by_id.get(doc_id)
        if document:
            urls = document.get('urls', [])
            for url in urls:
                relative_path = relative_path_from_url(url)
                if not relative_path.startswith(ARCHIVE_DIRECTORY):
                    filtered_ids.add(doc_id)
                    logging.debug(f'document "{doc_id}" marked for deletion')
                    break
    return filtered_ids

async def delete_documents(doc_ids):
    if doc_ids:
        await delete_documents_by_id(list(doc_ids))
        await wait_for_meili_idle()
        for doc_id in doc_ids:
            metadata_entry_path = os.path.join(METADATA_DIRECTORY, doc_id)
            if os.path.exists(metadata_entry_path):
                shutil.rmtree(metadata_entry_path)
                logging.debug(f'deleted document "{doc_id}"')
            delete_from_cache(doc_id)
    else:
        logging.debug('no documents to delete')
        
def get_file_list():
    dirs_with_mtime = []
    for metadata_dir_path in Path(METADATA_DIRECTORY).iterdir():
        if metadata_dir_path.is_dir() and (metadata_dir_path / "document.json").exists():
            with open(metadata_dir_path / "document.json", 'r') as file:
                document = json.load(file)
                file_path = path_from_meili_doc(document)
            mtime = os.stat(file_path).st_mtime
            dirs_with_mtime.append((file_path, document, metadata_dir_path, mtime))
    dirs_with_mtime.sort(key=lambda x: x[3], reverse=True)
    return dirs_with_mtime

async def augment_documents(module):
    try:
        logging.info(f"Initializing {module.NAME}")
        await module.init()
    except Exception as e:
        logging.exception(f"{module.NAME} initialization failed: {e}")
        return
    try:
        cancel_event = asyncio.Event()
        async def document_update_handler():
            async for document in module.process_files(get_file_list(), cancel_event):
                if document is None:
                    continue
                try:
                    await add_or_update_document(document)
                    write_document_json(document)
                except Exception as e:
                    logging.exception(f'{module.NAME} updating meili for document "{document}" failed: {e} ')        
        task = asyncio.create_task(document_update_handler())
        try:
            await asyncio.wait_for(task, timeout=ALLOWED_TIME_PER_MODULE)
        except asyncio.TimeoutError:
            logging.info(f"{module.NAME} timed out, cancelling...")
            cancel_event.set()
            await task
    except Exception as e:
        logging.exception(f"{module.NAME} processing failed: {e}")
    finally:
        try:
            logging.info(f"Cleaning up {module.NAME}")
            await module.cleanup()
        except Exception as e:
            logging.exception(f"{module.NAME} cleanup failed: {e}")

async def sync_documents():
    logging.debug('get all documents from meili')
    existing_documents = await get_all_documents()
    logging.info(f'meili returned "{len(existing_documents)}" total documents')
    existing_docs_by_id = {doc['id']: doc for doc in existing_documents}
    existing_doc_ids = set(existing_docs_by_id.keys())

    current_doc_ids = set()
    documents_to_delete_ids = set()
    documents_to_add_or_update = []
    restored_doc_ids = set()

    logging.info(f'recusively scan all files in {DOMAIN}')
    file_paths = []
    for root, dirs, files in os.walk(INDEX_DIRECTORY):
        dirs[:] = [d for d in dirs if os.path.join(root, d) != METADATA_DIRECTORY]
        for filename in files:
            file_path = os.path.join(root, filename)
            file_paths.append(file_path)
            
    logging.info(f'check file hashes, compute if necessary (could take a long time...)')    
    with Pool(processes=cpu_count()) as pool:
        all_file_infos = [fi for fi in pool.map(gather_file_info, file_paths) if fi]

    # Group files by hash (id)
    file_infos_by_hash = defaultdict(list)
    for file_info in all_file_infos:
        file_infos_by_hash[file_info['id']].append(file_info)

    logging.info('creating/adjusting documents for files found')
    semaphore = asyncio.Semaphore(48)
    async def process_document(hash, file_infos):
        async with semaphore:
            current_doc_ids.add(hash)
            document, is_document_changed = await asyncio.to_thread(get_document_for_hash, hash, file_infos)
            if document and is_document_changed:
                documents_to_add_or_update.append(document)
                logging.debug(f'queued document "{hash}" for add/update')
    await asyncio.gather(*(process_document(hash, file_infos) for hash, file_infos in file_infos_by_hash.items()))

    # Determine documents to delete
    documents_to_delete_ids = existing_doc_ids - current_doc_ids

    logging.info(f'checking "{METADATA_DIRECTORY}" against found file and index')
    restored_doc_ids.update(process_metadata_directory(
        current_doc_ids, existing_docs_by_id, documents_to_add_or_update, documents_to_delete_ids))

    documents_to_delete_filtered = filter_documents_to_delete(documents_to_delete_ids, existing_docs_by_id)

    if len(documents_to_add_or_update) > 0 or len(documents_to_delete_filtered) > 0:
        logging.info(f'sending updates to meili')
        if len(documents_to_delete_filtered) > 0:
            await delete_documents(documents_to_delete_filtered)
        if len(documents_to_add_or_update) > 0:
            await add_or_update_documents(documents_to_add_or_update)
        logging.info(f'waiting for meili')
        await wait_for_meili_idle()

    if len(restored_doc_ids) > 0:
        logging.info(f'restored {len(restored_doc_ids)} documents from "{METADATA_DIRECTORY}"')
    if len(documents_to_add_or_update) > 0:
        logging.info(f'added or updated {len(documents_to_add_or_update)} documents')
    if len(documents_to_delete_filtered) > 0:
        logging.info(f'deleted {len(documents_to_delete_filtered)} documents')
        
    total_documents = await get_document_count()
    logging.info(f'total documents {total_documents}')
     
class EventHandler(FileSystemEventHandler):
    def __init__(self):
        self.meili_client = Client(MEILISEARCH_HOST)
        self.index = self.meili_client.index(MEILISEARCH_INDEX_NAME)

    def should_ignore(self, path):
        return os.path.commonpath([path, METADATA_DIRECTORY]) == METADATA_DIRECTORY

    def on_created(self, event):
        if not event.is_directory and not self.should_ignore(event.src_path):
            try:
                logging.info(f'create "{event.src_path}"')
                self.add_or_update_document(event.src_path)
            except Exception:
                logging.exception(f'create failed "{event.src_path}"')

    def on_modified(self, event):
        if not event.is_directory and not self.should_ignore(event.src_path):
            try:
                logging.info(f'modify "{event.src_path}"')
                self.add_or_update_document(event.src_path)
            except Exception:
                logging.exception(f'modify failed "{event.src_path}"')

    def on_deleted(self, event):
        if not event.is_directory and not self.should_ignore(event.src_path):
            try:
                logging.info(f'delete "{event.src_path}"')
                self.delete_document(event.src_path)
            except Exception:
                logging.exception(f'delete failed "{event.src_path}"')

    def on_moved(self, event):
        if not event.is_directory and not (self.should_ignore(event.src_path) or self.should_ignore(event.dest_path)):
            try:
                logging.info(f'move "{event.src_path}" -> "{event.dest_path}"')
                self.delete_document(event.src_path)
                self.add_or_update_document(event.dest_path)
            except Exception:
                logging.exception(f'move failed "{event.src_path}" -> "{event.dest_path}"')

    def add_or_update_document(self, file_path):
        file_info = gather_file_info(file_path)
        if file_info:
            document, is_document_changed = get_document_for_hash(file_info["id"], [file_info])
            if document and is_document_changed:
                self.index.update_documents([document])
                logging.debug(f'added or updated document with id "{document["id"]}" in meilisearch')
        else:
            logging.debug(f'failed to gather file info for "{file_path}"')

    def delete_document(self, file_path):
        stat = os.stat(file_path)
        relative_path = os.path.relpath(file_path, INDEX_DIRECTORY)
        if relative_path.startswith(ARCHIVE_DIRECTORY):
            return
        hash = compute_file_hash(file_path, stat.st_mtime)
        if hash:
            metadata_entry_path = os.path.join(METADATA_DIRECTORY, hash)
            if os.path.exists(metadata_entry_path):
                shutil.rmtree(metadata_entry_path)
                logging.debug(f'deleted metadata for document "{hash}"')
                self.index.delete_document(hash)
                logging.debug(f'deleted document with id "{hash}" from meilisearch')
                delete_from_cache(file_path)
        else:
            logging.debug(f'no cache entry for file "{file_path}", cannot delete document')
        
def process_main():
    logger = logging.getLogger('watchdog-process')
    logging.root = logger
    logging.getLogger().handlers = logger.handlers

    try:
        logging.info(f'watchdog init')
        observer = Observer()
        handler = EventHandler()
        observer.schedule(handler, INDEX_DIRECTORY, recursive=True)
        observer.start()
        observer.join()
    except Exception:
        logging.exception(f'watchdog init failed')
    
async def begin_indexing():
    process = Process(target=process_main)
    process.start()    
    logging.info(f'watchdog process started')
    await sync_documents()
    
    while True:
        start_time = time.time()
        for module in modules:
            await augment_documents(module)
        elapsed_time = time.time() - start_time
        await wait_for_meili_idle()
        if elapsed_time < 60:
            await asyncio.sleep(RECHECK_TIME_AFTER_COMPLETE)
           
async def main():
    init_hash_cache_sqlite3()
    await init_meili()
    await begin_indexing()

if __name__ == "__main__":
    asyncio.run(main())