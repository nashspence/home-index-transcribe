import asyncio
import datetime
import json
import logging
import magic
import math
import mimetypes
import os
import tempfile
import time
import uuid

from itertools import chain
from logging.handlers import TimedRotatingFileHandler
from meilisearch_python_sdk import AsyncClient, Client
from multiprocessing import Process
from pathlib import Path
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

VERSION = 1
ALLOWED_TIME_PER_MODULE = int(os.environ.get("ALLOWED_TIME_PER_MODULE", "86400"))
DOMAIN = os.environ.get("DOMAIN", "private.0819870.xyz")
MEILISEARCH_BATCH_SIZE = int(os.environ.get("MEILISEARCH_BATCH_SIZE", "10000"))
MEILISEARCH_HOST = os.environ.get("MEILISEARCH_HOST", "http://meilisearch:7700")
MEILISEARCH_INDEX_NAME = os.environ.get("MEILISEARCH_INDEX_NAME", "files")
OS_DIRECTORY_TO_INDEX = os.environ.get("OS_DIRECTORY_TO_INDEX", "/data")
METADATA_DATABASE_DIRECTORY = os.environ.get("METADATA_DATABASE_DIRECTORY", "/data/metadata")
RECHECK_TIME_AFTER_COMPLETE = int(os.environ.get("RECHECK_TIME_AFTER_COMPLETE", "3600"))
DOCUMENT_ID_XATTR_FIELD = "user.0819870_xyz.index_id"

if not os.path.exists("./data/logs"):
    os.makedirs("./data/logs")
if not os.path.exists(METADATA_DATABASE_DIRECTORY):
    os.makedirs(METADATA_DATABASE_DIRECTORY)

import scrape_file_bytes
import transcribe_audio

modules = [scrape_file_bytes, transcribe_audio]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
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
        "name",
        "url",
    ] + list(chain(*[module.FILTERABLE_FIELD_NAMES for module in modules]))
    
    try:
        logging.debug(f"meili update index attrs")
        await index.update_filterable_attributes(filterable_attributes)
        await index.update_sortable_attributes(["modified_date", "size"] + list(chain(*[module.SORTABLE_FIELD_NAMES for module in modules])))
    except Exception:
        logging.exception(f"meili update index attrs failed")
        raise

async def get_doc_count_meili():
    if not index:
        raise Exception("meili index did not init")
    
    try:
        stats = await index.get_stats()
        return stats.number_of_documents
    except Exception:
        logging.exception(f"meili get stats failed")
        raise

async def add_or_update_doc_meili(doc):
    if not index:
        raise Exception("meili index did not init")
    
    if doc:
        try:
            await index.update_documents([doc])
        except Exception:
            logging.exception(f"meili update documents failed")
            raise

async def add_or_update_docs_meili(docs):
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

async def delete_docs_by_id_meili(ids):
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

async def get_doc_meili(doc_id):
    if not index:
        raise Exception("meili index did not init")
    
    try:
        doc = await index.get_document(doc_id)
        return doc
    except Exception:
        logging.exception(f"meili get document failed")
        raise

async def get_all_docs_meili():
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

async def wait_for_idle_meili():
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

def get_mime_magic(file_path):
    mime = magic.Magic(mime=True)
    mime_type = mime.from_file(file_path)
    
    if mime_type == 'application/octet-stream':
        mime_type, _ = mimetypes.guess_type(file_path)
        
        if mime_type is None:
            mime_type = 'application/octet-stream'
    
    return mime_type

XATTR_SUPPORTED = None

def check_xattr_supported():
    global XATTR_SUPPORTED
    if XATTR_SUPPORTED is None:
        try:
            with tempfile.NamedTemporaryFile() as temp_file:
                test_attr_name = "user.test"
                os.setxattr(temp_file.name, test_attr_name, b"test")
                os.removexattr(temp_file.name, test_attr_name)
            XATTR_SUPPORTED = True
        except (AttributeError, OSError):
            XATTR_SUPPORTED = False
            logging.warning("xattr operations are not supported on this system.")

check_xattr_supported()

def read_document_id_from_xattr(file_path):
    if not XATTR_SUPPORTED:
        return None
    try:
        data = os.getxattr(file_path, DOCUMENT_ID_XATTR_FIELD)
        document_id = data.decode('utf-8')
        return document_id
    except OSError:
        return None
    except Exception as e:
        logging.exception(f"Failed to read document ID from xattr on {file_path}: {e}")
        return None

def write_document_id_to_xattr(file_path, document_id):
    if not XATTR_SUPPORTED:
        return
    try:
        data = document_id.encode('utf-8')
        os.setxattr(file_path, DOCUMENT_ID_XATTR_FIELD, data)
    except Exception as e:
        logging.exception(f"Failed to write document ID to xattr on {file_path}: {e}")

def read_versions_json_for_file_path(file_path):
    id = read_document_id_from_xattr(file_path)
    
    try:
        with open(f'{METADATA_DATABASE_DIRECTORY}/{id}/version.json', 'r') as file:
            return json.load(file)
    except Exception as e:
        return None
    
def write_versions_json_for_file_path(file_path, dict):
    id = read_document_id_from_xattr(file_path)
    
    metadata_directory_path = f'{METADATA_DATABASE_DIRECTORY}/{id}'
    if not os.path.exists(metadata_directory_path):
        os.makedirs(metadata_directory_path)
    
    if id:
        with open(f'{METADATA_DATABASE_DIRECTORY}/{id}/version.json', 'w') as file:
            return json.dump(dict, file, indent=4, separators=(", ", ": "))
    else:
        raise Exception("no id available")
    
def read_document_json_for_file_path(file_path):
    id = read_document_id_from_xattr(file_path)
    try:
        with open(f'{METADATA_DATABASE_DIRECTORY}/{id}/document.json', 'r') as file:
            return json.load(file)
    except Exception as e:
        return None
    
def write_document_json_for_file_path(file_path, dict):
    id = read_document_id_from_xattr(file_path)
    
    metadata_directory_path = f'{METADATA_DATABASE_DIRECTORY}/{id}'
    if not os.path.exists(metadata_directory_path):
        os.makedirs(metadata_directory_path)
    
    if id:
        with open(f'{METADATA_DATABASE_DIRECTORY}/{id}/document.json', 'w') as file:
            return json.dump(dict, file, indent=4, separators=(", ", ": "))
    else:
        raise Exception("no id available")

def get_file_path_from_meili_doc(doc):
    return Path(doc['url'].replace(f"https://{DOMAIN}/", f"{OS_DIRECTORY_TO_INDEX}/")).as_posix()

def get_relative_path_from_meili_doc(doc):
    return Path(doc['url'].replace(f"https://{DOMAIN}/", "")).as_posix()

def get_url_from_relative_path(relative_path):
    return f"https://{DOMAIN}/{relative_path}"

def get_relative_path_from_file_path(file_path):
    return Path(file_path).relative_to(OS_DIRECTORY_TO_INDEX).as_posix()

async def sync_meili_docs():
    expected_documents = await get_all_docs_meili()
    expected_documents_by_id = {doc["id"]: doc for doc in expected_documents}
    previous_file_paths = {get_file_path_from_meili_doc(doc) for doc in expected_documents}
    current_file_paths = set()
    added_document_ids = set()
    restored_document_ids = set()
    updated_document_ids = set()
    create_document_tasks = []
    added_or_updated_documents = []

    logging.info(f"meili has {len(expected_documents)}")
    
    doc_creation_semaphore = asyncio.Semaphore(32)
    async def async_create_meili_doc_from_file_path(file_path):
        async with doc_creation_semaphore:
            return await asyncio.to_thread(create_meili_doc_from_file_path, file_path)

    directory_stack = [OS_DIRECTORY_TO_INDEX]

    while directory_stack:
        dir_path = directory_stack.pop()
        try:
            entries = await asyncio.to_thread(lambda: list(os.scandir(dir_path)))
            for entry in entries:
                if entry.is_dir(follow_symlinks=False):
                    if entry.path != METADATA_DATABASE_DIRECTORY:
                        directory_stack.append(entry.path)
                elif entry.is_file(follow_symlinks=False):
                    id = read_document_id_from_xattr(entry.path)
                    entry_path = Path(entry.path)
                    path_mtime = entry_path.stat().st_mtime
                    document = read_document_json_for_file_path(entry.path)
                    version = read_versions_json_for_file_path(entry.path)
                    if (
                        id and
                        document and 
                        version and 
                        id not in expected_documents_by_id and
                        math.isclose(document["modified_date"], path_mtime, abs_tol=1e-7)
                    ):
                        restored_document_ids.add(id)
                        added_or_updated_documents.append(document)
                    else:
                        task = asyncio.create_task(async_create_meili_doc_from_file_path(entry.path))
                        create_document_tasks.append(task)
                        if not id or id not in expected_documents_by_id:
                            added_document_ids.add(id)
                        elif not math.isclose(expected_documents_by_id[id]["modified_date"], path_mtime, abs_tol=1e-7):
                            updated_document_ids.add(id)
                    current_file_paths.add(entry.path)
        except Exception:
            logging.exception(f'os scan directory failed "{dir_path}"')

    if len(create_document_tasks) > 0:
        done, _ = await asyncio.wait(create_document_tasks)
        added_or_updated_documents = added_or_updated_documents.extend([task.result() for task in done if task.result() is not None])

    deleted_file_paths = previous_file_paths - current_file_paths
    deleted_document_ids = {read_document_id_from_xattr(fp) for fp in deleted_file_paths}

    await delete_docs_by_id_meili(list(deleted_document_ids | updated_document_ids))
    await wait_for_idle_meili()
    await add_or_update_docs_meili(added_or_updated_documents)
    await wait_for_idle_meili()

    total_expected_documents = await get_doc_count_meili()
    logging.info(f'meili restored {len(restored_document_ids)} from "{METADATA_DATABASE_DIRECTORY}"')
    logging.info(f"meili added {len(added_document_ids)}")
    logging.info(f"meili updated {len(updated_document_ids)}")
    logging.info(f"meili deleted {len(deleted_document_ids)}")
    logging.info(f"meili now has {total_expected_documents}")
    
def create_meili_doc_from_file_path(file_path):
    try:
        path = Path(file_path)
        relative_path = path.relative_to(OS_DIRECTORY_TO_INDEX).as_posix()
        stat = path.stat()
        current_mtime = stat.st_mtime
        mime_type = get_mime_magic(path)

        doc_id = read_document_id_from_xattr(file_path)
        if not doc_id:
            doc_id = str(uuid.uuid4())
            write_document_id_to_xattr(file_path, doc_id)
        
        document = {
            "id": doc_id,
            "name": path.name,
            "modified_date": current_mtime,
            "size": stat.st_size,
            "type": mime_type,
            "url": get_url_from_relative_path(relative_path),
        }
        
        version = { "version": VERSION }
        
        write_document_json_for_file_path(file_path, document)
        write_versions_json_for_file_path(file_path, version)

        return document
    except Exception:
        logging.exception(f'os create doc failed "{file_path}"')
        return None

async def augment_meili_docs(module):
    try:
        logging.debug(f"init {module.NAME}")
        await module.init()
    except Exception:
        logging.exception(f"{module.NAME} init failed")
        return

    try:
        semaphore = asyncio.Semaphore(32)

        async def sem_task(file_path, metadata_dir_path, document):
            async with semaphore:
                return await augment_meili_doc_from_file_path(file_path, metadata_dir_path, document, module)
            
        tasks_dict = {}
        tasks_list = []
        
        dirs_with_mtime = []
        for metadata_dir_path in Path(METADATA_DATABASE_DIRECTORY).iterdir():
            if metadata_dir_path.is_dir() and (metadata_dir_path / "document.json").exists():
                mtime = os.stat(metadata_dir_path / "document.json").st_mtime
                with open(metadata_dir_path / "document.json", 'r') as file:
                    document = json.load(file)
                dirs_with_mtime.append((metadata_dir_path, document, mtime))

        dirs_with_mtime.sort(key=lambda x: x[2], reverse=True)

        for metadata_dir_path, document, _ in dirs_with_mtime:
            file_path = get_file_path_from_meili_doc(document)
            task = asyncio.create_task(sem_task(file_path, metadata_dir_path, document))
            tasks_dict[file_path] = task
            tasks_list.append(task)
        
        _, postponed_tasks = await asyncio.wait(tasks_list, timeout=ALLOWED_TIME_PER_MODULE)
        
        if len(postponed_tasks) > 0:
            logging.info(f"{module.NAME} will yield")
            for postponed_task in postponed_tasks:
                postponed_task.cancel()
            
        success = []
        failure = []
        not_found = []
        postponed = []
        up_to_date = []
            
        for file_path, task in tasks_dict.items():
            try:
                result = await task
                if result == "success":
                    success.append(file_path)
                elif result == "up-to-date":
                    up_to_date.append(file_path)
                elif result == "not found":
                    not_found.append(file_path)
            except asyncio.CancelledError:
                postponed.append(file_path)
            except Exception:
                logging.exception( f"{module.NAME} failed at {file_path}")
                failure.append(file_path)
        
        if len(up_to_date) > 0:
            logging.info( f"{module.NAME} {len(up_to_date)} files already up-to-date")
        if len(success) > 0:
            logging.info( f"{module.NAME} {len(success)} files update succeeded")
        if len(postponed) > 0:
            logging.info( f"{module.NAME} {len(postponed)} files update postponed")
        if len(not_found) > 0:
            logging.warning( f"{module.NAME} {len(not_found)} files not found")
        if len(failure) > 0:
            logging.error( f"{module.NAME} {len(failure)} files update failed")
    except Exception:
        logging.exception(f'{module.NAME} failed')

    try:
        logging.debug(f"{module.NAME} cleaned up")
        await module.cleanup()
    except Exception:
        logging.exception(f"{module.NAME} cleanup failed")
        return

async def augment_meili_doc_from_file_path(file_path, metadata_dir_path, document, module):
    path = Path(file_path)
    relative_path = path.relative_to(OS_DIRECTORY_TO_INDEX).as_posix()
    logging.debug(f'{module.NAME} started "{relative_path}"')
    
    if not path.exists():
        return "not found"
    
    module_save_path = Path(metadata_dir_path) / module.NAME
    if not os.path.exists(module_save_path):
        os.makedirs(module_save_path)

    updated = False
    async for document in module.get_fields(file_path, module_save_path, document):
        updated = True
        if not path.exists():
            return "not found"
        write_document_json_for_file_path(file_path, document)
        await add_or_update_doc_meili(document)
    
    return "success" if updated else "up-to-date"
     
class EventHandler(FileSystemEventHandler):
    def __init__(self):
        self.meili_client = Client(MEILISEARCH_HOST)
        self.index = self.meili_client.index(MEILISEARCH_INDEX_NAME)

    def on_created(self, event):
        if not event.is_directory:
            try:
                logging.info(f'create "{event.src_path}"')
                self.create_or_update_meili(event.src_path)
            except Exception:
                logging.exception(f'create failed "{event.src_path}"')

    def on_modified(self, event):
        if not event.is_directory:
            try:
                logging.info(f'modify "{event.src_path}"')
                self.delete_meili(event.src_path)
                self.create_or_update_meili(event.src_path)
            except Exception:
                logging.exception(f'modify failed "{event.src_path}"')

    def on_deleted(self, event):
        if not event.is_directory:
            try:
                logging.info(f'delete "{event.src_path}"')
                self.delete_meili(event.src_path)
            except Exception:
                logging.exception(f'delete failed "{event.src_path}"')

    def on_moved(self, event):
        if not event.is_directory:
            try:
                logging.info(f'move "{event.src_path}" -> "{event.dest_path}"')
                self.delete_meili(event.src_path)
                self.delete_meili(event.dest_path)
                self.create_or_update_meili(event.dest_path)
            except Exception:
                logging.exception(f'move failed "{event.src_path}" -> "{event.dest_path}"')

    def create_or_update_meili(self, file_path):
        document = create_meili_doc_from_file_path(file_path)
        self.index.update_document(document)

    def delete_meili(self, file_path):
        id = read_document_id_from_xattr(file_path)
        self.index.delete_document(id)
        
def process_main():
    logger = logging.getLogger('watchdog')
    logging.root = logger
    logging.getLogger().handlers = logger.handlers

    try:
        logging.info(f'watchdog init')
        observer = Observer()
        handler = EventHandler()
        observer.schedule(handler, OS_DIRECTORY_TO_INDEX, recursive=True)
        observer.start()
        observer.join()
    except Exception:
        logging.exception(f'watchdog init failed')
    
async def update_meili_docs():
    await sync_meili_docs()
    
    process = Process(target=process_main)
    process.start()    
    logging.info(f'watchdog process started')
    
    while True:
        start_time = time.time()
        for module in modules:
            await augment_meili_docs(module)
        elapsed_time = time.time() - start_time
        await wait_for_idle_meili()
        if elapsed_time < 60:
            await asyncio.sleep(RECHECK_TIME_AFTER_COMPLETE)
           
async def main():
    await init_meili()
    await update_meili_docs()

if __name__ == "__main__":
    asyncio.run(main())