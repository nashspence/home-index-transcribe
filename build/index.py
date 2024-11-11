import asyncio
import datetime
import json
import logging
import magic
import math
import mimetypes
import os
import shutil
import tempfile
import time
import uuid

from collections import defaultdict
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
ARCHIVE_DIRECTORY = os.environ.get("METADATA_DATABASE_DIRECTORY", "/data/archive")
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
    level=logging.DEBUG,
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
        original_stat = os.stat(file_path)
        original_mtime = original_stat.st_mtime
        original_atime = original_stat.st_atime
        data = document_id.encode('utf-8')
        os.setxattr(file_path, DOCUMENT_ID_XATTR_FIELD, data)
        os.utime(file_path, (original_atime, original_mtime))
    except Exception as e:
        logging.exception(f"Failed to write document ID to xattr on {file_path}: {e}")

def read_versions_json_for_file_path(file_path):
    id = read_document_id_from_xattr(file_path)
    
    try:
        with open(os.path.join(METADATA_DATABASE_DIRECTORY, id, 'version.json'), 'r') as file:
            return json.load(file)
    except Exception as e:
        return None
    
def write_versions_json_for_file_path(file_path, dict):
    id = read_document_id_from_xattr(file_path)
    
    metadata_directory_path = f'{METADATA_DATABASE_DIRECTORY}/{id}'
    if not os.path.exists(metadata_directory_path):
        os.makedirs(metadata_directory_path)
    
    if id:
        with open(os.path.join(METADATA_DATABASE_DIRECTORY, id, 'version.json'), 'w') as file:
            return json.dump(dict, file, indent=4, separators=(", ", ": "))
    else:
        raise Exception("no id available")
    
def read_document_json_for_file_path(file_path):
    id = read_document_id_from_xattr(file_path)
    try:
        with open(os.path.join(METADATA_DATABASE_DIRECTORY, id, 'document.json'), 'r') as file:
            return json.load(file)
    except Exception as e:
        return None
    
def write_document_json_for_file_path(file_path, dict):
    id = read_document_id_from_xattr(file_path)
    
    metadata_directory_path = f'{METADATA_DATABASE_DIRECTORY}/{id}'
    if not os.path.exists(metadata_directory_path):
        os.makedirs(metadata_directory_path)
    
    if id:
        with open(os.path.join(METADATA_DATABASE_DIRECTORY, id, 'document.json'), 'w') as file:
            return json.dump(dict, file, indent=4, separators=(", ", ": "))
    else:
        raise Exception("no id available")

def get_file_path_from_meili_doc(doc):
    return Path(doc['url'].replace(f"https://{DOMAIN}/", f"{OS_DIRECTORY_TO_INDEX}/")).as_posix()

def get_relative_path_from_meili_doc(doc):
    return get_relative_path_from_url(doc['url'])

def get_relative_path_from_url(url):
    return Path(url.replace(f"https://{DOMAIN}/", "")).as_posix()

def get_url_from_relative_path(relative_path):
    return f"https://{DOMAIN}/{relative_path}"

def get_url_from_file_path(file_path):
    return get_url_from_relative_path(os.path.relpath(file_path, OS_DIRECTORY_TO_INDEX))

def get_relative_path_from_file_path(file_path):
    return Path(file_path).relative_to(OS_DIRECTORY_TO_INDEX).as_posix()

async def sync_meili_docs():
    expected_documents = await get_all_docs_meili()
    expected_documents_by_id = {doc["id"]: doc for doc in expected_documents}
    expected_document_ids = set(expected_documents_by_id.keys())
    current_document_ids = set()
    documents_to_delete_ids = set()
    added_or_updated_documents = []
    restored_document_ids = set()
    current_file_paths = set()

    logging.info(f"meili has {len(expected_documents)} documents")

    directory_stack = [OS_DIRECTORY_TO_INDEX]
    all_files_info = []

    while directory_stack:
        dir_path = directory_stack.pop()
        try:
            entries = await asyncio.to_thread(lambda: list(os.scandir(dir_path)))
            for entry in entries:
                if entry.is_dir(follow_symlinks=False):
                    if entry.path != METADATA_DATABASE_DIRECTORY:
                        directory_stack.append(entry.path)
                elif entry.is_file(follow_symlinks=False):
                    file_info = gather_file_info(entry.path)
                    if file_info:
                        all_files_info.append(file_info)
                        current_file_paths.add(entry.path)
        except Exception:
            logging.exception(f'failed to scan directory "{dir_path}"')

    files_by_id = defaultdict(list)
    for file_info in all_files_info:
        files_by_id[file_info['id']].append(file_info)

    if None in files_by_id:
        for file_info in files_by_id.pop(None):
            assign_new_id(file_info)
            current_document_ids.add(file_info['id'])
            document = process_document(file_info)
            if document:
                added_or_updated_documents.append(document)

    for id, files in files_by_id.items():
        if len(files) == 1:
            file_info = files[0]
            current_document_ids.add(id)
            document = process_single_file(file_info)
            if document:
                added_or_updated_documents.append(document)
        else:
            current_document_ids.update(handle_duplicate_ids(id, files, added_or_updated_documents, documents_to_delete_ids))

    documents_to_delete_ids.update(expected_document_ids - current_document_ids)

    restored_document_ids.update(process_metadata_directory(
        current_document_ids, expected_documents_by_id, added_or_updated_documents, documents_to_delete_ids))

    documents_to_delete_ids_filtered = filter_archive_documents(documents_to_delete_ids, expected_documents_by_id)

    await delete_documents(documents_to_delete_ids_filtered)
    await add_or_update_docs_meili(added_or_updated_documents)
    await wait_for_idle_meili()

    total_expected_documents = await get_doc_count_meili()
    logging.info(f"meili restored {len(restored_document_ids)} documents from '{METADATA_DATABASE_DIRECTORY}'")
    logging.info(f"meili added or updated {len(added_or_updated_documents)} documents")
    logging.info(f"meili deleted {len(documents_to_delete_ids_filtered)} documents")
    logging.info(f"meili now has {total_expected_documents} documents")

def gather_file_info(file_path):
    try:
        id = read_document_id_from_xattr(file_path)
        entry_path = Path(file_path)
        stat = entry_path.stat()
        mtime = stat.st_mtime
        document = read_document_json_for_file_path(file_path)
        version = read_versions_json_for_file_path(file_path)
        file_info = {
            'path': file_path,
            'id': id,
            'mtime': mtime,
            'document': document,
            'version': version,
            'stat': stat
        }
        return file_info
    except Exception:
        logging.exception(f'Failed to gather file info for "{file_path}"')
        return None

def assign_new_id(file_info):
    new_id = str(uuid.uuid4())
    write_document_id_to_xattr(file_info['path'], new_id)
    file_info['id'] = new_id

def process_document(file_info, existing_document=None, copies_info=None):
    try:
        path = Path(file_info['path'])
        relative_path = path.relative_to(OS_DIRECTORY_TO_INDEX).as_posix()
        current_mtime = file_info['mtime']
        mime_type = get_mime_magic(path)
        doc_id = file_info['id']

        document = {
            "id": doc_id,
            "modified_date": current_mtime,
            "size": file_info['stat'].st_size,
            "type": mime_type,
            "url": get_url_from_relative_path(relative_path),
        }

        if existing_document:
            if 'copies' in existing_document:
                document['copies'] = existing_document['copies']
            if 'copies' in document and copies_info:
                document['copies'].update(copies_info)
            elif copies_info:
                document['copies'] = copies_info
        else:
            if copies_info:
                document['copies'] = copies_info
            
        version = {"version": VERSION}
        
        write_document_json_for_file_path(file_info['path'], document)
        write_versions_json_for_file_path(file_info['path'], version)

        return document
    except Exception:
        logging.exception(f'Failed to process document for "{file_info["path"]}"')
        return None

def process_single_file(file_info):
    current_url = get_url_from_relative_path(os.path.relpath(file_info['path'], OS_DIRECTORY_TO_INDEX))
    document = file_info.get('document')

    if document:
        if document['url'] != current_url and math.isclose(file_info['mtime'], document.get('modified_date', 0), abs_tol=1e-7):
            document['url'] = current_url
            write_document_json_for_file_path(file_info['path'], document)
            return document
        if not math.isclose(file_info['mtime'], document.get('modified_date', 0), abs_tol=1e-7):
            return process_document(file_info, existing_document=document)
        if 'copies' in document:
            for key, value in document['copies'].items():
                if not os.path.join(METADATA_DATABASE_DIRECTORY, value):
                    del document['copies'][key]
            if not document['copies']:
                del document['copies']
            write_document_json_for_file_path(document)
            return document
    else:
        return process_document(file_info)
        
    return None

def handle_duplicate_ids(id, files, added_or_updated_documents, documents_to_delete_ids):
    new_ids = set()
    mtimes = [f['mtime'] for f in files]
    if all(ct == mtimes[0] for ct in mtimes):
        # Can't determine original, assign new IDs to all
        for file_info in files:
            assign_new_id(file_info)
            new_ids.add(file_info['id'])
            document = process_document(file_info)
            if document:
                added_or_updated_documents.append(document)
        documents_to_delete_ids.add(id)
    else:
        # Determine original and copies
        files_sorted = sorted(files, key=lambda f: f['mtime'])
        original_file = files_sorted[0]
        new_ids.add(original_file['id'])
        copies = files_sorted[1:]
        copies_info = {}
        for copy_file in copies:
            assign_new_id(copy_file)
            new_ids.add(copy_file['id'])
            document = process_document(copy_file)
            if document:
                added_or_updated_documents.append(document)
            copies_info[str(copy_file['mtime'])] = copy_file['id']
        # Update original document with copies info
        document = process_document(original_file, existing_document=original_file.get('document'), copies_info=copies_info)
        if document:
            added_or_updated_documents.append(document)
    return new_ids

def process_metadata_directory(current_document_ids, expected_documents_by_id, added_or_updated_documents, documents_to_delete_ids):
    restored_document_ids = set()
    try:
        entries = os.scandir(METADATA_DATABASE_DIRECTORY)
        for entry in entries:
            if entry.is_dir(follow_symlinks=False):
                doc_id = entry.name
                document_json_path = os.path.join(entry.path, 'document.json')
                if os.path.isfile(document_json_path):
                    with open(document_json_path, 'r') as f:
                        document = json.load(f)
                    if doc_id in current_document_ids:
                        continue
                    if doc_id not in expected_documents_by_id:
                        restored = restore_document_from_metadata(doc_id, document, added_or_updated_documents, documents_to_delete_ids)
                        if restored:
                            restored_document_ids.add(doc_id)
    except Exception:
        logging.exception(f'Failed to scan METADATA_DATABASE_DIRECTORY "{METADATA_DATABASE_DIRECTORY}"')
    return restored_document_ids

def restore_document_from_metadata(doc_id, document, added_or_updated_documents, documents_to_delete_ids):
    url = document['url']
    file_path = get_file_path_from_meili_doc(document)
    if os.path.exists(file_path):
        file_info = gather_file_info(file_path)
        if file_info:
            # Update document with current file information
            updated_document = process_document(file_info, existing_document=document)
            if updated_document:
                added_or_updated_documents.append(updated_document)
                return True
    else:
        relative_path = get_relative_path_from_url(url)
        if relative_path.startswith(ARCHIVE_DIRECTORY):
            # Restore the document to MeiliSearch
            added_or_updated_documents.append(document)
            return True
        else:
            # Delete the METADATA_DATABASE_DIRECTORY entry
            metadata_entry_path = os.path.join(METADATA_DATABASE_DIRECTORY, doc_id)
            shutil.rmtree(metadata_entry_path)
            documents_to_delete_ids.add(doc_id)
            return False
    return False

def filter_archive_documents(documents_to_delete_ids, expected_documents_by_id):
    filtered_ids = set()
    for id in documents_to_delete_ids:
        document = expected_documents_by_id.get(id)
        if document:
            url = document['url']
            relative_path = get_relative_path_from_url(url)
            if not relative_path.startswith(ARCHIVE_DIRECTORY):
                filtered_ids.add(id)
    return filtered_ids

async def delete_documents(documents_to_delete_ids_filtered):
    await delete_docs_by_id_meili(list(documents_to_delete_ids_filtered))
    await wait_for_idle_meili()
    for doc_id in documents_to_delete_ids_filtered:
        metadata_entry_path = os.path.join(METADATA_DATABASE_DIRECTORY, doc_id)
        if os.path.exists(metadata_entry_path):
            shutil.rmtree(metadata_entry_path)
    
async def augment_meili_docs(module):
    try:
        logging.info(f"init {module.NAME}")
        await module.init()
    except Exception:
        logging.exception(f"{module.NAME} init failed")
        return

    try:
        semaphore = asyncio.Semaphore(1)

        async def sem_task(file_path, metadata_dir_path, document):
            async with semaphore:
                return await augment_meili_doc_from_file_path(file_path, metadata_dir_path, document, module)
            
        tasks_dict = {}
        tasks_list = []
        
        dirs_with_mtime = []
        logging.info(f"determining {module.NAME} processing order")
        for metadata_dir_path in Path(METADATA_DATABASE_DIRECTORY).iterdir():
            if metadata_dir_path.is_dir() and (metadata_dir_path / "document.json").exists():
                with open(metadata_dir_path / "document.json", 'r') as file:
                    document = json.load(file)
                    file_path = get_file_path_from_meili_doc(document)
                mtime = os.stat(file_path).st_mtime
                dirs_with_mtime.append((metadata_dir_path, document, mtime))

        dirs_with_mtime.sort(key=lambda x: x[2], reverse=True)

        logging.info(f"start {module.NAME}")
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
        logging.debug(f'{module.NAME} did not find "{relative_path}"')
        return "not found"
    
    module_save_path = Path(metadata_dir_path) / module.NAME
    if not os.path.exists(module_save_path):
        os.makedirs(module_save_path)

    updated = False
    async for document in module.get_fields(file_path, module_save_path, document):
        updated = True
        if not path.exists():
            logging.debug(f'{module.NAME} did not find "{relative_path}"')
            return "not found"
        logging.debug(f'{module.NAME} updating "{relative_path}" to {document}')
        write_document_json_for_file_path(file_path, document)
        await add_or_update_doc_meili(document)
        logging.debug(f'{module.NAME} updated "{relative_path}"')
    
    if updated:
        logging.debug(f'{module.NAME} succeeded on "{relative_path}"')
    else:
        logging.debug(f'{module.NAME} unchanged "{relative_path}"')
        
    return "success" if updated else "up-to-date"
     
class EventHandler(FileSystemEventHandler):
    def __init__(self):
        self.meili_client = Client(MEILISEARCH_HOST)
        self.index = self.meili_client.index(MEILISEARCH_INDEX_NAME)

    def should_ignore(self, path):
        # Check if the event path starts with the ignored directory path
        return os.path.commonpath([path, METADATA_DATABASE_DIRECTORY]) == METADATA_DATABASE_DIRECTORY

    def on_created(self, event):
        if not event.is_directory and not self.should_ignore(event.src_path):
            try:
                logging.info(f'create "{event.src_path}"')
                self.copy_or_create_meili(event.src_path)
            except Exception:
                logging.exception(f'create failed "{event.src_path}"')

    def on_modified(self, event):
        if not event.is_directory and not self.should_ignore(event.src_path):
            try:
                logging.info(f'modify "{event.src_path}"')
                self.delete_meili(event.src_path)
                self.update_meili(event.src_path)
            except Exception:
                logging.exception(f'modify failed "{event.src_path}"')

    def on_deleted(self, event):
        if not event.is_directory and not self.should_ignore(event.src_path):
            try:
                logging.info(f'delete "{event.src_path}"')
                self.delete_meili(event.src_path)
            except Exception:
                logging.exception(f'delete failed "{event.src_path}"')

    def on_moved(self, event):
        if not event.is_directory and not (self.should_ignore(event.src_path) or self.should_ignore(event.dest_path)):
            try:
                logging.info(f'move "{event.src_path}" -> "{event.dest_path}"')
                self.update_meili(event.dest_path)
            except Exception:
                logging.exception(f'move failed "{event.src_path}" -> "{event.dest_path}"')

    def copy_or_create_meili(self, file_path):
        file_info = gather_file_info(file_path)
        if 'id' in file_info:
            assign_new_id(file_info)
            del file_info['document']
        self.index.update_document(process_single_file(file_info))

    def update_meili(self, file_path):
        file_info = gather_file_info(file_path)
        if not 'id' in file_info:
            assign_new_id(file_info)
        self.index.update_document(process_single_file(file_info))

    def delete_meili(self, file_path):
        relative_path = get_relative_path_from_file_path(file_path)
        if relative_path.startswith(ARCHIVE_DIRECTORY):
            return
        file_info = gather_file_info(file_path)
        if 'id' in file_info:
            shutil.rmtree(os.path.join(METADATA_DATABASE_DIRECTORY, file_info['id']))
            self.index.delete_document(file_info['id'])
        
def process_main():
    logger = logging.getLogger('watchdog-process')
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