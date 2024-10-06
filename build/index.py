import os
import logging
import asyncio
import hashlib
import magic
import mimetypes
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import time
from logging.handlers import TimedRotatingFileHandler
from meilisearch_python_sdk import AsyncClient, Client
from multiprocessing import Process
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

import tika_module
import whisper_module

modules = [tika_module, whisper_module]

if not os.path.exists("./data/logs"):
    os.makedirs("./data/logs")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        TimedRotatingFileHandler(
            "./data/logs/indexer.log",
            when="midnight",
            interval=1,
            backupCount=7,
            atTime=time(2, 30)
        ),
        logging.StreamHandler(),
    ],
)

other_loggers = ['httpx', 'tika.tika', 'faster_whisper'] 
for logger_name in other_loggers:
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.WARNING)
    file_handler = logging.FileHandler(f"./data/logs/{logger_name}.log")
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(file_handler)
    logger.propagate = False
    
logger = logging.getLogger('watchdog')
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(f"./data/logs/watchdog.log")
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(file_handler)
logger.propagate = False

ALLOWED_TIME_PER_MODULE = int(os.environ.get("ALLOWED_TIME_PER_MODULE", "900"))
DOMAIN = os.environ.get("DOMAIN", "private.0819870.xyz")
MEILISEARCH_BATCH_SIZE = int(os.environ.get("MEILISEARCH_BATCH_SIZE", "10000"))
MEILISEARCH_HOST = os.environ.get("MEILISEARCH_HOST", "http://meilisearch:7700")
MEILISEARCH_INDEX_NAME = os.environ.get("MEILISEARCH_INDEX_NAME", "files")
OS_DIRECTORY_TO_INDEX = os.environ.get("OS_DIRECTORY_TO_INDEX", "/data")
SLEEP_BETWEEN_MODULE_RUNS = int(os.environ.get("SLEEP_BETWEEN_MODULE_RUNS", "300"))

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

    filterable_attributes = ["id"] + [module.FIELD_NAME for module in modules]
    
    try:
        logging.debug(f"meili update index attrs")
        await index.update_filterable_attributes(filterable_attributes)
        await index.update_sortable_attributes(["mtime"])
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

async def get_all_pending_jobs(module):
    if not index:
        raise Exception("meili index did not init")
    
    docs = []
    offset = 0
    limit = MEILISEARCH_BATCH_SIZE
    filter_query = f'{module.FIELD_NAME} < {module.VERSION}'
    fields = ["url", "mtime", "type"] + module.DATA_FIELD_NAMES
    
    try:
        while True:
            response = await index.get_documents(
                filter=filter_query,
                limit=limit,
                offset=offset,
                fields=fields
            )
            docs.extend(response.results)
            if len(response.results) < limit:
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

def get_meili_id_from_relative_path(relative_path):
    return hashlib.sha256(relative_path.encode()).hexdigest()

def get_meili_id_from_file_path(file_path):
    return get_meili_id_from_relative_path(get_relative_path_from_file_path(file_path))

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
    updated_document_ids = set()
    create_document_tasks = []

    logging.info(f"meili {len(expected_documents)} indexed")
    
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
                    directory_stack.append(entry.path)
                elif entry.is_file(follow_symlinks=False):
                    id = get_meili_id_from_file_path(entry.path)
                    path_mtime = Path(entry.path).stat().st_mtime
                    if id not in expected_documents_by_id or expected_documents_by_id[id]["mtime"] < path_mtime:
                        task = asyncio.create_task(async_create_meili_doc_from_file_path(entry.path))
                        create_document_tasks.append(task)
                        if id not in expected_documents_by_id:
                            added_document_ids.add(id)
                        elif expected_documents_by_id[id]["mtime"] < path_mtime:
                            updated_document_ids.add(id)
                    current_file_paths.add(entry.path)
        except Exception:
            logging.exception(f'os scan directory failed "{dir_path}"')

    done, _ = await asyncio.wait(create_document_tasks)
    added_or_updated_documents = [task.result() for task in done if task.result() is not None]

    deleted_file_paths = previous_file_paths - current_file_paths
    deleted_document_ids = {get_meili_id_from_file_path(fp) for fp in deleted_file_paths}

    await delete_docs_by_id_meili(list(deleted_document_ids | updated_document_ids))
    await wait_for_idle_meili()
    await add_or_update_docs_meili(added_or_updated_documents)
    await wait_for_idle_meili()

    total_expected_documents = await get_doc_count_meili()
    logging.info(f"meili added {len(added_document_ids)}")
    logging.info(f"meili updated {len(updated_document_ids)}")
    logging.info(f"meili deleted {len(deleted_document_ids)}")
    logging.info(f"meili {total_expected_documents} now indexed")

def create_meili_doc_from_file_path(file_path):
    try:
        path = Path(file_path)
        relative_path = path.relative_to(OS_DIRECTORY_TO_INDEX).as_posix()
        stat = path.stat()
        current_mtime = stat.st_mtime
        mime = get_mime_magic(path)
        doc_id = get_meili_id_from_relative_path(relative_path)
        document = {
            "id": doc_id,
            "name": path.name,
            "size": stat.st_size,
            "mtime": current_mtime,
            "ctime": stat.st_ctime,
            "url": get_url_from_relative_path(relative_path),
            "type": mime,
        }
        for module in modules:
            if module.does_support_mime(mime):
                document[module.FIELD_NAME] = 0
        return document
    except Exception:
        logging.exception(f'os create doc failed "{file_path}"')
        return None

async def augment_meili_docs(module):
    try:
        logging.debug(f"{module.NAME} get pending files")
        pending_jobs = await get_all_pending_jobs(module)
        file_paths_with_mime = [
            [get_file_path_from_meili_doc(doc), doc]
            for doc in sorted(pending_jobs, key=lambda x: x['mtime'], reverse=True)
        ]
    except Exception:
        logging.exception(f"{module.NAME} failed to get pending files")
        return

    if not file_paths_with_mime:
        return

    logging.info(f"{module.NAME} start for {len(file_paths_with_mime)} files")

    try:
        logging.debug(f"init {module.NAME}")
        await module.init()
    except Exception:
        logging.exception(f"{module.NAME} init failed")
        return

    try:
        semaphore = asyncio.Semaphore(module.MAX_WORKERS)

        async def sem_task(fp):
            async with semaphore:
                return await augment_meili_doc_from_file_path(fp[0], fp[1], module)
            
        tasks_dict = {}
        tasks_list = []

        for fp in file_paths_with_mime:
            task = asyncio.create_task(sem_task(fp))
            tasks_dict[fp[0]] = task
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
            
        for file_path, task in tasks_dict.items():
            try:
                result = await task
                if result == "success":
                    success.append(file_path)
                elif result == "not found":
                    not_found.append(file_path)
            except asyncio.CancelledError:
                postponed.append(file_path)
            except Exception:
                logging.exception( f"{module.NAME} failed at {file_path}")
                failure.append(file_path)
         
        if len(success) > 0:
            logging.info( f"{module.NAME} {len(success)} files succeeded")
        if len(postponed) > 0:
            logging.info( f"{module.NAME} {len(postponed)} files postponed")
        if len(not_found) > 0:
            logging.warning( f"{module.NAME} {len(not_found)} files not found")
        if len(failure) > 0:
            logging.error( f"{module.NAME} {len(failure)} files failed")
    except Exception:
        logging.exception(f'{module.NAME} failed')

    try:
        logging.debug(f"{module.NAME} cleanup")
        await module.cleanup()
    except Exception:
        logging.exception(f"{module.NAME} cleanup failed")
        return

async def augment_meili_doc_from_file_path(file_path, doc, module):    
    path = Path(file_path)
    relative_path = path.relative_to(OS_DIRECTORY_TO_INDEX).as_posix()
    
    logging.debug(f'{module.NAME} start "{relative_path}"')
    
    id = get_meili_id_from_relative_path(relative_path)

    if not path.exists():
        return "not found"

    async for fields in module.get_fields(file_path, doc):
        if not path.exists():
            return "not found"
        
        updated_doc = {
            "id": id,
            **fields,
        }
        
        await add_or_update_doc_meili(updated_doc)
        
    updated_doc = {
        "id": id,
        f'{module.FIELD_NAME}': module.VERSION,
    }
    
    await add_or_update_doc_meili(updated_doc)
    return "success"
     
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
        fp, document, status = create_meili_doc_from_file_path(file_path)
        self.index.add_documents([document])

    def delete_meili(self, file_path):
        id = get_meili_id_from_file_path(file_path)
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
    
    logging.info(f'start watchdog process')
    process = Process(target=process_main)
    process.start()    
    
    while True:
        for module in modules:
            await augment_meili_docs(module)
        logging.info(f'wait for next cycle')
        await wait_for_idle_meili()
        await asyncio.sleep(SLEEP_BETWEEN_MODULE_RUNS)
           
async def main():
    await init_meili()
    await update_meili_docs()

if __name__ == "__main__":
    asyncio.run(main())