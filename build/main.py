import asyncio
import datetime
import json
import logging
import os
import shutil

from inotify_module import start_inotify
from shared import MEILISEARCH_HOST, MEILISEARCH_INDEX_NAME, MEILISEARCH_BATCH_SIZE, METADATA_DIRECTORY, INDEX_DIRECTORY, ALLOWED_TIME_PER_MODULE, RECHECK_TIME_AFTER_COMPLETE, gather_file_infos, delete_from_cache, modules, get_document_for_hash, path_from_meili_doc, path_from_relative_path, is_in_archive_directory, handle_document_changed
from collections import defaultdict
from process_tasks_module import process_tasks
from itertools import chain
from logging.handlers import TimedRotatingFileHandler
from meilisearch_python_sdk import AsyncClient
from multiprocessing import Process
from pathlib import Path

if not os.path.exists("./data/logs"):
    os.makedirs("./data/logs")
if not os.path.exists(METADATA_DIRECTORY):
    os.makedirs(METADATA_DIRECTORY)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    handlers=[
        TimedRotatingFileHandler(
            "./data/logs/indexer.log",
            when="midnight",
            interval=1,
            backupCount=7,
            atTime=datetime.time(2, 30),
        ),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger("asyncio")
logger.setLevel(logging.WARNING)
file_handler = logging.FileHandler(f"./data/logs/asyncio.log")
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(file_handler)
logger.propagate = False

logger = logging.getLogger("httpcore.connection")
logger.setLevel(logging.WARNING)
file_handler = logging.FileHandler(f"./data/logs/httpcore.connection.log")
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(file_handler)
logger.propagate = False

logger = logging.getLogger("httpcore.http11")
logger.setLevel(logging.WARNING)
file_handler = logging.FileHandler(f"./data/logs/httpcore.http11.log")
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(file_handler)
logger.propagate = False

logger = logging.getLogger("httpx")
logger.setLevel(logging.WARNING)
file_handler = logging.FileHandler(f"./data/logs/httpx.log")
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
        if getattr(e, "code", None) == "index_not_found":
            try:
                logging.info(f'meili create index "{MEILISEARCH_INDEX_NAME}"')
                index = await client.create_index(
                    MEILISEARCH_INDEX_NAME, primary_key="id"
                )
            except Exception:
                logging.exception(
                    f'meile create index failed "{MEILISEARCH_INDEX_NAME}"'
                )
                raise
        else:
            logging.exception(f"meili init failed")
            raise

    filterable_attributes = [
        "is_archived",
        "mtime",
        "paths",
        "size",
        "status",
        "type",
    ] + list(chain(*[module.FILTERABLE_FIELD_NAMES for module in modules]))

    try:
        logging.debug(f"meili update index attrs")
        await index.update_filterable_attributes(filterable_attributes)
        await index.update_sortable_attributes([
            "is_archived",
            "mtime",
            "paths",
            "size",
            "status",
            "type",
        ] + list(chain(*[module.SORTABLE_FIELD_NAMES for module in modules]))
        )
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
                batch = docs[i : i + MEILISEARCH_BATCH_SIZE]
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
                batch = ids[i : i + MEILISEARCH_BATCH_SIZE]
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


async def get_all_pending_jobs(module):
    if not index:
        raise Exception("MeiliSearch index is not initialized.")
    
    docs = []
    offset = 0
    limit = MEILISEARCH_BATCH_SIZE
    filter_query = f'status = {module.NAME}'
    
    try:
        while True:
            response = await index.get_documents(
                filter=filter_query,
                limit=limit,
                offset=offset,
            )
            docs.extend(response.results)
            if len(response.results) < limit:
                break
            offset += limit
        return docs
    except Exception as e:
        logging.error(f"Failed to get pending jobs from MeiliSearch: {e}")
        raise

async def wait_for_meili_idle():
    if not client:
        raise Exception("meili index did not init")

    try:
        while True:
            tasks = await client.get_tasks()
            active_tasks = [
                task
                for task in tasks.results
                if task.status in ["enqueued", "processing"]
            ]
            if len(active_tasks) == 0:
                break
            await asyncio.sleep(1)
    except Exception:
        logging.exception(f"meili wait for idle failed")
        raise

def restore_lost_docs_from_db(meili_ids, docs_to_update_in_meili):
    restored_doc_ids = set()
    logging.debug("processing metadata directory")
    if not os.path.exists(METADATA_DIRECTORY):
        logging.debug(f'metadata directory "{METADATA_DIRECTORY}" does not exist')
        return restored_doc_ids
    for entry in os.scandir(METADATA_DIRECTORY):
        if entry.is_dir(follow_symlinks=False):
            doc_id = entry.name
            if doc_id in meili_ids:
                continue
            document_json_path = os.path.join(entry.path, "document.json")
            if os.path.isfile(document_json_path):
                with open(document_json_path, "r") as f:
                    document = json.load(f)
                docs_to_update_in_meili.append(document)
                restored_doc_ids.add(doc_id)
    return restored_doc_ids

def filter_ids_to_delete_meili(ids_to_delete_from_meili, meili_docs_by_id):
    filtered_ids = set()
    for doc_id in ids_to_delete_from_meili:
        document = meili_docs_by_id.get(doc_id)
        if document:
            paths = document.get("paths", [])
            for relative_path in paths:
                if not is_in_archive_directory(path_from_relative_path(relative_path)):
                    filtered_ids.add(doc_id)
                    logging.debug(f'document "{doc_id}" marked for deletion')
                    break
    return filtered_ids

def delete_ids_from_db(ids):
    deleted = set()
    for doc_id in ids:
        doc_path = os.path.join(METADATA_DIRECTORY, doc_id, "document.json")
        with open(doc_path, "r") as f:
            document = json.load(f)
        if document:
            paths = document.get("paths", [])
            should_delete = True
            for relative_path in paths:
                path = path_from_relative_path(relative_path)
                if is_in_archive_directory(path):
                    should_delete = False
                elif not os.path.exists(path):
                    delete_from_cache(path)
            if should_delete:
                deleted.add(doc_id)
                metadata_entry_path = os.path.join(METADATA_DIRECTORY, doc_id)
                if os.path.exists(metadata_entry_path):
                    shutil.rmtree(metadata_entry_path)
    return deleted

async def sync_documents():
    logging.info(f"walk directory")
    file_paths = []
    for root, dirs, files in os.walk(INDEX_DIRECTORY):
        dirs[:] = [d for d in dirs if os.path.join(root, d) != METADATA_DIRECTORY]
        for filename in files:
            file_path = os.path.join(root, filename)
            file_paths.append(file_path)

    logging.info(f"get hashes")
    all_file_infos = gather_file_infos(file_paths)

    file_infos_by_hash = defaultdict(list)
    for file_info in all_file_infos:
        file_infos_by_hash[file_info["id"]].append(file_info)

    logging.info("collect documents")
    file_hashes = set()
    docs_to_update_in_meili = []
    dead_doc_ids = set()
    for hash, file_infos in file_infos_by_hash.items():
        file_hashes.add(hash)
        document, is_document_changed, is_document_dead = get_document_for_hash(hash, file_infos)
        if not is_document_dead and is_document_changed:
            docs_to_update_in_meili.append(document)
            logging.debug(f'queued document "{hash}" for add/update')
        if is_document_dead:
            dead_doc_ids.add(document["id"])
         
    logging.info("cleanup hash cache and metadata database")   
    file_system_db_ids = set()
    for entry in os.scandir(METADATA_DIRECTORY):
        if entry.is_dir(follow_symlinks=False):
            file_system_db_ids.add(entry.name)
    delete_ids_from_db((file_system_db_ids - file_hashes) | dead_doc_ids)
    
    logging.debug("get all documents from meili")
    meili_docs = await get_all_documents()
    meili_docs_by_id = {doc["id"]: doc for doc in meili_docs}
    meili_ids = set(meili_docs_by_id.keys())
    logging.info(f"meili has {len(meili_docs)} documents")
    ids_to_delete_from_meili = filter_ids_to_delete_meili((meili_ids - file_hashes) | dead_doc_ids, meili_docs_by_id)
    
    restored_doc_ids = restore_lost_docs_from_db(meili_ids, docs_to_update_in_meili)

    if len(docs_to_update_in_meili) > 0 or len(ids_to_delete_from_meili) > 0:
        logging.info(f"update meili")
        if len(ids_to_delete_from_meili) > 0:
            await delete_documents_by_id(list(ids_to_delete_from_meili))
            await wait_for_meili_idle()
        if len(docs_to_update_in_meili) > 0:
            await add_or_update_documents(docs_to_update_in_meili)
        logging.debug(f"waiting for meili")
        await wait_for_meili_idle()

    if len(restored_doc_ids) > 0:
        logging.info(
            f'restored {len(restored_doc_ids)} documents to meili'
        )
    if len(docs_to_update_in_meili) > 0:
        logging.info(
            f"added or updated {len(docs_to_update_in_meili)} documents in meili"
        )
    if len(ids_to_delete_from_meili) > 0:
        logging.info(f"deleted {len(ids_to_delete_from_meili)} documents in meili")

    total_docs_in_meili = await get_document_count()
    logging.info(f"meili has {total_docs_in_meili} up-to-date documents")
    
def get_file_list(module):
    dirs_with_mtime = []
    for metadata_dir_path in Path(METADATA_DIRECTORY).iterdir():
        try:
            if (
                metadata_dir_path.is_dir()
                and (metadata_dir_path / "document.json").exists()
            ):
                with open(metadata_dir_path / "document.json", "r") as file:
                    document = json.load(file)
                    file_path = path_from_meili_doc(document)
                mtime = os.stat(file_path).st_mtime
                if document['status'] == module.NAME:
                    dirs_with_mtime.append(
                        (file_path, document, metadata_dir_path, mtime)
                    )
        except Exception as e:
            logging.warning(f"{module.NAME} {metadata_dir_path} failed: {e}")
    dirs_with_mtime.sort(key=lambda x: (0 if x[1]['is_archived'] else 1, -x[3]))
    return dirs_with_mtime

async def augment_documents(module):
    logging.debug(f"{module.NAME} select files")
    file_list = get_file_list(module)
    try:
        if file_list:
            logging.info(f"{module.NAME} started for {len(file_list)} out-dated hashes")
            cancel_event = asyncio.Event()

            async def document_update_handler():
                async for pdoc, cdoc in process_tasks(module, file_list, cancel_event):
                    try:
                        document = handle_document_changed(pdoc, cdoc)
                        await add_or_update_document(document)
                    except Exception as e:
                        logging.exception(
                            f'{module.NAME} updating meili for document "{document}" failed: {e} '
                        )

            task = asyncio.create_task(document_update_handler())
            try:
                logging.debug(f"{module.NAME} go")
                await asyncio.wait_for(
                    asyncio.shield(task), timeout=ALLOWED_TIME_PER_MODULE
                )
                logging.debug(f"{module.NAME} up-to-date")
                return False
            except asyncio.TimeoutError:
                logging.debug(
                    f"{module.NAME} ask cancel after in-progress files are complete"
                )
                cancel_event.set()
                await task
    except Exception as e:
        logging.exception(f"{module.NAME} processing failed: {e}")
    logging.debug(f"{module.NAME} stop")
    return True

async def begin_indexing():
    await sync_documents()
    logging.info(f"run modules")
    log_up_to_date = True
    while True:
        run_again = False
        for module in modules:
            run_again = (await augment_documents(module)) or run_again
            log_up_to_date = run_again or log_up_to_date
        if not run_again:
            if log_up_to_date:
                logging.debug(
                    f"up-to-date, recheck on {RECHECK_TIME_AFTER_COMPLETE / 60} minute intervals"
                )
                log_up_to_date = False
            await asyncio.sleep(RECHECK_TIME_AFTER_COMPLETE)

async def main():
    await init_meili()
    Process(target=start_inotify).start()
    await begin_indexing()

if __name__ == "__main__":
    asyncio.run(main())
