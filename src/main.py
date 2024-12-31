# region "debugpy"


import os
import debugpy

debugpy.listen(("0.0.0.0", 5678))

if str(os.environ.get("WAIT_FOR_DEBUG_CLIENT", "false")).lower() == "true":
    print("Waiting for debugger to attach...")
    debugpy.wait_for_client()
    print("Debugger attached.")
    debugpy.breakpoint()


# endregion
# region "logging"

import logging

logging.basicConfig(level=logging.CRITICAL)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
file_handler = logging.RotatingFileHandler(
    "/storage/home-index-orchestrator.log", maxBytes=5_000_000, backupCount=10
)
stream_handler = logging.StreamHandler()
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
stream_handler.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
)
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

synclog = logging.getLogger("home-index-sync")
synclog.setLevel(logging.INFO)
file_handler = logging.RotatingFileHandler(
    "/storage/home-index-sync.log", maxBytes=5_000_000, backupCount=10
)
stream_handler = logging.StreamHandler()
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
stream_handler.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
)
synclog.addHandler(file_handler)
synclog.addHandler(stream_handler)

# endregion
# region "import"


import asyncio
import json
import shutil
import time
import magic
import mimetypes
import concurrent.futures
import xxhash
from xmlrpc.client import ServerProxy
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from multiprocessing import Process
from itertools import chain
from meilisearch_python_sdk import AsyncClient
from pathlib import Path
from pathlib import Path


# endregion
# region "config"


VERSION = 1
ALLOWED_TIME_PER_MODULE = int(os.environ.get("ALLOWED_TIME_PER_MODULE", "300"))
MEILISEARCH_BATCH_SIZE = int(os.environ.get("MEILISEARCH_BATCH_SIZE", "10000"))
MEILISEARCH_HOST = os.environ.get("MEILISEARCH_HOST", "http://localhost:7700")
MEILISEARCH_INDEX_NAME = os.environ.get("MEILISEARCH_INDEX_NAME", "files")
RECHECK_TIME_AFTER_COMPLETE = int(os.environ.get("RECHECK_TIME_AFTER_COMPLETE", "1800"))

INDEX_DIRECTORY = os.environ.get("INDEX_DIRECTORY", "/files")
if not os.path.exists(INDEX_DIRECTORY):
    os.makedirs(INDEX_DIRECTORY)

ARCHIVE_DIRECTORY = os.environ.get("ARCHIVE_DIRECTORY", "/files/archive")
if not os.path.exists(ARCHIVE_DIRECTORY):
    os.makedirs(ARCHIVE_DIRECTORY)

METADATA_DIRECTORY = os.environ.get("METADATA_DIRECTORY", "/files/metadata")
if not os.path.exists(METADATA_DIRECTORY):
    os.makedirs(METADATA_DIRECTORY)

modules = {}
module_values = []
hellos = []
hello_versions = []
hello_versions_changed = False
hello_versions_file_path = "/storage/hello_versions.json"

try:
    MODULES = os.environ.get("MODULES", "")
    for module_host in MODULES.split(","):
        proxy = ServerProxy(module_host.strip())
        hello = proxy.hello()
        name = hello["name"]
        version = hello["version"]
        hellos.append(hello)
        hello_versions.append([name, version])
        if name in modules:
            raise ValueError(
                f"multiple modules found with name {name}, this must be unique"
            )
        modules[name] = {"name": name, "proxy": proxy}
        module_values.append(modules[name])
    hello_versions_json = {}
    if os.path.exists(hello_versions_file_path):
        with open(hello_versions_file_path, "r") as file:
            hello_versions_json = json.load(file)
    known_hello_versions = hello_versions_json.get("hello_versions", "")
    hello_versions_changed = hello_versions != known_hello_versions
except ValueError:
    raise ValueError(
        "MODULES format should be 'http://domain:port,http://domain:port,...'"
    )


initial_module_id = module_values[0]["name"] if module_values else "idle"


def save_modules_state():
    os.makedirs(os.path.dirname(hello_versions_file_path), exist_ok=True)
    with open(hello_versions_file_path, "w") as file:
        json.dump({"hello_versions": hello_versions}, file)


# endregion
# region "meilisearch"


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
                    f'meili create index failed "{MEILISEARCH_INDEX_NAME}"'
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
    ] + list(chain(*[hello["filterable_attributes"] for hello in hellos]))

    try:
        logging.debug(f"meili update index attrs")
        await index.update_filterable_attributes(filterable_attributes)
        await index.update_sortable_attributes(
            [
                "is_archived",
                "mtime",
                "paths",
                "size",
                "status",
                "type",
            ]
            + list(chain(*[hello["sortable_attributes"] for hello in hellos]))
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
            logging.debug(f'index.update_documents "{doc["paths"][0]}" start')
            await index.update_documents([doc])
            logging.debug(f'index.update_documents "{doc["paths"][0]}" done')
        except Exception:
            logging.exception(
                f'index.update_documents "{doc["paths"][0]}" failed: "{[doc]}"'
            )
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
    filter_query = f"status = {module.NAME}"

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


# endregion
# region "sync"


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


def is_in_archive_directory(path):
    try:
        return os.path.commonpath([path, ARCHIVE_DIRECTORY]) == ARCHIVE_DIRECTORY
    except ValueError:
        return False


def compute_file_hash(file_path):
    hasher = xxhash.xxh64()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def should_rehash(file_path, existing_doc, rel_path):
    if not existing_doc:
        return True
    existing_mtime = existing_doc["paths"].get(rel_path, 0)
    current_mtime = os.stat(file_path).st_mtime
    return abs(existing_mtime - current_mtime) >= 1e-6


def process_file(file_path, index_dir, metadata_docs_by_id):
    rel_path = os.path.relpath(file_path, index_dir)
    stat_info = os.stat(file_path)
    file_mtime = stat_info.st_mtime

    # Check if we already know a doc that references this rel_path
    existing_doc = next(
        (doc for doc in metadata_docs_by_id.values() if rel_path in doc["paths"]), None
    )

    # Only compute the file hash if needed
    if existing_doc and not should_rehash(file_path, existing_doc, rel_path):
        # Reuse old doc_id
        doc_id = existing_doc["id"]
    else:
        doc_id = compute_file_hash(file_path)

    doc = metadata_docs_by_id.get(doc_id)
    if not doc:
        # Create new document metadata
        doc = {
            "id": doc_id,
            "paths": {rel_path: file_mtime},
            "mtime": file_mtime,
            "size": stat_info.st_size,
            "type": get_mime_type(file_path),
            "is_archived": is_in_archive_directory(file_path),
            "status": initial_module_id,
            "copies": 1,
        }
        # We return (doc_id, doc) so the main thread can persist + update dicts
        return (doc_id, doc)
    else:
        # Update existing doc if needed
        updated = False
        old_mtime = doc["paths"].get(rel_path)
        if old_mtime is None or abs(old_mtime - file_mtime) >= 1e-6:
            doc["paths"][rel_path] = file_mtime
            updated = True

        # Recount copies
        copies_count = len(doc["paths"])
        if doc.get("copies", 0) != copies_count:
            doc["copies"] = copies_count
            updated = True

        # Update doc["mtime"] if this file is the newest
        if file_mtime > doc.get("mtime", 0):
            doc["mtime"] = file_mtime
            updated = True

        # Recalculate size based on the first path
        p = doc["paths"][0]
        abs_p = path_from_relative_path(p)
        new_size = os.stat(abs_p).st_size
        if new_size != doc.get("size", 0):
            doc["size"] = new_size
            updated = True

        # If the new path is archived or if any path is archived => doc is archived
        was_archived = doc.get("is_archived", False)
        is_archived = any(
            is_in_archive_directory(path_from_relative_path(p)) for p in doc["paths"]
        )
        if is_archived != was_archived:
            doc["is_archived"] = is_archived
            updated = True

        # Update doc type if needed
        new_type = get_mime_type(file_path)
        if new_type != doc.get("type"):
            doc["type"] = new_type
            updated = True

        if hello_versions_changed:
            doc["status"] = initial_module_id

        if updated:
            return (doc_id, doc)
        else:
            return None


async def sync_documents():
    synclog.info("Starting document sync")

    # Load metadata documents
    synclog.info(f"Load metadata documents")
    metadata_docs_by_id = {}
    for entry in os.scandir(METADATA_DIRECTORY):
        if entry.is_dir():
            doc, _ = read_document_json(entry.name)
            if doc and isinstance(doc.get("paths", {}), dict):
                metadata_docs_by_id[entry.name] = doc

    # Fetch MeiliSearch documents
    synclog.info(f"Fetch MeiliSearch documents")
    meili_docs = await get_all_documents()
    meili_docs_by_id = {d["id"]: d for d in meili_docs}

    # Identify documents to delete
    synclog.info(f"Identify documents to delete")
    docs_to_delete_in_meili = {
        doc_id
        for doc_id, doc in metadata_docs_by_id.items()
        if not any(
            is_in_archive_directory(path_from_relative_path(rel_path))
            or os.path.exists(path_from_relative_path(rel_path))
            for rel_path in doc["paths"]
        )
    }

    # Remove outdated metadata entries on disk
    synclog.info(f"Remove outdated metadata entries on disk")
    for doc_id in docs_to_delete_in_meili:
        metadata_entry_path = os.path.join(METADATA_DIRECTORY, doc_id)
        if os.path.exists(metadata_entry_path):
            shutil.rmtree(metadata_entry_path)

    # Gather files from INDEX_DIRECTORY
    synclog.info(f'Gather all files from "{INDEX_DIRECTORY}"')
    file_paths = []
    for root, _, files in os.walk(INDEX_DIRECTORY):
        # Skip if root is the METADATA_DIRECTORY
        if os.path.abspath(root) == os.path.abspath(METADATA_DIRECTORY):
            continue
        for f in files:
            file_paths.append(os.path.join(root, f))

    # Process files in parallel
    docs_to_add_or_update = {}
    # Use ThreadPoolExecutor or ProcessPoolExecutor (try both, measure performance)
    synclog.info(f"Process files in parallel")
    with concurrent.futures.ProcessPoolExecutor(max_workers=16) as executor:
        futures = [
            executor.submit(process_file, fp, INDEX_DIRECTORY, metadata_docs_by_id)
            for fp in file_paths
        ]
        for fut in concurrent.futures.as_completed(futures):
            result = fut.result()
            if result is not None:
                doc_id, doc = result
                # Persist updated doc to disk
                write_document_json(doc)
                # Update local caches
                metadata_docs_by_id[doc_id] = doc
                docs_to_add_or_update[doc_id] = doc

    # Delete in Meili any doc that we've identified as stale
    synclog.info(f"Delete in Meili any doc that we've identified as stale")
    final_docs_to_delete = docs_to_delete_in_meili & set(meili_docs_by_id)
    if final_docs_to_delete:
        await delete_documents_by_id(list(final_docs_to_delete))
        await wait_for_meili_idle()

    # Add or update in Meili
    synclog.info(f"Add or update in Meili")
    if docs_to_add_or_update:
        await add_or_update_documents(list(docs_to_add_or_update.values()))
        await wait_for_meili_idle()

    total_docs_in_meili = await get_document_count()
    save_modules_state()
    synclog.info(f"Sync complete. MeiliSearch has {total_docs_in_meili} documents")


# endregion
# region "run modules"


def file_relpath_from_meili_doc(document):
    return Path(document["paths"][0])


def metadata_dir_relpath_from_doc(name, document):
    return Path(f"{document["id"]}/{name}")


def update_document_status(name, document):
    is_archived = False

    for relative_path in document["paths"]:
        if relative_path.startswith(ARCHIVE_DIRECTORY):
            is_archived = True

    document["is_archived"] = is_archived
    status = "idle"

    file_relpath = file_relpath_from_meili_doc(document)
    metadata_dir_relpath = metadata_dir_relpath_from_doc(name, document)
    for name, proxy in modules:
        if proxy.check(file_relpath, document, metadata_dir_relpath):
            status = name
            break

    document["status"] = status
    write_document_json(document)
    return document


async def run_module(name, proxy):
    try:
        logging.debug(f"{name} select files")
        documents = await get_all_pending_jobs(name)
        documents = sorted(documents, key=lambda x: x["mtime"], reverse=True)
        if documents:
            logging.info(f"{name} started for {len(documents)} documents")
            start_time = time.monotonic()
            for document in documents:
                try:
                    elapsed_time = time.monotonic() - start_time
                    if elapsed_time > ALLOWED_TIME_PER_MODULE:
                        logging.debug(f"{name} exceeded configured allowed time")
                        return True
                    file_relpath = file_relpath_from_meili_doc(document)
                    metadata_dir_relpath = metadata_dir_relpath_from_doc(name, document)
                    document = update_document_status(name, document)
                    if document.get("status", "idle") == name:
                        proxy.run(file_relpath, document, metadata_dir_relpath)
                        document = update_document_status(name, document)
                    logging.info(f'{name} "{file_relpath}" commit update')
                    await add_or_update_document(document)
                except Exception as e:
                    logging.exception(
                        f'{name} updating meili for document "{document}" failed: {e} '
                    )
        logging.debug(f"{name} up-to-date")
        return False
    except Exception as e:
        logging.exception(f"{name} processing failed: {e}")
        return True


async def run_modules():
    logging.info(f"run modules")
    while True:
        run_again = False
        for module in module_values:
            module_did_not_finish = await run_module(module["name"], module["proxy"])
            run_again = run_again or module_did_not_finish
        if not run_again:
            await asyncio.sleep(RECHECK_TIME_AFTER_COMPLETE)


# endregion
# region "set schedule"


def run_async_in_loop(func, *args):
    asyncio.run(func(*args))


def run_in_process(func, *args):
    process = Process(target=run_async_in_loop, args=(func,) + args)
    process.start()
    process.join()


async def main():
    await init_meili()
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        run_in_process,
        CronTrigger(hour=3, minute=0),
        args=[sync_documents],
        max_instances=1,
    )
    await sync_documents()
    scheduler.start()
    await run_modules()


if __name__ == "__main__":
    asyncio.run(main())


# endregion
