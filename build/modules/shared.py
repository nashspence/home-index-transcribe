import logging
import json
import os
import asyncio
from contextlib import contextmanager

@contextmanager
def setup_logger(document_id, file_path, log_path, level=logging.ERROR):
    """
    Context manager to set up a logger for the given document and file path.
    """
    logger = logging.getLogger(f'{document_id} {file_path}')
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    # Check if handlers are already added to prevent duplicate logs
    if not logger.handlers:
        # Console handler for errors
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] [%(name)s] %(message)s')
        console_handler.setFormatter(console_formatter)

        # File handler for debug logs
        file_handler = logging.FileHandler(log_path)
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] %(message)s')
        file_handler.setFormatter(file_formatter)

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

        handlers = [console_handler, file_handler]
    else:
        # If handlers are already set up, get them
        handlers = logger.handlers

    try:
        yield logger
    finally:
        # Cleanup code: close handlers and remove them from the logger
        for handler in handlers:
            handler.close()
            logger.removeHandler(handler)

def is_version_out_of_date(document, doc_db_path, name, version, mime_types=None, extra_files=[]):
    """
    Determines whether processing is needed based on versioning.

    Parameters:
        doc_db_path (Path): Path to the document's database directory.
        name (str): Module name.
        version (int): Current version of the module.

    Returns:
        bool: True if processing is needed, False otherwise.
    """
    if mime_types:
        if not document["type"] in mime_types:
            return False
    version_info = None
    version_path = doc_db_path / f"{name}.json"
    if version_path.exists():
        with open(version_path, 'r') as file:
            version_info = json.load(file)
    else:
        return True
    if version_info and version_info.get("version") == version:
        return False
    if "added_fields" in version_info:
        for field in version_info["added_fields"]:
            document.pop(field, None)
    for file_name in extra_files:
        file_path = doc_db_path / file_name
        if file_path.exists():
            os.remove(file_path)
    return True

async def process_files_sequential(file_list, process_file_func, cancel_event, name):
    """
    Processes files sequentially, suitable for resource-intensive tasks.

    Parameters:
        file_list (list): List of files to process.
        process_file_func (callable): Async function to process a single file.
        cancel_event (asyncio.Event): Event to signal cancellation.
        name (str): Module name.

    Yields:
        dict: Processed document data.
    """
    stats = {
        "success": 0,
        "failure": 0,
        "cancelled": 0
    }
    for file in file_list:
        if cancel_event.is_set():
            logging.debug(f"{name} cancellation requested")
            stats["cancelled"] += 1
            break
        try:
            result, stat = await process_file_func(file)
            for key in stats:
                stats[key] += stat.get(key, 0)
            if result:
                yield result
        except asyncio.CancelledError:
            stats["cancelled"] += 1
            break
        except Exception as e:
            logging.exception(f"Unhandled exception during file processing: {e}")
            stats["failure"] += 1

    logging.info("------------------------------------------")
    logging.info(f"{name} shutdown summary:")
    logging.info(f"  {stats['success']} succeeded")
    logging.info(f"  {stats['failure']} failed")
    logging.info(f"  {stats['cancelled']} postponed")
    logging.info("------------------------------------------")