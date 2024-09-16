import os
import hashlib
import json
from pathlib import Path
import meilisearch
import logging
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("./data/indexer.log"),
        logging.StreamHandler()
    ]
)

# Configuration from environment variables
DIRECTORY_TO_INDEX = os.environ.get('DIRECTORY_TO_INDEX', '/data')
MEILISEARCH_HOST = os.environ.get('MEILISEARCH_HOST', 'http://meilisearch:7700')
INDEX_NAME = os.environ.get('INDEX_NAME', 'files')
DOMAIN = os.environ.get('DOMAIN', 'private.0819870.xyz')
CACHE_FILE = os.environ.get('CACHE_FILE', './data/file_cache.json')
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '1000'))
MAX_WORKERS = int(os.environ.get('MAX_WORKERS', '5'))

def get_file_metadata(file_path, relative_path):
    """Extract metadata from a file."""
    try:
        stat = file_path.stat()
        file_id = hashlib.md5(str(file_path).encode('utf-8')).hexdigest()
        metadata = {
            'id': file_id,
            'path': str(file_path),
            'name': file_path.name,
            'size': stat.st_size,
            'modification_time': stat.st_mtime,
            'creation_time': stat.st_ctime,
            'url': f'https://{DOMAIN}/{relative_path.as_posix()}',
            # Placeholders for future features
            'thumbnails': [],
            'text_content': '',
            'transcription': ''
        }
        return metadata
    except Exception as e:
        logging.error(f"Error getting metadata for {file_path}: {e}")
        return None

def load_cache(cache_file):
    """Load the cache from a JSON file."""
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error loading cache file {cache_file}: {e}")
            return {}
    return {}

def save_cache(cache, cache_file):
    """Save the cache to a JSON file."""
    try:
        with open(cache_file, 'w') as f:
            json.dump(cache, f)
    except Exception as e:
        logging.error(f"Error saving cache file {cache_file}: {e}")

def process_file(file_path, directory):
    """Process a single file and return its metadata if it needs updating."""
    if file_path.is_file():
        relative_path = file_path.relative_to(directory)
        file_id = hashlib.md5(str(file_path).encode('utf-8')).hexdigest()
        mod_time = file_path.stat().st_mtime
        return file_id, mod_time, relative_path
    return None

def main():
    logging.info("Starting file indexing process.")

    # Connect to Meilisearch
    client = meilisearch.Client(MEILISEARCH_HOST)
    try:
        index = client.get_index(INDEX_NAME)
    except meilisearch.errors.MeilisearchApiError as e:
        if e.status_code == 404:
            logging.info(f"Index '{INDEX_NAME}' does not exist. Creating a new index.")
            task = client.create_index(INDEX_NAME, {'primaryKey': 'id'})
            client.wait_for_task(task['uid'])  # Wait for the index to be created
            index = client.get_index(INDEX_NAME)
        else:
            logging.error(f"Meilisearch API error: {e.message}")
            return
    except Exception as e:
        logging.error(f"Error connecting to Meilisearch: {e}")
        return

    # Load the existing cache
    cache = load_cache(CACHE_FILE)
    new_cache = {}
    files_to_add = []
    ids_to_delete = []

    directory = Path(DIRECTORY_TO_INDEX).resolve()
    if not directory.exists():
        logging.error(f"Directory to index does not exist: {directory}")
        return

    # Collect all file paths
    all_files = list(directory.rglob('*'))
    logging.info(f"Found {len(all_files)} items in directory.")

    # Process files using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = executor.map(lambda fp: process_file(fp, directory), all_files)

    for result in results:
        if result:
            file_id, mod_time, relative_path = result
            new_cache[file_id] = mod_time
            if file_id not in cache or cache[file_id] != mod_time:
                metadata = get_file_metadata(directory / relative_path, relative_path)
                if metadata:
                    files_to_add.append(metadata)

    # Identify deleted files
    existing_ids = set(cache.keys())
    current_ids = set(new_cache.keys())
    ids_to_delete = list(existing_ids - current_ids)

    # Batch processing for adding documents
    try:
        if files_to_add:
            logging.info(f"Adding/Updating {len(files_to_add)} documents.")
            for i in range(0, len(files_to_add), BATCH_SIZE):
                batch = files_to_add[i:i+BATCH_SIZE]
                index.add_documents(batch)
    except Exception as e:
        logging.error(f"Error adding documents to Meilisearch: {e}")

    # Batch processing for deleting documents
    try:
        if ids_to_delete:
            logging.info(f"Deleting {len(ids_to_delete)} documents.")
            for i in range(0, len(ids_to_delete), BATCH_SIZE):
                batch = ids_to_delete[i:i+BATCH_SIZE]
                index.delete_documents(batch)
    except Exception as e:
        logging.error(f"Error deleting documents from Meilisearch: {e}")

    # Save the new cache
    save_cache(new_cache, CACHE_FILE)
    logging.info("File indexing process completed.")

if __name__ == '__main__':
    main()
