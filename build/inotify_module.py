import logging
import os
import pyinotify
import shutil
import threading

from meilisearch_python_sdk import Client
from shared import MEILISEARCH_HOST, MEILISEARCH_INDEX_NAME, METADATA_DIRECTORY, gather_file_info, get_document_for_hash, delete_from_cache, move_in_cache, INDEX_DIRECTORY, get_from_cache, is_in_archive_directory

class EventHandler(pyinotify.ProcessEvent):
    def __init__(self):
        self.meili_client = Client(MEILISEARCH_HOST)
        self.index = self.meili_client.index(MEILISEARCH_INDEX_NAME)
        self.pending_events = {}
        self.lock = threading.Lock()
        self.timer = None
        self.debounce_delay = 60
        self.update_batch = {}
        self.delete_batch = {}
        
    def is_in_index_directory(self, path):
        try:
            return os.path.commonpath([path, INDEX_DIRECTORY]) == INDEX_DIRECTORY
        except ValueError:
            return False  # Occurs if paths are on different drives or invalid

    def is_in_metadata_directory(self, path):
        try:
            return os.path.commonpath([path, METADATA_DIRECTORY]) == METADATA_DIRECTORY
        except ValueError:
            return False

    def record_event(self, path, event_type, move_from_path=None):
        with self.lock:
            if move_from_path and event_type in ['move']:
                move_in_cache(move_from_path, path)
                logging.debug(f'{event_type} event caused moved hash cache entry from "{move_from_path}" to "{path}"')
            if event_type in ['delete', 'modify'] and not is_in_archive_directory(path):
                cache_entry = get_from_cache(path)
                doc_id = cache_entry["hash"]
                if doc_id:
                    metadata_entry_path = os.path.join(METADATA_DIRECTORY, doc_id)
                    if os.path.exists(metadata_entry_path):
                        shutil.rmtree(metadata_entry_path)
                        logging.debug(f'removed "{metadata_entry_path}"')
                    delete_from_cache(path)
                    logging.debug(f'{event_type} event caused delete "{path}" from hash cache')
                    self.delete_batch.add(doc_id)
                else:
                    logging.warning(f'{event_type} event for "{path}" failed to find previous hash id')
            if event_type in ['create', 'move', 'modify']:
                file_info = gather_file_info(path)
                document, is_changed, is_dead = get_document_for_hash(
                    file_info["id"], [file_info], move_from_path
                )
                self.update_batch[document["id"]] = document     
            if self.timer:
                self.timer.cancel()
            self.timer = threading.Timer(self.debounce_delay, self.process_pending_events)
            self.timer.start()

    def get_valid_path(self, event):
        path = event.pathname
        if self.is_in_metadata_directory(path) or event.dir:
            return None
        return path

    def handle_event(self, event, event_type):
        path = self.get_valid_path(event)
        if not path:
            return
        try:
            logging.info(f'{event_type} "{path}"')
            self.record_event(path, event_type)
        except Exception:
            logging.exception(f'{event_type} failed "{path}"')

    def process_IN_CREATE(self, event):
        self.handle_event(event, 'create')

    def process_IN_MODIFY(self, event):
        self.handle_event(event, 'modify')

    def process_IN_DELETE(self, event):
        self.handle_event(event, 'delete')

    def process_IN_MOVED_FROM(self, event):
        path = self.get_valid_path(event)
        if not path:
            return
        self.move_events[event.cookie] = path

    def process_IN_MOVED_TO(self, event):
        path = self.get_valid_path(event)
        if not path:
            return
        from_path = self.move_events.pop(event.cookie, None)
        if from_path:
            self.handle_move_event(from_path, path)
        else:
            if self.is_in_index_directory(path):
                logging.info(f'create "{path}"')
                self.record_event(path, 'create')

    def handle_move_event(self, from_path, to_path):
        from_in_index = self.is_in_index_directory(from_path)
        to_in_index = self.is_in_index_directory(to_path)
        if not from_in_index and to_in_index:
            logging.info(f'file moved into index: "{from_path}" -> "{to_path}"')
            self.record_event(to_path, 'create')
        elif from_in_index and not to_in_index:
            logging.info(f'file moved out of index: "{from_path}" -> "{to_path}"')
            self.record_event(from_path, 'delete')
        elif from_in_index and to_in_index:
            logging.info(f'file moved within index: "{from_path}" -> "{to_path}"')
            self.record_event(to_path, 'move', from_path)

    def process_pending_events(self):
        with self.lock:
            documents_to_update = self.update_batch.copy()
            documents_to_delete = self.delete_batch.copy()
            self.update_batch.clear()
            self.delete_batch.clear()
            self.timer = None  # Reset the timer
        if documents_to_update:
            self.index.update_documents(documents_to_update)
            logging.info(
                f'added or updated {len(documents_to_update)} in meili'
            )
        if documents_to_delete:
            self.index.delete_documents(documents_to_delete)
            logging.info(
                f'deleted {len(documents_to_delete)} from meili'
            )
    
def start_inotify():
    logger = logging.getLogger("inotify-process")
    logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler(f"./data/logs/inotify-process.log")
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(file_handler)
    logging.root = logger
    
    wm = pyinotify.WatchManager()
    mask = (
        pyinotify.IN_CREATE
        | pyinotify.IN_MODIFY
        | pyinotify.IN_DELETE
        | pyinotify.IN_MOVED_FROM
        | pyinotify.IN_MOVED_TO
    )

    handler = EventHandler()
    notifier = pyinotify.Notifier(wm, handler)
    wm.add_watch(INDEX_DIRECTORY, mask, rec=True, auto_add=True)

    logging.info("Starting inotify watcher")
    notifier.loop()
    
