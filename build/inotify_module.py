import logging
import os
import pyinotify
import shutil
import threading

from meilisearch_python_sdk import Client
from shared import MEILISEARCH_HOST, MEILISEARCH_INDEX_NAME, METADATA_DIRECTORY, gather_file_info, get_document_for_hash, delete_from_cache, move_in_cache, INDEX_DIRECTORY, get_from_cache, read_document_json, write_document_json, is_in_archive_directory, handle_document_changed, validate_and_update_document_paths

class EventHandler(pyinotify.ProcessEvent):
    def __init__(self):
        self.meili_client = Client(MEILISEARCH_HOST)
        self.index = self.meili_client.index(MEILISEARCH_INDEX_NAME)
        self.pending_events = {}
        self.lock = threading.Lock()
        self.timer = None
        self.debounce_delay = 60
        self.update_batch = {}
        self.delete_batch = set()
        self.move_events = {}
        
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

    def _handle_delete_path(self, path, doc_id, document):
        if document:
            paths = set(document['paths'])
            relative_path = os.path.relpath(path, INDEX_DIRECTORY)
            if not is_in_archive_directory(path) and relative_path in paths:
                existing_document = document.copy()
                paths.remove(relative_path)
                delete_from_cache(path)
                document['paths'] = list(paths)
                if document['paths']:
                    document['copies'] = len(document['paths'])
                    handle_document_changed(existing_document, document)
                    write_document_json(document)
                    self.update_batch[doc_id] = document
                else:
                    metadata_entry_path = os.path.join(METADATA_DIRECTORY, doc_id)
                    if os.path.exists(metadata_entry_path):
                        shutil.rmtree(metadata_entry_path)
                    self.delete_batch.add(doc_id)
                    if doc_id in self.update_batch:
                        del self.update_batch[doc_id]

    def record_event(self, path, event_type, move_from_path=None):
        with self.lock:
            if event_type == 'move' and move_from_path:
                move_in_cache(move_from_path, path)
            if not os.path.exists(path) or event_type == "modify":
                cache_entry = get_from_cache(path)
                if cache_entry:
                    old_doc_id = cache_entry["hash"]
                    old_document, is_version_changed = read_document_json(old_doc_id)
                    self._handle_delete_path(path, old_doc_id, old_document)
            if not os.path.exists(path):
                return
            file_info = gather_file_info(path)
            doc_id = file_info["id"]
            document, is_changed, is_dead = get_document_for_hash(doc_id, [file_info])
            if is_dead:
                self._handle_delete_path(path, doc_id, document)
            elif is_changed:
                self.update_batch[doc_id] = document
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
            documents_to_update = list(self.update_batch.values())
            documents_to_delete = list(self.delete_batch)
            self.update_batch.clear()
            self.delete_batch.clear()
            self.timer = None  # Reset the timer
        if documents_to_update:
            self.index.update_documents(documents_to_update)
            logging.info(
                f'upserted {len(documents_to_update)} in meili'
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

    logging.info("start inotify watcher")
    notifier.loop()
    
