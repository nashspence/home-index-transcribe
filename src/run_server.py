import logging
import os
from xmlrpc.server import SimpleXMLRPCServer
from contextlib import contextmanager
from pathlib import Path

HOST = os.environ.get("HOST", "localhost")
PORT = os.environ.get("PORT", 9000)
LOGGING_LEVEL = os.environ.get("LOGGING_LEVEL", "INFO")
METADATA_DIRECTORY = Path(os.environ.get("METADATA_DIRECTORY", "/files/metadata"))
FILES_DIRECTORY = Path(os.environ.get("FILES_DIRECTORY", "/files"))


@contextmanager
def log_to_file_and_stdout(file_path):
    file_handler = logging.FileHandler(file_path)
    file_handler.setLevel(LOGGING_LEVEL)
    file_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    file_handler.setFormatter(file_formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(LOGGING_LEVEL)
    stream_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    stream_handler.setFormatter(stream_formatter)

    root_logger = logging.getLogger()
    root_logger.addHandler(file_handler)
    root_logger.addHandler(stream_handler)

    try:
        yield
    finally:
        root_logger.removeHandler(file_handler)
        file_handler.close()
        root_logger.removeHandler(stream_handler)


def run_server(hello_fn, check_fn, run_fn, load_fn=None, unload_fn=None):
    class Handler:
        def hello(self):
            return hello_fn()

        def check(self, file_relpath, document, metadata_dir_relpath):
            file_path = FILES_DIRECTORY / file_relpath
            metadata_dir_path = METADATA_DIRECTORY / metadata_dir_relpath
            with log_to_file_and_stdout(metadata_dir_path / "log.txt"):
                x = check_fn(file_path, document, metadata_dir_path)
            return x

        def load(self):
            if load_fn:
                load_fn()

        def run(self, file_relpath, document, metadata_dir_relpath):
            file_path = FILES_DIRECTORY / file_relpath
            metadata_dir_path = METADATA_DIRECTORY / metadata_dir_relpath
            with log_to_file_and_stdout(metadata_dir_path / "log.txt"):
                x = run_fn(file_path, document, metadata_dir_path)
            return x

        def unload(self):
            if unload_fn:
                unload_fn()

    server = SimpleXMLRPCServer((HOST, PORT), allow_none=True)
    server.register_instance(Handler())
    print(f"Server running at {server.server_address}")
    server.serve_forever()
