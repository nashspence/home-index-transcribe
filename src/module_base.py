import logging
from xmlrpc.server import SimpleXMLRPCServer
from contextlib import contextmanager

@contextmanager
def log_to_file_and_stdout(file_path):
    # Create a file handler
    file_handler = logging.FileHandler(file_path)
    file_handler.setLevel(logging.INFO)  # Adjust log level as needed
    file_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    file_handler.setFormatter(file_formatter)

    # Create a stream handler for stdout
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")  # Simple format for stdout
    stream_handler.setFormatter(stream_formatter)

    # Get the root logger
    root_logger = logging.getLogger()
    root_logger.addHandler(file_handler)
    root_logger.addHandler(stream_handler)

    try:
        # Yield control back to the with block
        yield
    finally:
        # Ensure both handlers are removed and closed
        root_logger.removeHandler(file_handler)
        file_handler.close()
        root_logger.removeHandler(stream_handler)

class ModuleBase:
    def __init__(self, host="localhost", port=9000):
        self.server = SimpleXMLRPCServer((host, port), allow_none=True)
        self.server.register_instance(self)
        print(f"Server running at {self.server.server_address}")
        self.server.serve_forever()

    def hello(self):
        raise NotImplementedError("Subclasses must implement configure")
    
    def check(self, file_path, document, metadata_dir_path):
        raise NotImplementedError("Subclasses must implement configure")

    def load(self):
        return

    def run(self, file_path, document, metadata_dir_path):
        raise NotImplementedError("Subclasses must implement run")

    def unload(self):
        return
