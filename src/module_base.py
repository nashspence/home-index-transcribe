from xmlrpc.server import SimpleXMLRPCServer

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
